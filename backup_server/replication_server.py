
import json
import math
import socket
import sqlite3
import threading

from database import DB_NAME, initialize_database

HOST = "0.0.0.0"
PORT = 5001
MAX_MESSAGE_SIZE = 65536


def apply_replication(request):
    """Apply a transaction once and save its result durably."""
    if not isinstance(request, dict):
        return {"status": "error", "message": "Invalid request"}

    required = ("request_id", "action", "account", "amount")
    if any(key not in request for key in required):
        return {"status": "error", "message": "Missing required fields"}

    request_id = request["request_id"]
    action = request["action"]
    account = request["account"]
    amount = request["amount"]

    if (
        not isinstance(request_id, str)
        or not request_id.strip()
        or len(request_id) > 128
    ):
        return {"status": "error", "message": "Invalid request ID"}

    request_id = request_id.strip()

    if action not in ("deposit", "withdraw"):
        return {"status": "error", "message": "Unsupported action"}

    if not isinstance(account, str) or not account.strip():
        return {"status": "error", "message": "Invalid account"}

    account = account.strip()

    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or amount <= 0
    ):
        return {"status": "error", "message": "Invalid amount"}

    amount = float(amount)

    conn = sqlite3.connect(
        DB_NAME,
        timeout=10,
        isolation_level=None,
    )

    try:
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("BEGIN IMMEDIATE")

        # A request ID can only represent one specific operation.
        previous = conn.execute(
            """
            SELECT action, account_id, amount, result_json
            FROM transactions
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()

        if previous is not None:
            old_action, old_account, old_amount, old_result = previous

            if (
                old_action != action
                or old_account != account
                or old_amount is None
                or float(old_amount) != amount
            ):
                conn.execute("ROLLBACK")
                return {
                    "status": "error",
                    "message": "Request ID reused for different transaction",
                    "request_id": request_id,
                }

            conn.execute("ROLLBACK")
            return json.loads(old_result)

        if action == "deposit":
            cursor = conn.execute(
                """
                UPDATE accounts
                SET balance = balance + ?
                WHERE account_id = ?
                """,
                (amount, account),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE accounts
                SET balance = balance - ?
                WHERE account_id = ?
                  AND balance >= ?
                """,
                (amount, account, amount),
            )

        if cursor.rowcount == 0:
            exists = conn.execute(
                "SELECT 1 FROM accounts WHERE account_id = ?",
                (account,),
            ).fetchone()

            result = {
                "status": "error",
                "message": (
                    "Insufficient funds"
                    if exists
                    else "Unknown account"
                ),
            }
        else:
            row = conn.execute(
                "SELECT balance FROM accounts WHERE account_id = ?",
                (account,),
            ).fetchone()

            result = {
                "status": "success",
                "account": account,
                "balance": float(row[0]),
            }

        conn.execute(
            """
            INSERT INTO transactions (
                request_id,
                action,
                account_id,
                amount,
                status,
                result_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                action,
                account,
                amount,
                result["status"],
                json.dumps(result, ensure_ascii=False),
            ),
        )

        conn.execute("COMMIT")
        return result

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


def get_database_snapshot():
    """
    Return a consistent snapshot of the backup's accounts
    and complete transaction history.
    """
    conn = sqlite3.connect(
        DB_NAME,
        timeout=10,
        isolation_level=None,
    )

    try:
        conn.execute("BEGIN")

        accounts = conn.execute(
            """
            SELECT account_id, balance
            FROM accounts
            ORDER BY account_id
            """
        ).fetchall()

        transactions = conn.execute(
            """
            SELECT
                request_id,
                action,
                account_id,
                amount,
                status,
                result_json,
                created_at
            FROM transactions
            ORDER BY created_at, request_id
            """
        ).fetchall()

        snapshot = {
            "status": "success",
            "accounts": [
                {
                    "account_id": row[0],
                    "balance": row[1],
                }
                for row in accounts
            ],
            "transactions": [
                {
                    "request_id": row[0],
                    "action": row[1],
                    "account_id": row[2],
                    "amount": row[3],
                    "status": row[4],
                    "result_json": row[5],
                    "created_at": row[6],
                }
                for row in transactions
            ],
        }

        conn.execute("COMMIT")
        return snapshot

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


def receive_line(conn):
    data = bytearray()

    while True:
        chunk = conn.recv(4096)

        if not chunk:
            if not data:
                return None
            raise ValueError("Incomplete message")

        data.extend(chunk)

        if len(data) > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large")

        if b"\n" in data:
            message, _, extra = data.partition(b"\n")

            if extra:
                raise ValueError("Unexpected extra data")

            return json.loads(message.decode("utf-8"))


def handle_client(conn, address):
    with conn:
        try:
            request = receive_line(conn)

            if request is None:
                return

            if (
                isinstance(request, dict)
                and request.get("action") == "snapshot"
            ):
                response = get_database_snapshot()
            else:
                response = apply_replication(request)

        except Exception as error:
            print(f"Request from {address} failed: {error}")
            response = {
                "status": "error",
                "message": "Replication request failed",
            }

        try:
            conn.sendall(
                json.dumps(response).encode("utf-8") + b"\n"
            )
        except OSError as error:
            print(f"Could not send response to {address}: {error}")

        if (
            isinstance(request, dict)
            and request.get("action") == "snapshot"
        ):
            print(
                f"Snapshot requested by {address[0]}"
            )
        else:
            print(
                f"Replication request from {address[0]}: {response}"
            )


def main():
    initialize_database()

    with socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    ) as server:
        server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )
        server.bind((HOST, PORT))
        server.listen(10)

        print(
            f"Backup replication service listening on {HOST}:{PORT}"
        )

        while True:
            conn, address = server.accept()

            threading.Thread(
                target=handle_client,
                args=(conn, address),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()

