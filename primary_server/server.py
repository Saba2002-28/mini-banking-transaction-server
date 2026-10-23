
import json
import logging
import os
import socket
import sqlite3
import threading
import uuid
import time
import math
from datetime import datetime, timezone



#################### Configuration #########################


HOST = "0.0.0.0"
PORT = 5000

BACKUP_HOST = "10.194.197.134" 
BACKUP_PORT = 5001

SOCKET_TIMEOUT = 10
MAX_REQUEST_SIZE = 1024 * 1024
MAX_SNAPSHOT_SIZE = 16 * 1024 * 1024

# NEW: maximum concurrent clients, configurable through an environment variable.
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", "10"))

if MAX_CLIENTS < 1:
    raise ValueError("MAX_CLIENTS must be at least 1")

client_semaphore = threading.BoundedSemaphore(MAX_CLIENTS)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bank.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_PATH = os.path.join(LOG_DIR, "banking_server.log")

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

database_lock = threading.RLock()



############################# Database ################################


def connect_database():
    """Create a SQLite connection with durable transaction settings."""
    conn = sqlite3.connect(
        DB_PATH,
        timeout=10,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database():
    """Create the required tables without resetting existing balances."""
    conn = connect_database()

    try:
        conn.execute("PRAGMA journal_mode = WAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                balance REAL NOT NULL CHECK (balance >= 0)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                request_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                account_id TEXT NOT NULL,
                amount REAL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        initial_accounts = [
            ("A1001", 1000.0),
            ("A1002", 2500.0),
            ("A1003", 500.0),
        ]

        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            """
            INSERT OR IGNORE INTO accounts (account_id, balance)
            VALUES (?, ?)
            """,
            initial_accounts,
        )
        conn.commit()

    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def read_saved_transaction(request_id):
    """Return the stored result for a previously processed request."""
    conn = connect_database()

    try:
        row = conn.execute(
            """
            SELECT result_json
            FROM transactions
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()

        if row is None:
            return None

        return json.loads(row["result_json"])
    finally:
        conn.close()



############################## Socket helpers #########################


def receive_line(conn, maximum_size=MAX_REQUEST_SIZE):
    """Read one newline-terminated JSON message."""
    data = bytearray()

    while True:
        chunk = conn.recv(4096)

        if not chunk:
            if not data:
                return None
            break

        newline_index = chunk.find(b"\n")

        if newline_index >= 0:
            data.extend(chunk[:newline_index])
            break

        data.extend(chunk)

        if len(data) > maximum_size:
            raise ValueError("Message exceeds maximum allowed size")

    if len(data) > maximum_size:
        raise ValueError("Message exceeds maximum allowed size")

    return json.loads(data.decode("utf-8"))


def send_json(conn, payload):
    """Send one JSON response followed by a newline."""
    message = json.dumps(payload).encode("utf-8") + b"\n"
    conn.sendall(message)



######################### Backup communication #####################


def request_backup_snapshot():
    """Request the current accounts and transaction history from VM3."""
    request = {"action": "snapshot"}

    print(
        f"Requesting recovery snapshot from "
        f"{BACKUP_HOST}:{BACKUP_PORT}..."
    )

    with socket.create_connection(
        (BACKUP_HOST, BACKUP_PORT),
        timeout=SOCKET_TIMEOUT,
    ) as conn:
        conn.settimeout(SOCKET_TIMEOUT)
        conn.sendall(json.dumps(request).encode("utf-8") + b"\n")

        response = receive_line(
            conn,
            maximum_size=MAX_SNAPSHOT_SIZE,
        )

    if not isinstance(response, dict):
        raise ValueError("Backup returned an invalid snapshot response")

    if response.get("status") == "error":
        raise ValueError(
            response.get("message", "Backup snapshot request failed")
        )

    return response


def validate_snapshot(snapshot):
    """
    Validate the backup snapshot before replacing the local database.

    Failed transactions may refer to unknown accounts because a failed
    withdrawal against an invalid account must remain in the audit history.
    Successful transactions must refer to existing accounts.
    """
    accounts = snapshot.get("accounts")
    transactions = snapshot.get("transactions")

    if not isinstance(accounts, list):
        raise ValueError("Snapshot accounts must be a list")

    if not isinstance(transactions, list):
        raise ValueError("Snapshot transactions must be a list")

    account_ids = set()
    validated_accounts = []

    for account in accounts:
        if not isinstance(account, dict):
            raise ValueError("Invalid account record in snapshot")

        account_id = account.get("account_id")
        balance = account.get("balance")

        if not isinstance(account_id, str) or not account_id:
            raise ValueError("Invalid account ID in snapshot")

        if isinstance(balance, bool) or not isinstance(
            balance, (int, float)
        ):
            raise ValueError("Invalid account balance in snapshot")

        if balance < 0:
            raise ValueError("Negative account balance in snapshot")

        if account_id in account_ids:
            raise ValueError("Duplicate account ID in snapshot")

        account_ids.add(account_id)

        validated_accounts.append({
            "account_id": account_id,
            "balance": float(balance),
        })

    validated_transactions = []
    transaction_ids = set()

    for transaction in transactions:
        if not isinstance(transaction, dict):
            raise ValueError("Invalid transaction record in snapshot")

        request_id = transaction.get("request_id")
        action = transaction.get("action")
        account_id = transaction.get("account_id")
        amount = transaction.get("amount")
        status = transaction.get("status")
        result_json = transaction.get("result_json")
        created_at = transaction.get("created_at")

        if not isinstance(request_id, str) or not request_id:
            raise ValueError("Invalid transaction request ID")

        if request_id in transaction_ids:
            raise ValueError("Duplicate transaction request ID")

        transaction_ids.add(request_id)

        if action not in ("deposit", "withdraw"):
            raise ValueError("Invalid transaction action in snapshot")

        if not isinstance(account_id, str) or not account_id:
            raise ValueError("Invalid transaction account ID")

        if status not in ("success", "error"):
            raise ValueError("Invalid transaction status in snapshot")

        # A failed transaction can legitimately refer to an unknown account.
        # A successful transaction must refer to an existing account.
        if account_id not in account_ids and status != "error":
            raise ValueError(
                "Successful transaction references an unknown account"
            )

        if amount is not None:
            if isinstance(amount, bool) or not isinstance(
                amount, (int, float)
            ):
                raise ValueError("Invalid transaction amount")

        if isinstance(result_json, str):
            try:
                result_json = json.loads(result_json)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "Invalid transaction result JSON"
                ) from exc

        if not isinstance(result_json, dict):
            raise ValueError("Transaction result must be a JSON object")

        if not isinstance(created_at, str) or not created_at:
            created_at = datetime.now(timezone.utc).isoformat()

        validated_transactions.append({
            "request_id": request_id,
            "action": action,
            "account_id": account_id,
            "amount": amount,
            "status": status,
            "result_json": result_json,
            "created_at": created_at,
        })

    return validated_accounts, validated_transactions


def synchronize_from_backup():
    """
    Restore the local database from VM3 before accepting client requests.

    Both tables are replaced in one SQLite transaction, so a failed restore
    does not leave a partially synchronized database.
    """
    snapshot = request_backup_snapshot()
    accounts, transactions = validate_snapshot(snapshot)

    conn = connect_database()

    try:
        conn.execute("BEGIN IMMEDIATE")

        conn.execute("DELETE FROM transactions")
        conn.execute("DELETE FROM accounts")

        conn.executemany(
            """
            INSERT INTO accounts (account_id, balance)
            VALUES (?, ?)
            """,
            [
                (account["account_id"], account["balance"])
                for account in accounts
            ],
        )

        conn.executemany(
            """
            INSERT INTO transactions (
                request_id,
                action,
                account_id,
                amount,
                status,
                result_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    transaction["request_id"],
                    transaction["action"],
                    transaction["account_id"],
                    transaction["amount"],
                    transaction["status"],
                    json.dumps(transaction["result_json"]),
                    transaction["created_at"],
                )
                for transaction in transactions
            ],
        )

        conn.commit()

    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()

    message = (
        f"Recovery complete: {len(accounts)} accounts and "
        f"{len(transactions)} transaction records synchronized."
    )
    print(message)
    logger.info(message)


def replicate_to_backup(request):
    """
    Send a mutation to VM3 and return its response.

    The backup processes the request first. The primary only updates its
    local database after receiving the backup's response.
    """
    with socket.create_connection(
        (BACKUP_HOST, BACKUP_PORT),
        timeout=SOCKET_TIMEOUT,
    ) as conn:
        conn.settimeout(SOCKET_TIMEOUT)

        replication_request = {
            "request_id": request["request_id"],
            "action": request["action"],
            "account": request["account_id"],
            "amount": request["amount"],
        }

        conn.sendall(
            json.dumps(replication_request).encode("utf-8") + b"\n"
        )

        response = receive_line(conn)

    if not isinstance(response, dict):
        raise ValueError("Backup returned an invalid response")

    return response



######################### Local transaction synchronization ####################


def synchronize_local_database(request, backup_result):
    """
    Apply a backup-confirmed transaction to the primary database.

    The request ID is unique, preventing a retried request from being
    applied to the primary database more than once.
    """
    request_id = request["request_id"]
    action = request["action"]
    account_id = request["account_id"]
    amount = request.get("amount")

    conn = connect_database()

    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            """
            SELECT result_json
            FROM transactions
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()

        if existing is not None:
            conn.commit()
            return json.loads(existing["result_json"])

        status = backup_result.get("status", "error")

        # Only update a local balance when the backup confirms success.
        if status == "success":
            if account_id not in {
                row["account_id"]
                for row in conn.execute(
                    "SELECT account_id FROM accounts"
                ).fetchall()
            }:
                raise ValueError(
                    "Backup reported success for an unknown account"
                )

            if "balance" in backup_result:
                conn.execute(
                    """
                    UPDATE accounts
                    SET balance = ?
                    WHERE account_id = ?
                    """,
                    (float(backup_result["balance"]), account_id),
                )
            else:
                # Fallback for backup responses that omit the new balance.
                row = conn.execute(
                    """
                    SELECT balance
                    FROM accounts
                    WHERE account_id = ?
                    """,
                    (account_id,),
                ).fetchone()

                if row is None:
                    raise ValueError("Account not found locally")

                current_balance = float(row["balance"])

                if action == "deposit":
                    new_balance = current_balance + float(amount)
                else:
                    new_balance = current_balance - float(amount)

                if new_balance < 0:
                    raise ValueError(
                        "Backup response would produce a negative balance"
                    )

                conn.execute(
                    """
                    UPDATE accounts
                    SET balance = ?
                    WHERE account_id = ?
                    """,
                    (new_balance, account_id),
                )

        conn.execute(
            """
            INSERT INTO transactions (
                request_id,
                action,
                account_id,
                amount,
                status,
                result_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                action,
                account_id,
                amount,
                status,
                json.dumps(backup_result),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

        conn.commit()
        return backup_result

    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()



########################## Request processing #########################


def handle_request(request):
    """Process a balance inquiry, deposit, or withdrawal."""
    if not isinstance(request, dict):
        return {
            "status": "error",
            "message": "Request must be a JSON object",
        }

    action = request.get("action")
    account_id = request.get("account_id", request.get("account"))

    if action in ("balance", "get_balance"):
        if not isinstance(account_id, str) or not account_id:
            return {
                "status": "error",
                "message": "A valid account_id is required",
            }

        conn = connect_database()

        try:
            row = conn.execute(
                """
                SELECT balance
                FROM accounts
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()

            if row is None:
                return {
                    "status": "error",
                    "message": "Unknown account",
                    "account_id": account_id,
                }

            return {
                "status": "success",
                "account_id": account_id,
                "balance": float(row["balance"]),
            }
        finally:
            conn.close()

    if action not in ("deposit", "withdraw"):
        return {
            "status": "error",
            "message": "Unsupported action",
        }

    if not isinstance(account_id, str) or not account_id:
        return {
            "status": "error",
            "message": "A valid account_id is required",
        }

    amount = request.get("amount")

    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or amount <= 0
    ):
        return {
            "status": "error",
            "message": "Amount must be a positive finite number",
        }

    request_id = request.get("request_id")

    if not isinstance(request_id, str) or not request_id:
        request_id = str(uuid.uuid4())
        request["request_id"] = request_id

    request_to_replicate = {
        "request_id": request_id,
        "action": action,
        "account_id": account_id,
        "amount": amount,
    }

    # If this request was already committed locally, return its old result.
    previous_result = read_saved_transaction(request_id)

    if previous_result is not None:
        return previous_result

    # The backup must durably process the mutation before the primary
    # acknowledges it to the client.
    backup_result = replicate_to_backup(request_to_replicate)

    # Keep the primary's database consistent with the backup result.
    return synchronize_local_database(
        request_to_replicate,
        backup_result,
    )



########################## Client connection handling #######################


def handle_client(conn, address):
    """Handle one client connection."""
    logger.info("Client connected: %s:%s", address[0], address[1])

    try:
        conn.settimeout(SOCKET_TIMEOUT)

        while True:
            request = receive_line(conn)

            if request is None:
                break

            # Give every valid JSON-object request a correlation ID.
            # Mutation handlers reuse this same ID for idempotency.
            if isinstance(request, dict):
                request_id = request.get("request_id")
                if not isinstance(request_id, str) or not request_id:
                    request_id = str(uuid.uuid4())
                    request["request_id"] = request_id
            else:
                request_id = str(uuid.uuid4())

            request_start = time.perf_counter()

            try:
                with database_lock:
                    response = handle_request(request)

            except (OSError, TimeoutError, ValueError) as exc:
                logger.exception("Request failed for %s", address)

                response = {
                    "status": "error",
                    "message": str(exc),
                }

            except Exception:
                logger.exception("Unexpected request error from %s", address)

                response = {
                    "status": "error",
                    "message": "Internal server error",
                }

            latency_ms = (time.perf_counter() - request_start) * 1000

            if isinstance(request, dict):
                action = request.get("action", "unknown")
                account_id = request.get(
                    "account_id",
                    request.get("account", "unknown"),
                )
            else:
                action = "invalid_request"
                account_id = "unknown"

            result = (
                response.get("status", "unknown")
                if isinstance(response, dict)
                else "unknown"
            )

            logger.info(
                "Request processed | request_id=%s | client_ip=%s | "
                "client_port=%s | action=%s | account_id=%s | "
                "timestamp=%s | result=%s | latency_ms=%.3f | "
                "request=%s | response=%s",
                request_id,
                address[0],
                address[1],
                action,
                account_id,
                datetime.now(timezone.utc).isoformat(),
                result,
                latency_ms,
                json.dumps(request, ensure_ascii=False),
                json.dumps(response, ensure_ascii=False),
            )

            send_json(conn, response)

    except (ConnectionError, TimeoutError, OSError):
        logger.info("Connection closed or timed out: %s", address)

    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("Invalid client message from %s: %s", address, exc)

        try:
            send_json(conn, {
                "status": "error",
                "message": "Invalid JSON request",
            })
        except OSError:
            pass

    finally:
        conn.close()
        logger.info("Client disconnected: %s:%s", address[0], address[1])


# NEW: maximum concurrent clients
def handle_client_with_limit(conn, address):
    """Handle a client and release its concurrency slot when finished."""
    try:
        handle_client(conn, address)
    finally:
        client_semaphore.release()



########################## Server startup ###################################


def start_server():
    """Recover from the backup, then begin accepting clients."""
    initialize_database()

    # Do not accept client requests until recovery has succeeded.
    # If the backup is unavailable, startup stops rather than silently
    # serving potentially stale account balances.
    try:
        synchronize_from_backup()
    
    except (OSError, TimeoutError, ValueError) as exc:
        message = (
            "Cannot start the primary banking server because "
            f"recovery from the replication server "
            f"({BACKUP_HOST}:{BACKUP_PORT}) failed. "
            "Check that the replication server is running, "
            "verify its IP address and port, and try again. "
            f"Details: {exc}"
        )
        
        logger.error(message)
        print(f"\nERROR: {message}")
        raise SystemExit(1)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )

        server_socket.bind((HOST, PORT))
        server_socket.listen(50)

        print(f"Banking server listening on {HOST}:{PORT}")
        logger.info("Banking server listening on %s:%s", HOST, PORT)

        while True:
            conn, address = server_socket.accept()

            # NEW: wait for an available client-handling slot.
            client_semaphore.acquire()

            try:
                client_thread = threading.Thread(
                    target=handle_client_with_limit,
                    args=(conn, address),
                    daemon=True,
                )
                client_thread.start()

            except Exception:
                conn.close()
                client_semaphore.release()
                raise


if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("\nBanking server stopped.")
        logger.info("Banking server stopped by keyboard interrupt.")
    except Exception:
        logger.exception("Banking server failed to start")
        print(
            "Banking server failed to start. "
            f"Check the log file: {LOG_PATH}"
        )
        raise

