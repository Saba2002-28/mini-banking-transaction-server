import math
import os
import sqlite3
import json

DB_NAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "bank.db",
)


def _connect():
    conn = sqlite3.connect(
        DB_NAME,
        timeout=10,
        isolation_level=None,
    )
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _validate_amount(amount):
    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or amount <= 0
    ):
        raise ValueError("Amount must be a positive finite number")


def initialize_database():
    conn = _connect()

    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("BEGIN IMMEDIATE")

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
                created_at TEXT NOT NULL DEFAULT (
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
            )
        """)

        initial_accounts = [
            ("A1001", 1000.00),
            ("A1002", 2500.00),
            ("A1003", 500.00),
	    ("A1004", 1500.00),
    	    ("A1005", 3000.00),

        ]

        # Existing balances are preserved.
        conn.executemany("""
            INSERT OR IGNORE INTO accounts (account_id, balance)
            VALUES (?, ?)
        """, initial_accounts)

        conn.execute("COMMIT")

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


def get_balance(account_id):
    conn = _connect()

    try:
        row = conn.execute(
            "SELECT balance FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()

        return float(row[0]) if row else None

    finally:
        conn.close()


def deposit(account_id, amount):
    _validate_amount(amount)

    conn = _connect()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cursor = conn.execute("""
            UPDATE accounts
            SET balance = balance + ?
            WHERE account_id = ?
        """, (amount, account_id))

        if cursor.rowcount == 0:
            conn.execute("ROLLBACK")
            return None

        row = conn.execute(
            "SELECT balance FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()

        new_balance = float(row[0])
        conn.execute("COMMIT")

        return new_balance

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


def withdraw(account_id, amount):
    _validate_amount(amount)

    conn = _connect()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cursor = conn.execute("""
            UPDATE accounts
            SET balance = balance - ?
            WHERE account_id = ?
              AND balance >= ?
        """, (amount, account_id, amount))

        if cursor.rowcount == 0:
            conn.execute("ROLLBACK")
            return None

        row = conn.execute(
            "SELECT balance FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()

        new_balance = float(row[0])
        conn.execute("COMMIT")

        return new_balance

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


def get_transaction(request_id):
    """Return a previously recorded request, if it exists."""
    conn = _connect()

    try:
        row = conn.execute("""
            SELECT action, account_id, amount, status, result_json, created_at
            FROM transactions
            WHERE request_id = ?
        """, (request_id,)).fetchone()

        if row is None:
            return None

        return {
            "request_id": request_id,
            "action": row[0],
            "account_id": row[1],
            "amount": row[2],
            "status": row[3],
            "result": json.loads(row[4]),
            "created_at": row[5],
        }

    finally:
        conn.close()


def record_transaction(
    request_id,
    action,
    account_id,
    amount,
    result,
):
    """
    Store a transaction result durably.

    This function records the result; it does not itself apply a deposit
    or withdrawal, and it does not replicate anything to another VM.
    """
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError("Request ID is required")

    if action not in ("balance", "deposit", "withdraw"):
        raise ValueError("Unsupported transaction action")

    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError("Account ID is required")

    if not isinstance(result, dict):
        raise ValueError("Result must be a dictionary")

    conn = _connect()

    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute("""
            SELECT result_json
            FROM transactions
            WHERE request_id = ?
        """, (request_id,)).fetchone()

        if existing is not None:
            conn.execute("ROLLBACK")
            return {
                "recorded": False,
                "duplicate": True,
                "result": json.loads(existing[0]),
            }

        conn.execute("""
            INSERT INTO transactions (
                request_id,
                action,
                account_id,
                amount,
                status,
                result_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            request_id,
            action,
            account_id,
            amount,
            result.get("status", "unknown"),
            json.dumps(result, ensure_ascii=False),
        ))

        conn.execute("COMMIT")

        return {
            "recorded": True,
            "duplicate": False,
            "result": result,
        }

    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()
