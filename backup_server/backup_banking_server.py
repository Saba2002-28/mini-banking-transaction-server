import json
import socket
import threading
import uuid

from database import initialize_database, get_balance
from replication_server import apply_replication

HOST = "0.0.0.0"
PORT = 5000
BUFFER_SIZE = 4096
MAX_MESSAGE_SIZE = 65536


def receive_message(conn):
    data = bytearray()

    while True:
        chunk = conn.recv(BUFFER_SIZE)

        if not chunk:
            if not data:
                return None
            raise ValueError("Incomplete request")

        data.extend(chunk)

        if len(data) > MAX_MESSAGE_SIZE:
            raise ValueError("Request too large")

        if b"\n" in data:
            message, _, extra = data.partition(b"\n")

            if extra:
                raise ValueError("Unexpected extra data")

            return json.loads(message.decode("utf-8"))


def handle_request(request):
    if not isinstance(request, dict):
        return {"status": "error", "message": "Invalid request"}

    action = request.get("action")
    account = request.get("account")

    if not isinstance(account, str) or not account.strip():
        return {"status": "error", "message": "Account ID is required"}

    account = account.strip()

    if action == "balance":
        balance = get_balance(account)

        if balance is None:
            return {"status": "error", "message": "Unknown account"}

        return {
            "status": "success",
            "account": account,
            "balance": balance,
        }

    if action not in ("deposit", "withdraw"):
        return {"status": "error", "message": "Unsupported action"}

    request_id = request.get("request_id")

    if request_id is None:
        request_id = str(uuid.uuid4())

    amount = request.get("amount")

    # Use the same transactional, idempotent operation handler
    # as the replication service.
    return apply_replication({
        "request_id": request_id,
        "action": action,
        "account": account,
        "amount": amount,
    })


def handle_client(conn, address):
    with conn:
        try:
            request = receive_message(conn)

            if request is None:
                return

            response = handle_request(request)

        except (ValueError, TypeError) as error:
            print(f"Invalid request from {address}: {error}")
            response = {
                "status": "error",
                "message": "Invalid request",
            }

        except Exception as error:
            print(f"Request from {address} failed: {error}")
            response = {
                "status": "error",
                "message": "Internal server error",
            }

        try:
            conn.sendall(
                json.dumps(response).encode("utf-8") + b"\n"
            )
        except OSError as error:
            print(f"Could not respond to {address}: {error}")

        print(f"Client {address[0]}: {request} -> {response}")


def main():
    initialize_database()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(10)

        print(f"Backup banking server listening on {HOST}:{PORT}")

        while True:
            conn, address = server.accept()
            threading.Thread(
                target=handle_client,
                args=(conn, address),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()
