
import json
import socket
import threading
import uuid

PRIMARY_IP = "10.194.197.59" # Address of primary server
BACKUP_IP = "10.194.197.134" # Address of backup server
SERVER_PORT = 5000
BUFFER_SIZE = 1024
MAX_MESSAGE_SIZE = 65536
HEALTH_CHECK_INTERVAL = 3

ACTIVE_SERVER_IP = PRIMARY_IP

# Signals that the primary is reachable again.
PRIMARY_AVAILABLE = threading.Event()


def receive_message(client):
    data = bytearray()

    while True:
        chunk = client.recv(BUFFER_SIZE)

        if not chunk:
            raise ConnectionError(
                "Server closed before completing its response"
            )

        data.extend(chunk)

        if len(data) > MAX_MESSAGE_SIZE:
            raise ValueError("Response is too large")

        if b"\n" in data:
            message, _, extra = data.partition(b"\n")

            if extra:
                raise ValueError(
                    "Unexpected extra data after one response"
                )

            return json.loads(message.decode("utf-8"))


def send_to_server(request, server_ip):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.settimeout(5)
        client.connect((server_ip, SERVER_PORT))

        message = json.dumps(request).encode("utf-8") + b"\n"
        client.sendall(message)

        return receive_message(client)

########## Checking of primary server is available or not ############

def primary_is_reachable():
    try:
        with socket.create_connection(
            (PRIMARY_IP, SERVER_PORT), timeout=2
        ):
            return True
    except OSError:
        return False

############# Sending message to client if primary server has returned ###########

def monitor_primary(stop_event):
    """Check periodically whether the primary has returned."""

    while not stop_event.wait(HEALTH_CHECK_INTERVAL):
        if ACTIVE_SERVER_IP != BACKUP_IP:
            continue

        if PRIMARY_AVAILABLE.is_set():
            continue

        if primary_is_reachable():
            PRIMARY_AVAILABLE.set()
            print(
                "\nPrimary server is reachable again. "
                "The client will switch back on its next request."
            )

############### Request ################

def send_request(request):
    global ACTIVE_SERVER_IP

    # Switch back only between requests, not during an operation.
    if (
        ACTIVE_SERVER_IP == BACKUP_IP
        and PRIMARY_AVAILABLE.is_set()
    ):
        ACTIVE_SERVER_IP = PRIMARY_IP
        PRIMARY_AVAILABLE.clear()
        print(f"\nSwitching back to primary server {PRIMARY_IP}...")

    try:
        return send_to_server(request, ACTIVE_SERVER_IP)

    except (OSError, TimeoutError) as error:
        if ACTIVE_SERVER_IP != PRIMARY_IP:
            raise

        print(f"\nPrimary server {PRIMARY_IP} is unreachable: {error}")
        print(f"Switching to backup server {BACKUP_IP}...")

        ACTIVE_SERVER_IP = BACKUP_IP
        PRIMARY_AVAILABLE.clear()

        # Retry the same request with its original request ID.
        return send_to_server(request, ACTIVE_SERVER_IP)


def main():
    stop_event = threading.Event()

    monitor = threading.Thread(
        target=monitor_primary,
        args=(stop_event,),
        daemon=True,
    )
    monitor.start()

    print("Mini Banking Client")
    print(f"Initially connecting to primary: {PRIMARY_IP}")
    print("Available actions: balance, deposit, withdraw")
    print("Type 'exit' to quit.")

    try:
        while True:
            action = input("\nAction: ").strip().lower()

            if action == "exit":
                break

            if action not in ("balance", "deposit", "withdraw"):
                print("Invalid action. Try again.")
                continue

            account = input("Account ID (e.g. A1001): ").strip()
            request = {"action": action, "account": account}

            if action in ("deposit", "withdraw"):
                amount_text = input("Amount: ").strip()
                
                # Check whether the amount was entered.
                if not amount_text:
                    print("Please enter an amount. The request is incomplete.")
                    continue
                
                try:
                    request["amount"] = float(amount_text)
                except ValueError:
                    print("Amount must be numeric.")
                    continue

                # Generate once; retain the ID during failover retry.
                request["request_id"] = str(uuid.uuid4())

            try:
                response = send_request(request)
                print(f"Active banking server: {ACTIVE_SERVER_IP}")
                print("Server response:")
                print(json.dumps(response, indent=4))

            except (OSError, TimeoutError, ValueError) as error:
                print("Communication error:", error)

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                print("Invalid response received from server:", error)

    finally:
        stop_event.set()
        monitor.join(timeout=1)


if __name__ == "__main__":
    main()

