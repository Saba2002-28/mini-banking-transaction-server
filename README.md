# Mini Banking Transaction Server

## Overview

This project implements a simulated banking system using a client-server architecture over TCP sockets. It supports basic banking operations and uses SQLite for data storage. The system includes a primary server, a backup server, and a client, with support for concurrent client connections and transaction replication.

## Features

- **Balance inquiry:** Retrieve an account's current balance.
- **Deposits and withdrawals:** Perform basic banking transactions.
- **Concurrent clients:** Handle multiple client connections using threads.
- **Data storage:** Store banking data in an SQLite database.
- **Backup and replication:** Support replication of transactions to a backup server.
- **Client failover:** Configure the client to connect to the primary or backup server.
- **Error handling and logging:** Handle request errors and record server activity.

## Requirements

- Python 3.10 or newer recommended.
- No third-party Python packages are required.
- SQLite is accessed through Python's standard library.

## Project Structure

```text
mini-banking-transaction-server/
├── primary_server/
│   ├── server.py
│   └── database.py
├── client/
│   └── client.py
├── backup_server/
│   ├── backup_banking_server.py
│   ├── replication_server.py
│   └── database.py
├── .gitignore
├── requirements.txt
└── README.md
```

**File descriptions:**

- `client.py` — Sends banking requests to the server and supports switching between primary and backup servers.
- `server.py` — Implements the primary banking server.
- `backup_banking_server.py` — Implements the backup banking server.
- `replication_server.py` — Receives and applies replicated transaction data.
- `database.py` — Provides database initialization and banking database operations.
- `config.json` — Contains server configuration values, if used by the implementation.
- `requirements.txt` — Documents the project's Python dependency requirements.

The SQLite database and log files may be created when the application runs. Their locations depend on the implementation and configuration.

## How to Run

The system is designed to run across separate virtual machines.

1. **Prepare the virtual machines.** Install Python on the machines and ensure they can communicate over the network.
2. **Configure the addresses.** Update the server IP addresses and port settings in the relevant Python files to match your virtual machines. The server port used by the current configuration is `5000`.
3. **Start the backup server.** On the backup server VM, start the replication components as required by the implementation to send the snapshot to the rpimary server:

   ```bash
   python replication_server.py
   ```

4. **Start the primary server.** At the first time for initializing the database run:

   ```bash
   python3 -c "from database import initialize_database; initialize_database();               print('Database initialized successfully')"
   ```
 then primary server VM, run:

   ```bash
   python3 server.py
   ```
5. **Start the backup server.** At the first time for initializing the database on the backup server VM, copy the bank.db created on the primary server VM to the backup server VM and run:

   ```bash
   python3 -c "from database import initialize_database, get_balance; print('A1001:', get_balance('A1001')); print('A1002:', get_balance('A1002')); print('A1003:', get_balance('A1003')); print('A1004:', get_balance('A1004')); print('A1005:', get_balance('A1005'))"
   ```
   to make sure of the in sync databases. restart the server.py while the replication server is still running to get the snapshot of the database created.
   then run:
   ```bash
   python backup_banking_server.py
   ```
   
5. **Start the client.** On the client VM, run:

   ```bash
   python client.py
   ```

Run each command in the appropriate project directory. Ensure that the server addresses, listening ports, and replication settings match across the machines before starting the application.

## Dependencies

All currently used Python modules belong to the standard library. No external packages need to be installed.

## Notes and Limitations

- This is an educational simulation; accounts and transactions do not involve real money.
- Ensure that the virtual machines have network connectivity and that the required ports are accessible.
- Transaction consistency, replication, failover, and recovery behavior should be verified through testing.
