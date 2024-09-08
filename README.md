# Redis-like Server Implementation

This repository contains a custom implementation of a Redis-like server, supporting key-value store functionalities, persistence with RDB file parsing, and basic master-slave replication.

## Features

- **Key-Value Store**: Supports setting and getting keys with optional expiry times.
- **Persistence**: Parses and stores data using a custom RDB file format.
- **Master-Slave Replication**: Can act as a master or slave, supporting replication with basic commands like `PSYNC`.
- **RESP Protocol**: Communicates with clients using the Redis Serialization Protocol (RESP).
- **Multithreaded**: Handles multiple client connections using threads.

## Getting Started

### Prerequisites

- GCC or any C compiler
- POSIX-compatible operating system (e.g., Linux, macOS)

### Compilation

To compile the server, use the following command:

```bash
gcc server.c -o redis_server
./redis_server.out
