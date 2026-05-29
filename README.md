# Radis - Redis Server in Go

Radis is a Redis-compatible server built in Go. It implements a subset of Redis over TCP using the RESP protocol, with support for in-memory key-value storage, list operations, and basic master-replica replication.

## What Has Been Built

### RESP Protocol

The server includes a custom RESP parser and serializer supporting simple strings, errors, integers, bulk strings, null bulk strings, arrays, and null arrays.

### Core Commands

Implemented commands:

- `PING`
- `ECHO`
- `SET`
- `GET`

`SET` supports optional expiry:

- `EX <seconds>`
- `PX <milliseconds>`

### List Commands

Implemented list operations:

- `RPUSH`
- `LPUSH`
- `LRANGE`
- `LLEN`
- `LPOP`

Lists are stored in memory and support positive and negative indexes for `LRANGE`.

### Replication

The server can run as either a master or replica.

Master support includes `INFO replication`, `REPLCONF`, `PSYNC`, full sync using an empty RDB payload, connected replica tracking, `SET` propagation, replica ACK tracking, and the `WAIT` command.

Replica support includes connecting to a master with `--replicaof`, performing the replication handshake, receiving the initial RDB payload, applying propagated commands, and responding to `REPLCONF GETACK *`.

Partial resync/backlog structures exist, but the current behavior uses full resync for `PSYNC`.

## Project Structure

```text
.
├── main.go
├── radis/
│   ├── radis-server.go
│   ├── commands.go
│   ├── list-commands.go
│   ├── master.go
│   ├── replica.go
│   ├── util.go
│   └── resp/
│       └── resp.go
├── empty
└── *_test.go
```

## Running Locally

Start a master:

```sh
go run . --port 6379
```

Start a replica:

```sh
go run . --port 6380 --replicaof "127.0.0.1 6379"
```

Connect using `redis-cli`:

```sh
redis-cli -p 6379
```

Example commands:

```sh
PING
ECHO hello
SET name radis
GET name
RPUSH items one two three
LRANGE items 0 -1
```

## Running Tests

```sh
go test ./...
```

## Notes

Radis is intentionally small and incomplete compared to Redis. Data is stored in memory, persistence is not implemented beyond sending an empty RDB payload during full sync, and replication currently focuses on full resync plus command propagation.

