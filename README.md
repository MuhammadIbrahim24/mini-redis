# Mini Redis

A small concurrent in-memory key-value store written in Go. It provides an interactive CLI with worker-pool processing, key expiration, and JSON file persistence.

## Requirements

- Go 1.26.5 or compatible

## Run

From the project root:

```powershell
go run .
```

The application loads existing data from `cached_data.json` when it starts and saves changes when you exit.

## Commands

Enter one command per line:

| Command | Description | Example |
| --- | --- | --- |
| `SET key value` | Store a value | `SET name Ibrahim` |
| `SET key value EX seconds` | Store a value with expiration | `SET session active EX 60` |
| `GET key` | Read a value | `GET name` |
| `DEL key` | Delete a key | `DEL name` |
| `EXISTS key` | Check whether a non-expired key exists | `EXISTS name` |
| `COUNT` | Count non-expired keys | `COUNT` |
| `KEYS` | List non-expired keys | `KEYS` |
| `EXIT` | Stop the application and persist changes | `EXIT` |

Values may contain spaces. Expired keys are ignored by reads, counts, and key listings, and are removed periodically by the expiration loop.

## Example session

```text
SET user Ibrahim
GET user
SET temporary value EX 10
EXISTS temporary
COUNT
KEYS
EXIT
```

Commands that are handled asynchronously report a job ID and either `SUCCESS` or `FAILED`.

## Project layout

- `main.go` - Starts the CLI, worker pool, expiration loop, and persistence loop.
- `command/` - Parses and validates CLI commands.
- `store/` - Thread-safe in-memory storage, expiration, and persistence scheduling.
- `persistence/` - JSON file storage implementation.
- `work/` - Jobs, results, workers, and the worker pool.
- `cached_data.json` - Runtime persistence file.

## Test

Run the full test suite with:

```powershell
go test ./...
```

For race detection:

```powershell
go test -race ./...
```
