# go-pgvcr Project Analysis Notes

## Overview
**Project**: go-pgvcr
**Repository**: github.com/hectorj/go-pgvcr
**Purpose**: A VCR-style testing library for PostgreSQL that records database interactions once and replays them without needing a real PostgreSQL server
**Language**: Go 1.24.2
**License**: Present (LICENSE file exists)

## Core Concept
The library emulates a real PostgreSQL server using in-memory pipes (`net.Pipe()`), allowing any PostgreSQL driver to connect to it. It operates in two modes:
1. **Recording Mode**: Acts as a proxy between client and real PostgreSQL server (via pipes), recording all messages
2. **Replaying Mode**: Replays recorded messages without needing a real database

## Key Benefits
- Faster test execution than real PostgreSQL
- Lower resource requirements (ideal for CI)
- Less manual work than mocks
- SQL queries tested at least once against real server
- Works with any PostgreSQL driver (protocol-level implementation)
- No TCP overhead - all connections via in-memory pipes
- No port conflicts - no need for OS port allocation
- Synctest-ready architecture for deterministic concurrent testing

## Architecture

### Main Components

1. **server.go** - Core server implementation
   - `Server` type: Main entry point
   - `NewServer(cfg Config)`: Creates server with config defaults
   - `Start(ctx)`: Returns `StartedServer` with pipe acceptor
   - `StartedServer.Wait()`: Blocks until server stops
   - `StartedServer.Stop()`: Gracefully stops server
   - `StartedServer.DialFunc()`: Returns pgconn-compatible dial function for connecting via pipes

2. **config.go** - Configuration
   - `Config` struct with fields:
     - `EchoFilePath`: Path to recording file (default: "testdata/pgvcr.gob")
     - `RealPostgresBuilder`: Function to provide real PostgreSQL connection string
     - `IsRecording`: Function to determine record vs replay mode
     - `Logger`: slog.Logger for debugging
     - `QueryOrderValidationStrategy`: How to handle concurrent queries
     - `ConnectionString`: Template connection string for pgx configuration (default: "postgres://user:password@localhost/db?sslmode=disable")
   - Query validation strategies:
     - `QueryOrderValidationStrategyStrict`: Sequential only, no concurrency
     - `QueryOrderValidationStrategyStalling`: Handles out-of-order by stalling (3s timeout)
   - Helper functions: `ForceRecording`, `ForceReplaying`, `ReplayIfRecordExists`

3. **pipe_server.go** - In-memory pipe networking
   - `pipeAcceptor`: Implements `net.Listener` using channels and `net.Pipe()`
   - `pipeConnectionRequest`: Represents a new pipe connection request
   - `pipeAddr`: Implements `net.Addr` for pipe connections
   - Enables connection via in-memory pipes instead of TCP

4. **recording_server.go** - Recording mode implementation
   - Establishes dual connections (client↔proxy↔server)
   - Uses `recordingPgprotoBackend` to handle both sides
   - Records all messages via `recorder`
   - Handles SSL denial (denies SSL, falls back to plaintext)
   - Uses atomic counter for connection IDs

5. **replaying_server.go** - Replay mode entry point
   - Reads recorded messages from file
   - Creates `replayer` with greetings and records
   - Delegates to `startReplayHub`

6. **replaying_hub.go** - Complex replay orchestration
   - `replayingHub`: Central coordinator for multiple connections
   - Manages connection ID mapping (recorded→replaying)
   - Handles concurrent queries via stalling mechanism
   - Main loops:
     - `acceptConnectionsLoop`: Accepts new client connections
     - `receiveMessagesLoop`: Reads messages from each client
     - `mainLoop`: Consumes recorded messages and matches/replays
   - Message matching validates exact byte equality
   - Supports special handling for ping queries (filters them out)
   - Error reporting via PostgreSQL wire protocol

7. **recorder.go** - Recording functionality
   - `recorder` type with gob encoder
   - `Record(connectionID, msg, isIncoming)`: Records single message
   - `FlushToFile(filepath)`: Writes buffer to disk
   - Thread-safe with mutex
   - Creates directory if necessary (added in recent commit: 8776a78)

8. **replayer.go** - Replay functionality
   - `replayer` type with message cursor
   - `ConsumeGreetings(fn)`: Processes connection greetings
   - `ConsumeNext(fn)`: Consumes next recorded message
   - Thread-safe with mutexes

9. **reader.go** - Record file parsing
   - `readMessages(filepath)`: Reads gob-encoded messages
   - Filters out auth, ping, and terminate messages
   - Splits greetings (ParameterStatus, BackendKeyData, initial ReadyForQuery) from other messages
   - Handles gob serialization quirk with empty slices (Go issue #10905)

10. **postgres_builder.go** - Real PostgreSQL provisioning
   - `PostgresBuilderFallback`: Tries multiple builders in order
   - `PostgresBuilderViaEnvVar`: Uses environment variable (default: PGVCR_CONNECTION_STRING)
   - `PostgresBuilderViaTestContainers`: Spins up Docker container (postgres:15.2-alpine)

11. **gob.go** - Message type registration
    - Registers all pgproto3 message types with gob encoding
    - Required for serialization/deserialization

11. **logger.go** - Logging utilities
    - `NoopHandler`: Default silent logger
    - Context-based logger storage
    - Helper functions: `logDebug`, `logError`

### Data Structures

- `messageWithID`: Records a message with connection ID and byte encoding
- `recordedConnectionID`: ID during recording (uint64)
- `replayingConnectionID`: ID during replay (uint64)
- `ConnectionString`: Type alias for DSN string

## Dependencies

### Direct Dependencies
- `github.com/jackc/pgx/v5`: PostgreSQL wire protocol implementation
- `github.com/testcontainers/testcontainers-go`: Docker container management for tests
- `braces.dev/errtrace`: Enhanced error tracing
- `golang.org/x/sync/errgroup`: Concurrent error handling
- `dagger.io/dagger`: CI/CD pipeline definitions
- `github.com/stretchr/testify`: Test assertions

## Testing

### Test Files
- **server_test.go**: Main integration tests
  - `TestServer`: Tests both recording and replaying modes
  - Uses pgxpool for connection pooling
  - Tests: CREATE TABLE, INSERT, SELECT, transactions, CopyFrom
  - Includes empty string test (Go issue #10905)

### CI/CD
- **GitHub Actions** (.github/workflows/dagger.yml)
  - Runs on every push
  - Uses Go 1.24.2
  - Executes Dagger pipeline

- **Dagger Pipeline** (ci/main.go)
  - Runs in parallel:
    1. EditorConfig checker (npx editorconfig-checker)
    2. golangci-lint (v1.54.2)
    3. Tests with real PostgreSQL container
  - Uses errgroup for parallel execution

### Linting
- **golangci-lint** (.golangci.yml)
  - Enabled linters: errcheck, gosimple, govet, ineffassign, staticcheck, unused, asasalint, contextcheck, decorder, dogsled, dupl, exhaustive, exportloopref, goconst, godox, gosec, musttag, nilerr

## File Organization
```
/
├── config.go               # Configuration types and defaults
├── server.go               # Main server interface
├── recording_server.go     # Recording mode implementation
├── replaying_server.go     # Replay mode entry point
├── replaying_hub.go        # Replay orchestration (complex)
├── recorder.go             # Recording to file
├── replayer.go             # Replay from memory
├── reader.go               # Read/parse recording files
├── postgres_builder.go     # Real PostgreSQL provisioning
├── gob.go                  # Gob type registration
├── logger.go               # Logging utilities
├── server_test.go          # Integration tests
├── server_pgx_testing.go   # Test helpers
├── ci/main.go              # Dagger CI pipeline
├── testdata/               # Recording files (*.pgvcr.gob)
├── .github/workflows/      # GitHub Actions
├── .golangci.yml           # Linter config
├── .editorconfig           # Editor config
└── go.mod                  # Go module definition
```

## Recent Changes (from git log)
1. **8776a78**: Create EchoFilePath's dir if necessary
2. **87a8c56**: Upgrade Github Actions' versions
3. **cf00f31**: Upgrade Go version in CI
4. **144cb53**: Don't fail when server was just asked to stop
5. **3b2c139**: Use new test context

## Usage Pattern

```go
// Create server
server := pgvcr.NewServer(pgvcr.Config{
    EchoFilePath: "testdata/mytest.pgvcr.gob",
    IsRecording: pgvcr.ReplayIfRecordExists, // Default: record if file missing
    Logger: slog.Default(),
})

// Start server
started, err := server.Start(ctx)
// Use started.ConnectionString() to connect

// Stop when done
err = started.Stop()
```

## Edge Cases & Special Handling

1. **SSL Requests**: Denied with 'N' byte response
2. **Ping Queries**: Filtered out (`-- ping` query string)
3. **Authentication**: Filtered from recordings (sends AuthenticationOk automatically)
4. **Terminate Messages**: Filtered from recordings
5. **Empty Slices**: Both message and byte encoding stored (Go gob issue #10905)
6. **Concurrent Queries**: Stalling strategy with 3-second timeout
7. **Connection Cleanup**: Uses `context.AfterFunc` for cleanup
8. **Error Context**: Uses errtrace for enhanced stack traces

## Known Limitations

1. SSL not supported (intentionally denied for simplicity)
2. Ping queries must match exact format: `-- ping`
3. Query order matters (even with stalling, can deadlock connection pool)
4. Recording files are binary (gob format), not human-readable
5. No incremental updates to recordings (must re-record entire file)

## Performance Characteristics

- **Recording**: Adds minimal overhead (proxying + encoding)
- **Replaying**: Much faster than real PostgreSQL (no disk I/O, no query execution)
- **Memory**: Entire recording loaded into memory for replay
- **Concurrency**: Supports concurrent connections with stalling strategy

## Key APIs for External Users

- `pgvcr.NewServer(Config) Server`
- `Server.Start(context.Context) (StartedServer, error)`
- `StartedServer.ConnectionString() string`
- `StartedServer.Stop() error`
- `StartedServer.Wait() error`

## Typical Test Workflow

1. First run: Recording mode, connects to real PostgreSQL
2. Recording file created (testdata/*.pgvcr.gob)
3. Subsequent runs: Replay mode, no PostgreSQL needed
4. To re-record: Delete recording file

## Implementation Details

### Recording Flow
1. Client connects to pgvcr listener
2. pgvcr creates connection to real PostgreSQL
3. All messages proxied bidirectionally
4. Each message recorded with connection ID and direction flag
5. On server shutdown, recording flushed to gob file

### Replaying Flow
1. Recording file loaded and parsed on startup
2. Messages split into greetings and queries
3. Client connects to pgvcr listener
4. pgvcr sends AuthenticationOk + greeting messages
5. For each incoming message:
   - Verify it matches next expected message (byte equality)
   - Send corresponding recorded response messages
   - Track connection state for concurrent query handling
6. If message mismatch and stalling enabled:
   - Queue mismatched message
   - Try consuming another recorded message
   - Timeout after 3 seconds

### Connection ID Mapping
- Recording uses sequential atomic counter
- Replay maps recorded IDs to replaying connection IDs
- Tracks which connection is executing which recorded command
- Ensures messages from multi-connection recordings route correctly

### Message Filtering
Messages filtered during replay setup:
- **Auth messages**: Replaced with simple AuthenticationOk
- **Ping queries**: Handled inline without replay
- **Terminate**: Connection close, no replay needed
- **Greetings**: Sent once per connection (ParameterStatus, BackendKeyData, ReadyForQuery)

## Thread Safety

- `recorder`: Protected by mutex during record and flush
- `replayer`: Separate mutexes for greetings and main messages
- `replayingHub`: Connection map protected by mutex
- Individual connection sends protected by per-connection mutex

## Error Handling Philosophy

- Uses `braces.dev/errtrace` for wrapped errors with stack traces
- Wraps all errors at every level
- Special error types for control flow (constError)
- Errors sent to clients as PostgreSQL ErrorResponse messages
- Context cancellation used for graceful shutdown

## Testing Strategy

- Integration tests run in both modes (record + replay)
- Same test code validates both modes behave identically
- Uses testcontainers for real PostgreSQL in CI
- Can use env var for external PostgreSQL instance
- Test recording files committed to repo

This project is production-ready with comprehensive testing, CI/CD, and active maintenance.

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`

## Recent Major Changes (February 2026)

### Pipe-Based Architecture Migration

The project has been migrated from TCP-based connections to in-memory pipes (`net.Pipe()`):

**What Changed:**
- Removed TCP listener and port allocation
- Implemented `pipeAcceptor` that acts as a `net.Listener` using channels
- Changed API: `StartedServer.ConnectionString()` → `StartedServer.DialFunc()`
- Added `Config.ConnectionString` field (template for pgx configuration)
- Removed `Config.Listener` field

**New Usage Pattern:**
```go
server, _ := pgvcr.NewServer(cfg).Start(ctx)
poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
db, _ := pgxpool.NewWithConfig(ctx, poolConfig)
```

**Benefits:**
- No TCP overhead - faster tests
- No port conflicts
- Synctest-ready for future deterministic concurrent testing
- Simpler architecture

**Testing with Docker:**
To run recording tests locally, use Docker with PostgreSQL 15.2-alpine:
```bash
docker run --rm -e POSTGRES_DB=db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:15.2-alpine
export PGVCR_CONNECTION_STRING="postgres://user:password@localhost:5432/db?sslmode=disable"
go test -v
```

Or use the CI pipeline: `go run ./ci`
