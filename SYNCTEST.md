# Using Synctest Strategy

The `QueryOrderValidationStrategySynctest` provides fully deterministic replay of concurrent queries using Go's `testing/synctest` package.

## Requirements

- Go 1.24.2 or later (with synctest enabled by default)

## How It Works

The synctest strategy leverages `testing/synctest` to provide deterministic goroutine scheduling:

1. **Controlled Execution**: All goroutines run within a "bubble" where synctest controls their scheduling
2. **Fake Time**: Time advances only when all goroutines are blocked, eliminating race conditions
3. **No Timeouts Needed**: Unlike the stalling strategy, synctest doesn't need manual timeout logic
4. **Perfect Determinism**: Same code always produces same behavior, making tests fully reproducible

## Usage

### Basic Test

```go
package mypackage

import (
    "testing"
    "testing/synctest"
    "github.com/hectorj/go-pgvcr"
)

func TestMyCode(t *testing.T) {
    synctest.Run(func() {
        server, _ := pgvcr.NewServer(pgvcr.Config{
            EchoFilePath: "testdata/test.pgvcr.gob",
            QueryOrderValidationStrategy: pgvcr.QueryOrderValidationStrategySynctest,
            IsRecording: pgvcr.ForceReplaying,
        }).Start(ctx)

        // Configure pgx with DialFunc
        poolConfig, _ := pgxpool.ParseConfig("postgres://user:password@localhost/db")
        poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
        db, _ := pgxpool.NewWithConfig(ctx, poolConfig)

        // Run concurrent queries - fully deterministic!
        // ...
    })
}
```

### Running Tests with Synctest

```bash
# Run tests normally - synctest is enabled by default in Go 1.24.2+
go test -v ./...
```

## When to Use Synctest Strategy

**Use synctest when:**
- You have concurrent database queries
- You need 100% deterministic test behavior
- You want to eliminate flaky tests from race conditions

**Use stalling strategy when:**
- You have light concurrency (won't exhaust connection pool)
- You're okay with 3-second timeout limits

**Use strict strategy when:**
- You have no concurrent queries
- You want the simplest, fastest replay

## Comparison with Other Strategies

| Feature | Strict | Stalling | Synctest |
|---------|--------|----------|----------|
| Concurrent queries | ❌ No | ⚠️ Limited | ✅ Yes |
| Deterministic | ✅ Yes | ⚠️ Usually | ✅ Always |
| Timeouts | N/A | 3 seconds | None needed |
| Experimental | No | No | No |
| Performance | Fast | Medium | Fast |

## Implementation Details

The synctest implementation:
1. Uses the same replay hub as strict mode
2. Relies on synctest's goroutine scheduling for coordination
3. No stalling queue or timeout logic needed
4. Messages naturally arrive in the correct order due to controlled scheduling

## Known Limitations

1. **Test-Only**: Should not be used in production code (synctest is for testing only)
2. **Go Version**: Requires Go 1.24.2 or later with synctest support

## Example: Concurrent Query Test

See `server_synctest_test.go` for a full example of testing concurrent queries with synctest.

```go
func TestRun_Concurrency_Synctest(t *testing.T) {
    synctest.Run(func() {
        // Set up server with synctest strategy
        cfg := Config{
            EchoFilePath: "testdata/concurrency.pgvcr.gob",
            QueryOrderValidationStrategy: QueryOrderValidationStrategySynctest,
            IsRecording: ForceReplaying,
        }

        server, _ := NewServer(cfg).Start(ctx)
        defer server.Stop()

        // Configure pool with DialFunc
        poolConfig, _ := pgxpool.ParseConfig(cfg.ConnectionString)
        poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()
        db, _ := pgxpool.NewWithConfig(ctx, poolConfig)

        // Run 10 concurrent queries - all deterministic!
        var wg sync.WaitGroup
        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func(i int) {
                defer wg.Done()
                conn, _ := db.Acquire(ctx)
                defer conn.Release()

                row := conn.QueryRow(ctx, "SELECT $1::int", i)
                var result int
                row.Scan(&result)
                // Always produces same result in same order
            }(i)
        }
        wg.Wait()
    })
}
```

## Future Direction

The synctest strategy is the recommended approach for testing concurrent database code with pgvcr when you need deterministic behavior.
