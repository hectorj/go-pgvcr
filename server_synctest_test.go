package pgvcr

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Regression test for https://github.com/hectorj/go-pgvcr/issues/2:
// processIncomingMessage under QueryOrderValidationStrategySynctest used to push
// the mismatched message onto stalledMessages *before* recursing. Since
// getNextIncomingMessage pops stalledMessages LIFO first, the recursive call
// kept getting the same message back and the goroutine stack-overflowed.
func TestReplayingHub_SynctestMismatch_DoesNotInfiniteRecurse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &replayingHub{
			replayer:                     &replayer{},
			incomingMessagesChan:         make(chan incomingMessage),
			connectionIDsMap:             make(map[recordedConnectionID]replayingConnectionMetadata),
			connectionsLock:              sync.Mutex{},
			queryOrderValidationStrategy: QueryOrderValidationStrategySynctest,
		}

		// Prime a stalled message that won't match what we expect.
		h.stalledMessages = append(h.stalledMessages, incomingMessage{
			connectionID: 0,
			message:      &pgproto3.Query{String: "SELECT 1"},
		})

		expected := messageWithID{
			ConnectionID: 0,
			IsIncoming:   true,
			Message:      &pgproto3.Query{String: "SELECT 2"},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := h.processIncomingMessage(ctx, expected)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrMessageMismatch), "expected ErrMessageMismatch, got: %v", err)
	})
}

func TestRun_Concurrency_Synctest(t *testing.T) {
	const echoFilePath = "testdata/TestRun_Concurrency_Synctest.pgvcr.gob"

	runTest := func(t *testing.T, ctx context.Context, isRecording func(ctx context.Context, cfg Config) (bool, error)) {
		cfg := Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategySynctest,
			IsRecording:                  isRecording,
		}

		server, err := NewServer(cfg).Start(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, server.Stop()) }()

		poolConfig, err := pgxpool.ParseConfig("postgres://user:password@localhost/db?sslmode=disable")
		require.NoError(t, err)

		poolConfig.HealthCheckPeriod = time.Hour
		poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()

		db, err := pgxpool.NewWithConfig(ctx, poolConfig)
		require.NoError(t, err)
		defer db.Close()

		eg, ctx := errgroup.WithContext(ctx)
		for i := 0; i < 10; i++ {
			i := i
			eg.Go(func() error {
				conn, err := db.Acquire(ctx)
				if err != nil {
					return err
				}
				defer conn.Release()

				row := conn.QueryRow(ctx, "SELECT $1::int -- query "+strconv.Itoa(i), i)
				var result int
				if err := row.Scan(&result); err != nil {
					return err
				}
				require.Equal(t, i, result)
				return nil
			})
		}
		require.NoError(t, eg.Wait())
	}

	t.Run("recording", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping recording test in short mode")
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		defer cancelFn()

		runTest(t, ctx, ForceRecording)
	})

	t.Run("replaying", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()

			runTest(t, ctx, ForceReplaying)
		})
	})
}

func TestServer_Synctest(t *testing.T) {
	const echoFilePath = "testdata/TestServer_Synctest.pgvcr.gob"

	runTest := func(t *testing.T, ctx context.Context, isRecording func(ctx context.Context, cfg Config) (bool, error)) {
		cfg := Config{
			EchoFilePath:                 echoFilePath,
			IsRecording:                  isRecording,
			QueryOrderValidationStrategy: QueryOrderValidationStrategySynctest,
		}

		server, err := NewServer(cfg).Start(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, server.Stop()) }()

		poolConfig, err := pgxpool.ParseConfig("postgres://user:password@localhost/db?sslmode=disable")
		require.NoError(t, err)

		poolConfig.HealthCheckPeriod = time.Hour
		poolConfig.ConnConfig.Config.DialFunc = server.DialFunc()

		db, err := pgxpool.NewWithConfig(ctx, poolConfig)
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t, db.Ping(ctx))

		tx, err := db.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `CREATE TABLE test ( id serial primary key, value text not null );`)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `INSERT INTO test (value) VALUES ($1);`, "testvalue")
		require.NoError(t, err)

		row := tx.QueryRow(ctx, `SELECT value FROM test LIMIT 1;`)
		var val string
		err = row.Scan(&val)
		require.NoError(t, err)

		require.Equal(t, "testvalue", val)

		err = tx.Rollback(ctx)
		require.NoError(t, err)
	}

	t.Run("recording", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping recording test in short mode")
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		defer cancelFn()

		runTest(t, ctx, ForceRecording)
	})

	t.Run("replaying", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()

			runTest(t, ctx, ForceReplaying)
		})
	})
}
