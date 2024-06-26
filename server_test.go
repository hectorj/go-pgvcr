package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	echoFilePath := "testdata/" + t.Name() + ".pgvcr.gob"

	testCases := []struct {
		name   string
		server Server
	}{
		{
			name: "recording",
			server: NewServer(
				Config{
					EchoFilePath: echoFilePath,
					IsRecording:  ForceRecording,
					Logger:       newTestLogger(t),
				},
			),
		},
		{
			name: "replaying",
			server: NewServer(
				Config{
					EchoFilePath:                 echoFilePath,
					IsRecording:                  ForceReplaying,
					Logger:                       newTestLogger(t),
					QueryOrderValidationStrategy: QueryOrderValidationStrategyStrict,
				},
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			t.Cleanup(cancelFn)

			server, err := tt.server.Start(ctx)
			require.NoError(t, err)

			t.Cleanup(func() { require.NoError(t, server.Stop()) })

			db, err := pgxpool.New(ctx, server.ConnectionString())
			require.NoError(t, err)

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

			// test empty value because of https://github.com/golang/go/issues/10905
			_, err = tx.Exec(ctx, `INSERT INTO test (value) VALUES ($1);`, "")
			require.NoError(t, err)

			// Testing copy from usage
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"test"}, []string{"value"}, pgx.CopyFromRows([][]interface{}{{"test2"}}))
			require.NoError(t, err)

			row = tx.QueryRow(ctx, `SELECT value FROM test ORDER BY id DESC LIMIT 1;`)
			var newVal string
			err = row.Scan(&newVal)
			require.NoError(t, err)

			require.Equal(t, "test2", newVal)

			err = tx.Rollback(ctx)
			require.NoError(t, err)

			db.Close()
		})
	}
}

func newTestLogger(_ *testing.T) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource:   true,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	})

	return slog.New(handler)
}

func TestRun_Concurrency(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	db := NewPgxTestingServer(t, func(config *Config) {
		config.QueryOrderValidationStrategy = QueryOrderValidationStrategyStalling
		config.Logger = newTestLogger(t)
	})

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		i := i
		eg.Go(
			func() error {
				t.Logf("acquiring connection %d", i)
				conn, err := db.Acquire(ctx)
				if !assert.NoError(t, err) {
					return errtrace.Wrap(err)
				}
				defer conn.Release()
				time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond) //nolint:gosec
				t.Logf("selecting on connection %d", i)
				row := conn.QueryRow(ctx, "SELECT $1::int -- query "+strconv.Itoa(i), i)
				t.Logf("received result on connection %d", i)
				var result int
				if !assert.NoError(t, row.Scan(&result)) {
					return errtrace.Wrap(err)
				}
				require.Equal(t, i, result)
				return nil
			},
		)
	}
	require.NoError(t, eg.Wait())
}
