package postgrecho_test

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/hectorj/echo/postgrecho"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

type testStruct struct {
	A string `json:"A"`
}

func TestRun(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	recordingServer := NewServer(Config{
		EchoFilePath: "testdata/echo.jsonl",
		IsRecording:  ForceRecording,
		//Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		//	AddSource:   true,
		//	Level:       slog.LevelDebug,
		//	ReplaceAttr: nil,
		//})),
	})
	replayingServer := NewServer(Config{
		EchoFilePath: "testdata/echo.jsonl",
		IsRecording:  ForceReplaying,
		//Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		//	AddSource:   true,
		//	Level:       slog.LevelDebug,
		//	ReplaceAttr: nil,
		//})),
	})

	for _, server := range []Server{recordingServer, replayingServer} {
		func() {
			server, err := server.Start(ctx)
			require.NoError(t, err)

			defer func() { require.NoError(t, server.Stop()) }()

			db, err := pgxpool.New(ctx, server.ConnectionString())
			require.NoError(t, err)

			require.NoError(t, db.Ping(ctx))

			_, err = db.Exec(ctx, `CREATE TABLE t (id SERIAL PRIMARY KEY, data JSONB NOT NULL)`)
			require.NoError(t, err)

			tx, err := db.Begin(ctx)
			require.NoError(t, err)

			jsonData, err := json.Marshal(testStruct{A: "test"})
			require.NoError(t, err)
			_, err = db.Exec(ctx, `INSERT INTO t (data) VALUES ($1)`, jsonData)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `INSERT INTO t (data) VALUES ($1)`, testStruct{A: "test2"})
			require.NoError(t, err)

			row := tx.QueryRow(ctx, `SELECT * FROM t ORDER BY id DESC LIMIT 1`)
			var id int
			var data map[string]any
			require.NoError(t, row.Scan(&id, &data))
			require.Equal(t, "test2", data["A"].(string))

			require.NoError(t, tx.Rollback(ctx))

			row = tx.QueryRow(ctx, `SELECT * FROM t ORDER BY id DESC LIMIT 1`)
			err = row.Scan(&id, &data)
			require.True(t, errors.Is(err, pgx.ErrTxClosed))

			row = db.QueryRow(ctx, `SELECT * FROM t ORDER BY id DESC LIMIT 1`)
			require.NoError(t, row.Scan(&id, &data))
			require.Equal(t, "test", data["A"].(string))

			row = db.QueryRow(ctx, `SELECT * FROM t ORDER BY id DESC LIMIT 0`)
			err = row.Scan(&id, &data)
			require.True(t, errors.Is(err, pgx.ErrNoRows))
		}()
	}
}

func TestRun_Concurrency(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	server, err := NewServer(Config{
		EchoFilePath: "testdata/echo_concurrency.jsonl",
		//Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		//	AddSource:   true,
		//	Level:       slog.LevelDebug,
		//	ReplaceAttr: nil,
		//})),
	}).Start(ctx)
	require.NoError(t, err)

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		i := i
		eg.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
			row := db.QueryRow(ctx, "SELECT $1::int -- query "+strconv.Itoa(i), i)
			var result int
			require.NoError(t, row.Scan(&result))
			require.Equal(t, i, result)
			return nil
		})
	}
	require.NoError(t, eg.Wait())
}

func TestRun_CopyFrom(t *testing.T) {
	t.Skip("not supported yet")
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	server, err := NewServer(Config{
		EchoFilePath: "testdata/echo_copyfrom.jsonl",
		//Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		//	AddSource:   true,
		//	Level:       slog.LevelDebug,
		//	ReplaceAttr: nil,
		//})),
	}).Start(ctx)
	require.NoError(t, err)

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	_, err = db.Exec(ctx, `CREATE TABLE t (id serial PRIMARY KEY, data int[] NOT NULL)`)
	require.NoError(t, err)

	rowsCount, err := db.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"data"}, pgx.CopyFromRows([][]any{
		{
			[]any{
				[]int{4, 4, 2},
			},
			[]any{
				[]int{3, 5, 2},
			},
			[]any{
				[]int{7, 7, 7},
			},
		},
	}))
	require.NoError(t, err)
	require.Equal(t, 3, rowsCount)

	row := db.QueryRow(ctx, `SELECT COUNT(*) FROM t`)
	require.NoError(t, row.Scan(&rowsCount))
	require.Equal(t, 3, rowsCount)
}
