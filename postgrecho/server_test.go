package postgrecho_test

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/hectorj/echo/postgrecho"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"

	"github.com/stretchr/testify/require"
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
	})
	replayingServer := NewServer(Config{
		EchoFilePath: "testdata/echo.jsonl",
		IsRecording:  ForceReplaying,
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
