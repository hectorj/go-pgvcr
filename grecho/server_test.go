package grecho_test

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/hectorj/echo/grecho"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type testStruct struct {
	A string `json:"A"`
}

func TestRun_Tx(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"
	recordingServer := NewServer(
		Config{
			EchoFilePath: echoFilePath,
			IsRecording:  ForceRecording,
		},
	)
	replayingServer := NewServer(
		Config{
			EchoFilePath: echoFilePath,
			IsRecording:  ForceReplaying,
		},
	)

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

			_, err = db.Exec(ctx, `INSERT INTO t (data) VALUES ($1)`, testStruct{A: "test"})
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
	db := NewPgxTestingServer(t)

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		i := i
		eg.Go(
			func() error {
				conn, err := db.Acquire(ctx)
				require.NoError(t, err)
				defer conn.Release()
				time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond) //nolint:gosec
				row := conn.QueryRow(ctx, "SELECT $1::int -- query "+strconv.Itoa(i), i)
				var result int
				require.NoError(t, row.Scan(&result))
				require.Equal(t, i, result)
				return nil
			},
		)
	}
	require.NoError(t, eg.Wait())
}

func TestRun_CopyFrom(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	db := NewPgxTestingServer(t)

	_, err := db.Exec(ctx, `CREATE TABLE t (id serial PRIMARY KEY, data int[] NOT NULL)`)
	require.NoError(t, err)

	rowsCount, err := db.CopyFrom(
		ctx, pgx.Identifier{"t"}, []string{"data"}, pgx.CopyFromRows(
			[][]any{
				{
					[]int{4, 4, 2},
				},
				{
					[]int{3, 5, 2},
				},
				{
					[]int{7, 7, 7},
				},
			},
		),
	)
	require.NoError(t, err)
	require.Equal(t, int64(3), rowsCount)

	row := db.QueryRow(ctx, `SELECT COUNT(*) FROM t`)
	require.NoError(t, row.Scan(&rowsCount))
	require.Equal(t, int64(3), rowsCount)
}

func TestRun_Notify(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	db := NewPgxTestingServer(t)

	poolConn, err := db.Acquire(ctx)
	require.NoError(t, err)

	conn := poolConn.Conn()

	_, err = conn.Exec(ctx, `listen mychannel`)
	require.NoError(t, err)

	_, err = db.Exec(ctx, `notify mychannel, 'testPayload'`)
	require.NoError(t, err)

	notif, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)

	require.Equal(t, "testPayload", notif.Payload)
}

func TestNoMatch(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath: echoFilePath,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	if server.IsRecording() {
		row := db.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		row := db.QueryRow(ctx, "SELECT 2;")
		require.Error(t, row.Scan(&result))
	}
}

func TestStallingOutOfOrderDeadlock(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategyStalling,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	if server.IsRecording() {
		row := db.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = db.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		startTime := time.Now()
		row := db.QueryRow(ctx, "SELECT 2;")
		require.Error(t, row.Scan(&result))
		require.Less(t, time.Since(startTime), time.Second*15)
	}
}

func TestStallingOutOfOrderEventually(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategyStalling,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	conn1, err := db.Acquire(ctx)
	require.NoError(t, err)
	defer conn1.Release()
	conn2, err := db.Acquire(ctx)
	require.NoError(t, err)
	defer conn2.Release()
	if server.IsRecording() {
		row := conn1.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = conn2.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		go func() {
			time.Sleep(3 * time.Second)
			var result int
			row := conn1.QueryRow(ctx, "SELECT 1;")
			require.NoError(t, row.Scan(&result))
		}()
		row := conn2.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))

	}
}

func TestStrictOutOfOrderDeadlock(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategyStrict,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	if server.IsRecording() {
		row := db.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = db.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		startTime := time.Now()
		row := db.QueryRow(ctx, "SELECT 2;")
		require.Error(t, row.Scan(&result))
		require.Less(t, time.Since(startTime), time.Second)
	}
}

func TestExtraConnection(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategyStalling,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	conn1, err := db.Acquire(ctx)
	require.NoError(t, err)
	defer conn1.Release()
	if server.IsRecording() {
		row := conn1.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = conn1.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		conn2, err := db.Acquire(ctx)
		require.NoError(t, err)
		defer conn2.Release()

		row := conn1.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = conn2.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))

	}
}

func TestExtraPings(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	echoFilePath := "testdata/" + t.Name() + ".grecho.jsonl"

	server, err := NewServer(
		Config{
			EchoFilePath:                 echoFilePath,
			QueryOrderValidationStrategy: QueryOrderValidationStrategyStalling,
		},
	).Start(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, server.Stop()) }()

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	var result int
	if server.IsRecording() {
		row := db.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		row = db.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))
		t.SkipNow()
	} else {
		row := db.QueryRow(ctx, "SELECT 1;")
		require.NoError(t, row.Scan(&result))
		require.NoError(t, db.Ping(ctx))
		require.NoError(t, db.Ping(ctx))
		require.NoError(t, db.Ping(ctx))
		row = db.QueryRow(ctx, "SELECT 2;")
		require.NoError(t, row.Scan(&result))

	}
}
