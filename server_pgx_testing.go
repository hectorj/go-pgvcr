package pgvcr

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func NewPgxTestingServer(t testing.TB, options ...func(config *Config)) *pgxpool.Pool {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	cfg := Config{
		EchoFilePath: "testdata/" + t.Name() + ".pgvcr.gob",
	}
	for _, option := range options {
		option(&cfg)
	}
	server, err := NewServer(cfg).Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(
		func() {
			require.NoError(t, server.Stop())
		},
	)

	db, err := pgxpool.New(ctx, server.ConnectionString())
	require.NoError(t, err)

	return db
}
