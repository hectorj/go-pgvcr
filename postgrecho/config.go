package postgrecho

import (
	"context"
	"log/slog"
	"net"
	"os"
)

type Config struct {
	// EchoFilePath should be a path to a file we can read and write to store the mocking data.
	// Default value is "testdata/postgrecho.jsonl"
	EchoFilePath string
	// RealPostgresBuilder is a function returning a connection string to a real postgres database.
	// It will only be used when recording.
	// Default value uses "github.com/testcontainers/testcontainers-go/modules/postgres" to start a postgres in Docker.
	// Unused in replaying mode.
	RealPostgresBuilder func(ctx context.Context, cfg Config) (ConnectionString, error)
	// MustRecord is a function telling the server if it must write to the record or read it.
	// Default value decides to record if the file does not exist yet, and to replay if it does.
	IsRecording func(ctx context.Context, cfg Config) (bool, error)
	// Logger will be used by the server for all its logging.
	// It is meant to be an *slog.Logger.
	// Default value is a no-op logger.
	Logger *slog.Logger
	// QueryOrderValidationStrategy determines how to handle out-of-order queries in replay mode.
	// Default is QueryOrderValidationStrategyStalling
	QueryOrderValidationStrategy QueryOrderValidationStrategy
	// Listener
	// Default value listens on tcp://127.0.0.1: (random port)
	Listener net.Listener
}

type QueryOrderValidationStrategy string

const (
	// QueryOrderValidationStrategyStrict forbids out-of-order queries.
	// Probably won't work if you have concurrent queries, but provides more security if you don't.
	QueryOrderValidationStrategyStrict QueryOrderValidationStrategy = "strict"
	// QueryOrderValidationStrategyStalling handles out-of-order queries by stalling them until it is their turn.
	// This allows concurrent queries as long as your queries don't end up waiting on each other (when you have maxed out your connection pool for example).
	// A 3 seconds timeout is configured so that it doesn't hang forever.
	QueryOrderValidationStrategyStalling QueryOrderValidationStrategy = "stalling"
)

var defaultQueryOrderValidationStrategy = QueryOrderValidationStrategyStalling

const defaultEchoFilePath = "testdata/postgrecho.jsonl"

func ForceRecording(_ context.Context, _ Config) (bool, error) {
	return true, nil
}

func ForceReplaying(_ context.Context, _ Config) (bool, error) {
	return false, nil
}

func ReplayIfRecordExists(_ context.Context, cfg Config) (bool, error) {
	_, err := os.Stat(cfg.EchoFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

var defaultIsRecording = ReplayIfRecordExists
