package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"errors"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log/slog"
	"os"
	"time"
)

func PostgresBuilderFallback(builders ...func(context.Context, Config) (ConnectionString, error)) func(
	context.Context,
	Config,
) (ConnectionString, error) {
	return func(ctx context.Context, cfg Config) (ConnectionString, error) {
		var errs error
		for _, builder := range builders {
			cs, err := builder(ctx, cfg)
			if err == nil {
				return cs, nil
			}
			errs = errors.Join(errs, err)
		}
		return "", errtrace.Wrap(fmt.Errorf("all postgres builders failed: %w", errs))
	}
}

func PostgresBuilderViaEnvVar(envKey string) func(_ context.Context, _ Config) (ConnectionString, error) {
	return func(_ context.Context, _ Config) (ConnectionString, error) {
		dsn := os.Getenv(envKey)
		if dsn == "" {
			return "", errtrace.Wrap(fmt.Errorf("env var %q not found", envKey))
		}
		return dsn, nil
	}
}

func PostgresBuilderViaTestContainers(ctx context.Context, cfg Config) (ConnectionString, error) {
	postgresContainer, err := postgres.RunContainer(
		ctx,
		testcontainers.CustomizeRequest(
			testcontainers.GenericContainerRequest{
				Logger: slog.NewLogLogger(
					cfg.Logger.Handler(),
					slog.LevelDebug,
				),
			},
		),
		testcontainers.WithImage("docker.io/postgres:15.2-alpine"),
		postgres.WithDatabase("db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		return "", errtrace.Wrap(err)
	}

	return errtrace.Wrap2(postgresContainer.ConnectionString(ctx, "sslmode=disable"))
}
