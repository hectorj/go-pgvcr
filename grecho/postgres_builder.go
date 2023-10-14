package grecho

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var defaultPostgresBuilder = PostgresBuilderFallback(
	PostgresBuilderViaEnvVar,
	PostgresBuilderViaTestContainers,
)

type PostgresAddr = string

func PostgresBuilderFallback(builders ...func(context.Context, Config) (PostgresAddr, error)) func(
	context.Context,
	Config,
) (PostgresAddr, error) {
	return func(ctx context.Context, cfg Config) (PostgresAddr, error) {
		var errs error
		for _, builder := range builders {
			cs, err := builder(ctx, cfg)
			if err == nil {
				return cs, nil
			}
			errs = errors.Join(errs, err)
		}
		return "", fmt.Errorf("all postgres builders failed: %w", errs)
	}
}

func PostgresBuilderViaEnvVar(_ context.Context, _ Config) (PostgresAddr, error) {
	const envVar = "grecho_TARGET_ADDR"
	dsn := os.Getenv(envVar)
	if dsn == "" {
		return "", fmt.Errorf("env var %q not found", envVar)
	}
	return dsn, nil
}

func PostgresBuilderViaTestContainers(ctx context.Context, cfg Config) (PostgresAddr, error) {
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
		return "", err
	}

	containerPort, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", err
	}

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(host, containerPort.Port()), nil
}
