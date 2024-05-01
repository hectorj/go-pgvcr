package main

import (
	"context"
	"fmt"
	"os"

	"dagger.io/dagger"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := build(context.Background()); err != nil {
		fmt.Println(err)
	}
}

func build(ctx context.Context) error {
	fmt.Println("Building with Dagger")

	// initialize Dagger client
	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stderr))
	if err != nil {
		return err
	}
	defer client.Close()

	src := client.Host().Directory(
		".", dagger.HostDirectoryOpts{
			Exclude: []string{".git/", ".idea/", "ci/"},
		},
	)

	goMod := client.Host().File("./go.mod")
	goSum := client.Host().File("./go.sum")
	golang := client.Container().From("golang:1.21.2").WithWorkdir("/src")
	golangWithDependencies := golang.WithFile("/src/go.mod", goMod).WithFile("/src/go.sum", goSum).
		WithExec([]string{"go", "mod", "download"})

	node := client.Container().From("node:20.8.0").WithWorkdir("/src")

	editorconfigChecker := node.WithDirectory("/src", src).
		WithExec([]string{"npx", "editorconfig-checker@5.1.1"})

	golangciLint := golangWithDependencies.WithExec(
		[]string{
			"go",
			"install",
			"github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2",
		},
	).
		WithDirectory("/src", src).
		WithExec([]string{"golangci-lint", "run"})

	postgres := client.Container().From("docker.io/postgres:15.2-alpine").
		WithEnvVariable("POSTGRES_DB", "db").
		WithEnvVariable("POSTGRES_USER", "user").
		WithEnvVariable("POSTGRES_PASSWORD", "password")

	tests := golangWithDependencies.WithServiceBinding("postgres", postgres).
		WithEnvVariable("PGVCR_CONNECTION_STRING", "postgres://user:password@postgres:5432/db?sslmode=disable").
		WithDirectory("/src", src).
		WithExec([]string{"go", "test", "./..."})

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(sync(ctx, editorconfigChecker))
	eg.Go(sync(ctx, golangciLint))
	eg.Go(sync(ctx, tests))

	return eg.Wait()
}

func sync(ctx context.Context, container *dagger.Container) func() error {
	return func() error {
		_, err := container.Sync(ctx)
		return err
	}
}
