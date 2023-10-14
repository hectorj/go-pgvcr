package grecho

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type ConnectionString = string

type server struct {
	cfg Config
}

type Server interface {
	Start(ctx context.Context) (StartedServer, error)
}

func NewServer(cfg Config) Server {
	if cfg.EchoFilePath == "" {
		cfg.EchoFilePath = defaultEchoFilePath
	}
	if cfg.RealPostgresBuilder == nil {
		cfg.RealPostgresBuilder = defaultPostgresBuilder
	}
	if cfg.IsRecording == nil {
		cfg.IsRecording = defaultIsRecording
	}
	if cfg.QueryOrderValidationStrategy == "" {
		cfg.QueryOrderValidationStrategy = defaultQueryOrderValidationStrategy
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(noopHandler{})
	}
	cfg.Logger = cfg.Logger.WithGroup("grecho")

	return &server{
		cfg: cfg,
	}
}

// Start is a non-blocking function which starts the server.
func (s *server) Start(ctx context.Context) (StartedServer, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	isRecording, err := s.cfg.IsRecording(ctx, s.cfg)
	if err != nil {
		cancelFn()
		return nil, err
	}

	listener := s.cfg.Listener
	if listener == nil {
		listener, err = net.Listen("tcp", "127.0.0.1:")
		if err != nil {
			cancelFn()
			return nil, err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	var serveFunc func() error
	if isRecording {
		serveFunc, err = s.recordingServer(ctx, listener)
	} else {
		serveFunc, err = s.replayingServer(ctx, listener)
	}
	if err != nil {
		cancelFn()
		return nil, err
	}
	eg.Go(serveFunc)

	eg.Go(
		func() error {
			<-ctx.Done()
			return listener.Close()
		},
	)

	return &startedServer{
		eg:               eg,
		connectionString: "postgresql://user:password@" + listener.Addr().String() + "/db?sslmode=disable",
		cancelFn:         cancelFn,
	}, nil
}

type startedServer struct {
	connectionString ConnectionString
	eg               *errgroup.Group
	cancelFn         context.CancelFunc
}

type StartedServer interface {
	Wait() error
	Stop() error
	ConnectionString() ConnectionString
}

func (s *startedServer) Wait() error {
	return s.eg.Wait()
}

func (s *startedServer) Stop() error {
	s.cancelFn()
	err := s.Wait()
	if strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}
	return err
}

func (s *startedServer) ConnectionString() ConnectionString {
	return s.connectionString
}

func wireSessionHandler() func(ctx context.Context) (context.Context, error) {
	sessionIDCounter := &atomic.Int64{}
	return func(ctx context.Context) (context.Context, error) {
		sessionID := sessionIDCounter.Add(1) - 1
		ctx = context.WithValue(ctx, sessionIDCtxKey, sessionID)
		return ctx, nil
	}

}

type ctxKey string

const sessionIDCtxKey ctxKey = "sessionID"
