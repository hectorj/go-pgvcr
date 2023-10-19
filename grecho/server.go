package grecho

import (
	"context"
	"log/slog"
	"net"
	"strings"

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
		listener, err = net.Listen("tcp", "localhost:")
		if err != nil {
			cancelFn()
			return nil, err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	var (
		serveFunc        func() error
		connectionString ConnectionString
	)
	if isRecording {
		serveFunc, connectionString, err = s.recordingServer(ctx, listener)
	} else {
		serveFunc, connectionString, err = s.replayingServer(ctx, listener)
	}
	if err != nil {
		cancelFn()
		return nil, err
	}
	eg.Go(serveFunc)

	eg.Go(
		func() error {
			<-ctx.Done()
			_ = listener.Close()
			return nil
		},
	)

	return &startedServer{
		eg:               eg,
		connectionString: connectionString,
		cancelFn:         cancelFn,
		isRecording:      isRecording,
	}, nil
}

type startedServer struct {
	connectionString ConnectionString
	eg               *errgroup.Group
	cancelFn         context.CancelFunc
	isRecording      bool
}

type StartedServer interface {
	Wait() error
	Stop() error
	ConnectionString() ConnectionString
	IsRecording() bool
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

func (s *startedServer) IsRecording() bool {
	return s.isRecording
}

func (s *startedServer) ConnectionString() ConnectionString {
	return s.connectionString
}
