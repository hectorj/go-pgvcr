package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"net"
	"strings"
)

type server struct {
	cfg Config
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
		cfg.Logger = slog.New(NoopHandler{})
	}
	cfg.Logger = cfg.Logger.WithGroup("pgvcr")

	return &server{
		cfg: cfg,
	}
}

type Server interface {
	Start(ctx context.Context) (StartedServer, error)
}

func (s *server) Start(ctx context.Context) (StartedServer, error) {
	ctx, cancelFn := context.WithCancel(ctx)

	if s.cfg.Logger != nil {
		ctx = contextWithLogger(ctx, s.cfg.Logger)
	}

	isRecording, err := s.cfg.IsRecording(ctx, s.cfg)
	if err != nil {
		cancelFn()
		return nil, errtrace.Wrap(err)
	}

	listener := s.cfg.Listener
	if listener == nil {
		listener, err = net.Listen("tcp", "localhost:")
		logDebug(ctx, "started listening", slog.String("address", listener.Addr().String()))
		if err != nil {
			cancelFn()
			return nil, errtrace.Wrap(err)
		}
	}
	_ = context.AfterFunc(ctx, func() {
		err := listener.Close()
		if err != nil {
			s.cfg.Logger.Error(err.Error(), slog.Any("error", err))
		}
	})

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
		return nil, errtrace.Wrap(err)
	}
	eg.Go(serveFunc)

	return &startedServer{
		eg:               eg,
		connectionString: connectionString,
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
	return errtrace.Wrap(s.eg.Wait())
}

func (s *startedServer) Stop() error {
	s.cancelFn()
	err := s.Wait()
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}
	return errtrace.Wrap(err)
}

func (s *startedServer) ConnectionString() ConnectionString {
	return s.connectionString
}

type serveFn func() error
