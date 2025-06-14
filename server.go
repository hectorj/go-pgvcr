package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"errors"
	"fmt"
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

type Server = *server

func (s *server) Start(ctx context.Context) (StartedServer, error) {
	ctx, cancelFn := context.WithCancelCause(ctx)

	if s.cfg.Logger != nil {
		ctx = contextWithLogger(ctx, s.cfg.Logger)
	}

	isRecording, err := s.cfg.IsRecording(ctx, s.cfg)
	if err != nil {
		cancelFn(errtrace.Wrap(err))
		return nil, errtrace.Wrap(err)
	}

	listener := s.cfg.Listener
	if listener == nil {
		listener, err = net.Listen("tcp", "localhost:")
		logDebug(ctx, "started listening", slog.String("address", listener.Addr().String()))
		if err != nil {
			cancelFn(errtrace.Wrap(err))
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
		serveFunc, connectionString, err = s.replayingServer(ctx, listener, s.cfg.QueryOrderValidationStrategy)
	}
	if err != nil {
		cancelFn(errtrace.Wrap(err))
		return nil, errtrace.Wrap(err)
	}
	eg.Go(func() error {
		var err error
		defer func() { cancelFn(errtrace.Wrap(err)) }()
		err = serveFunc()
		return errtrace.Wrap(err)
	})

	return &startedServer{
		eg:               eg,
		connectionString: connectionString,
		cancelFn:         cancelFn,
	}, nil
}

type startedServer struct {
	connectionString ConnectionString
	eg               *errgroup.Group
	cancelFn         context.CancelCauseFunc
}

type StartedServer = *startedServer

func (s *startedServer) Wait() error {
	err := s.eg.Wait()
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) || errors.Is(err, ErrServerStopped) {
		err = nil
	}
	if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}
	if err != nil {
		err = fmt.Errorf("pgvcr: %w", err)
	}
	return errtrace.Wrap(err)
}

const ErrServerStopped = constError("server was asked to stop")

func (s *startedServer) Stop() error {
	s.cancelFn(errtrace.Wrap(ErrServerStopped))
	return errtrace.Wrap(s.Wait())
}

func (s *startedServer) ConnectionString() ConnectionString {
	return s.connectionString
}

type serveFn func() error
