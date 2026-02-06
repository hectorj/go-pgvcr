package pgvcr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"braces.dev/errtrace"
	"golang.org/x/sync/errgroup"
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

	// Create pipe acceptor instead of TCP listener
	pipeAcceptor := newPipeAcceptor(ctx)
	logDebug(ctx, "started pipe acceptor")

	eg, ctx := errgroup.WithContext(ctx)

	var serveFunc func() error
	if isRecording {
		serveFunc, err = s.recordingServer(ctx, pipeAcceptor)
	} else {
		serveFunc, err = s.replayingServer(ctx, pipeAcceptor, s.cfg.QueryOrderValidationStrategy)
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
		eg:           eg,
		cancelFn:     cancelFn,
		pipeAcceptor: pipeAcceptor,
	}, nil
}

type startedServer struct {
	eg           *errgroup.Group
	cancelFn     context.CancelCauseFunc
	pipeAcceptor *pipeAcceptor
}

type StartedServer = *startedServer

// DialFunc returns a function compatible with pgconn.Config.DialFunc
// that connects to this server via in-memory pipes
func (s *startedServer) DialFunc() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()

		req := pipeConnectionRequest{
			serverConn: serverConn,
		}

		select {
		case s.pipeAcceptor.connChan <- req:
			return clientConn, nil
		case <-ctx.Done():
			_ = clientConn.Close()
			_ = serverConn.Close()
			return nil, context.Cause(ctx)
		case <-s.pipeAcceptor.closeChan:
			_ = clientConn.Close()
			_ = serverConn.Close()
			return nil, net.ErrClosed
		}
	}
}

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

type serveFn func() error
