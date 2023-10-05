package postgrecho

import (
	"context"
	wire "github.com/jeroenrinzema/psql-wire"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(noopHandler{})
	}
	cfg.Logger = cfg.Logger.WithGroup("postgrecho")

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

	var wireServer *wire.Server
	if isRecording {
		wireServer, err = s.buildRecordingWireServer(ctx)
	} else {
		wireServer, err = s.buildReplayingWireServer(ctx)
	}
	if err != nil {
		cancelFn()
		return nil, err
	}

	if s.cfg.Listener == nil {
		s.cfg.Listener, err = net.Listen("tcp", "127.0.0.1:")
		if err != nil {
			cancelFn()
			return nil, err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		<-ctx.Done()
		return wireServer.Close()
	})

	eg.Go(func() error { return wireServer.Serve(s.cfg.Listener) })

	return &startedServer{
		wireServer:       wireServer,
		eg:               eg,
		connectionString: "postgresql://user:password@" + s.cfg.Listener.Addr().String() + "/db?sslmode=disable",
		cancelFn:         cancelFn,
	}, nil
}

type startedServer struct {
	wireServer       *wire.Server
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
