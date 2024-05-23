package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sync/errgroup"
	"io"
	"net"
	"net/url"
	"sync/atomic"
)

func (s *server) recordingServer(ctx context.Context, listener net.Listener) (serveFn, ConnectionString, error) {
	targetConnectionString, err := s.cfg.RealPostgresBuilder(ctx, s.cfg)
	if err != nil {
		return nil, "", errtrace.Wrap(err)
	}
	targetURL, err := url.Parse(targetConnectionString)
	if err != nil {
		return nil, "", errtrace.Wrap(err)
	}
	targetHost := targetURL.Host
	targetURL.Host = listener.Addr().String()
	newConnectionString := targetURL.String()

	// We need to add an atomic counter to be used as a connection ID
	var connectionIDCounter uint64 = 0

	serveFunc := func() error {
		ctx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		r := &recorder{}
		defer func() {
			err := r.FlushToFile(s.cfg.EchoFilePath)
			if err != nil {
				s.cfg.Logger.ErrorContext(ctx, fmt.Sprintf("%+v", err))
			}
		}()

		mainEg, _ := errgroup.WithContext(ctx)

		for ctx.Err() == nil {
			backendConn, err := listener.Accept()
			if err != nil {
				return errtrace.Wrap(err)
			}

			_ = context.AfterFunc(ctx, func() {
				_ = backendConn.Close()
			})

			frontendConn, err := net.Dial("tcp", targetHost)
			if err != nil {
				return errtrace.Wrap(err)
			}

			_ = context.AfterFunc(ctx, func() {
				_ = frontendConn.Close()
			})

			// Use atomic to increment the counter safely
			backend := recordingPgprotoBackend{
				connectionID: atomic.AddUint64(&connectionIDCounter, 1),
				backend:      pgproto3.NewBackend(backendConn, backendConn),
				backendConn:  backendConn,
				frontend:     pgproto3.NewFrontend(frontendConn, frontendConn),
				frontendConn: frontendConn,
				recorder:     r,
			}

			mainEg.Go(func() error {
				err = backend.Run()
				if err != nil {
					return errtrace.Wrap(err)
				}
				return nil
			})
		}

		return errtrace.Wrap(mainEg.Wait())
	}
	return serveFunc, newConnectionString, nil
}

type recordingPgprotoBackend struct {
	connectionID uint64

	backend     *pgproto3.Backend
	backendConn net.Conn

	frontend     *pgproto3.Frontend
	frontendConn net.Conn

	recorder *recorder
}

func (b *recordingPgprotoBackend) handleStartup() error {
	startupMessage, err := b.backend.ReceiveStartupMessage()
	if err != nil {
		return errtrace.Wrap(fmt.Errorf("error receiving startup message: %w", err))
	}

	switch startupMessage.(type) {
	case *pgproto3.SSLRequest:
		// deny SSL
		_, err = b.backendConn.Write([]byte("N"))
		if err != nil {
			return errtrace.Errorf("error sending deny SSL request: %w", err)
		}
		return errtrace.Wrap(b.handleStartup())
	default:
		// forward startup message
		b.frontend.Send(startupMessage)
		err = b.frontend.Flush()
		if err != nil {
			return errtrace.Wrap(err)
		}
	}

	return nil
}

func (b *recordingPgprotoBackend) Run() error {
	defer b.Close()

	err := b.handleStartup()
	if err != nil {
		return errtrace.Wrap(err)
	}

	eg, _ := errgroup.WithContext(context.TODO())

	eg.Go(func() error {
		for {
			msg, err := b.backend.Receive()

			if err != nil {
				return errtrace.Errorf("error receiving message (backend): %w", err)
			}
			err = b.recorder.Record(b.connectionID, msg, true)
			if err != nil {
				return errtrace.Wrap(err)
			}

			b.frontend.Send(msg)
			err = b.frontend.Flush()
			if err != nil {
				return errtrace.Errorf("error sending message (frontend): %w", err)
			}
		}
	})

	eg.Go(func() error {
		for {
			msg, err := b.frontend.Receive()
			if err != nil {
				return errtrace.Errorf("error receiving message (frontend): %w", err)
			}
			err = b.recorder.Record(b.connectionID, msg, false)
			if err != nil {
				return errtrace.Wrap(err)
			}
			err = b.backend.SetAuthType(b.frontend.GetAuthType())
			if err != nil {
				return errtrace.Wrap(err)
			}

			b.backend.Send(msg)
			err = b.backend.Flush()
			if err != nil {
				return errtrace.Errorf("error sending message (backend): %w", err)
			}
		}
	})

	err = errtrace.Wrap(eg.Wait())
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return errtrace.Wrap(err)
}

func (b *recordingPgprotoBackend) Close() error {
	return errtrace.Wrap(b.backendConn.Close())
}
