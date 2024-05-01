package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"net"
	"slices"
	"time"
)

func (s *server) replayingServer(ctx context.Context, listener net.Listener) (serveFn, ConnectionString, error) {
	greetings, records, err := readMessages(s.cfg.EchoFilePath)
	if err != nil {
		return nil, "", errtrace.Wrap(err)
	}
	r := &replayer{
		messages:  records,
		greetings: greetings,
	}

	connectionString := "postgresql://user:password@" + listener.Addr().String() + "/db?sslmode=disable"

	serveFn := func() error {
		ctx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		eg, _ := errgroup.WithContext(ctx)

		connectionID := 0

		for ctx.Err() == nil {
			backendConn, err := listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if err != nil {
				return errtrace.Wrap(err)
			}

			_ = context.AfterFunc(ctx, func() {
				_ = backendConn.Close()
			})

			backend := replayingBackend{
				backend:                      pgproto3.NewBackend(backendConn, backendConn),
				backendConn:                  backendConn,
				replayer:                     r,
				queryOrderValidationStrategy: s.cfg.QueryOrderValidationStrategy,
				logger:                       s.cfg.Logger.With(slog.Int("connectionID", connectionID)),
			}
			connectionID++

			eg.Go(func() error {
				return errtrace.Wrap(backend.Run(ctx))
			})
		}

		return errtrace.Wrap(eg.Wait())
	}

	return serveFn, connectionString, nil
}

type replayingBackend struct {
	backend                      *pgproto3.Backend
	backendConn                  net.Conn
	queryOrderValidationStrategy QueryOrderValidationStrategy
	replayer                     *replayer
	logger                       *slog.Logger
}

func (p *replayingBackend) handleStartup() error {
	p.logger.Debug("expecting startup message")
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return errtrace.Errorf("error receiving startup message: %w", err)
	}
	p.logger.Debug("received startup message", slog.Any("startupMessage", startupMessage))

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
		if err != nil {
			return errtrace.Wrap(err)
		}
		p.logger.Debug("sending AuthenticationOk")
		_, err = p.backendConn.Write(buf)
		if err != nil {
			return errtrace.Wrap(err)
		}
	case *pgproto3.SSLRequest:
		p.logger.Debug("refusing SSL")
		_, err = p.backendConn.Write([]byte("N"))
		if err != nil {
			return errtrace.Wrap(err)
		}
		return errtrace.Wrap(p.handleStartup())
	default:
		return errtrace.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *replayingBackend) Run(ctx context.Context) error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return errtrace.Wrap(err)
	}

	err = func() error {
		err := p.replayer.ConsumeGreetings(func(msgs []messageWithID) error {
			for _, msg := range msgs {
				p.backend.Send(msg.Message.(pgproto3.BackendMessage))
			}
			return errtrace.Wrap(p.backend.Flush())
		})
		if err != nil {
			return errtrace.Wrap(err)
		}
		for err == nil {
			err = p.replayer.ConsumeNext(func(msg messageWithID) error {
				if ctx.Err() != nil {
					return errtrace.Wrap(ctx.Err())
				}
				if msg.IsIncoming {
					return errtrace.Wrap(p.processIncomingMessage(ctx, msg))
				}
				p.logger.Debug("sending outgoing recorded message", slog.Any("message", msg))
				p.backend.Send(msg.Message.(pgproto3.BackendMessage))
				return errtrace.Wrap(p.backend.Flush())
			})
		}
		if errors.Is(err, errNoMoreMessages) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errtrace.Wrap(err)
	}()
	if err != nil {
		// Send error back on the wire as SQL error
		p.backend.Send(&pgproto3.ErrorResponse{
			Severity:            "FATAL",
			SeverityUnlocalized: "FATAL",
			Code:                "XX000",
			Message:             err.Error(),
		})
		_ = p.backend.Flush()
		return errtrace.Wrap(err)
	}
	return nil
}

func (p *replayingBackend) processIncomingMessage(ctx context.Context, msg messageWithID) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	p.logger.Debug("expecting incoming message", slog.Any("expectedMessage", msg))
	_ = p.backendConn.SetReadDeadline(time.Now().Add(time.Minute))
	actualMessage, err := p.backend.Receive()
	if err != nil {
		return errtrace.Wrap(err)
	}
	p.logger.Debug("received incoming message", slog.Any("incomingMessage", actualMessage))

	if isPingQuery(actualMessage) {
		p.logger.Debug("sending ping response")
		p.backend.Send(&pgproto3.EmptyQueryResponse{})
		p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		err = p.backend.Flush()
		if err != nil {
			return errtrace.Wrap(err)
		}
		return p.processIncomingMessage(ctx, msg)
	}

	var expectedBytes, actualBytes []byte
	expectedBytes, err = msg.Message.Encode(expectedBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	actualBytes, err = actualMessage.Encode(actualBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	if !slices.Equal(expectedBytes, actualBytes) {
		return errtrace.Errorf("unexpected message expected(%#v) != actual(%#v)", msg.Message, actualMessage)
	}
	return nil
}

func (p *replayingBackend) Close() error {
	return errtrace.Wrap(p.backendConn.Close())
}
