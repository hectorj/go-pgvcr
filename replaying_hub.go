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
	"sync"
	"time"
)

type incomingMessage struct {
	connectionID uint64
	message      pgproto3.Message
}

type replayingConnection struct {
	connectionID   uint64
	conn           net.Conn
	pgprotoBackend *pgproto3.Backend
}

type replayingHub struct {
	listener             net.Listener
	replayer             *replayer
	connections          []replayingConnection
	incomingMessagesChan chan incomingMessage
	connectionIDsMap     map[uint64]uint64
	lock                 sync.Mutex
}

func startReplayHub(ctx context.Context, r *replayer, listener net.Listener) error {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	h := &replayingHub{
		listener:             listener,
		replayer:             r,
		connections:          make([]replayingConnection, 0, 1),
		incomingMessagesChan: make(chan incomingMessage),
		connectionIDsMap:     make(map[uint64]uint64),
		lock:                 sync.Mutex{},
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return errtrace.Wrap(h.acceptConnectionsLoop(ctx))
	})

	eg.Go(func() error {
		return errtrace.Wrap(h.mainLoop(ctx))
	})

	return errtrace.Wrap(eg.Wait())
}

func (h *replayingHub) acceptConnectionsLoop(ctx context.Context) error {
	connectionID := uint64(0)
	for ctx.Err() == nil {
		conn, err := h.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			return errtrace.Wrap(err)
		}

		_ = context.AfterFunc(ctx, func() {
			_ = conn.Close()
		})

		rConn := replayingConnection{
			connectionID:   connectionID,
			conn:           conn,
			pgprotoBackend: pgproto3.NewBackend(conn, conn),
		}

		h.lock.Lock()
		h.connections = append(h.connections, rConn)
		h.lock.Unlock()

		go h.receiveMessagesLoop(ctx, rConn)

		connectionID++
	}
	return errtrace.Wrap(ctx.Err())
}

func (h *replayingHub) receiveMessagesLoop(ctx context.Context, conn replayingConnection) {
	err := h.handleStartup(ctx, conn)
	if errIsClosedConnection(err) {
		return
	}
	if err != nil {
		logError(ctx, errtrace.Errorf("handling connection startup: %w", err))
		// TODO: close connection? send error on wire?
		return
	}

	for ctx.Err() == nil {
		msg, err := conn.pgprotoBackend.Receive()
		if errIsClosedConnection(err) {
			return
		}
		if err != nil {
			logError(ctx, errtrace.Errorf("receiving message: %w", err))
			// TODO: close connection? send error on wire?
			return
		}
		if isTerminate(msg) {
			return
		}
		if isPingQuery(msg) {
			logDebug(ctx, "sending ping response") // FIXME
			conn.pgprotoBackend.Send(&pgproto3.EmptyQueryResponse{})
			conn.pgprotoBackend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			err := conn.pgprotoBackend.Flush()
			if err != nil {
				logError(ctx, errtrace.Errorf("sending ping response: %w", err))
				// TODO: close connection? send error on wire?
				return
			}
			continue
		}
		select {
		case h.incomingMessagesChan <- incomingMessage{
			connectionID: conn.connectionID,
			message:      msg,
		}:
		case <-ctx.Done():
			return
		}
	}
}
func errIsClosedConnection(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || (err != nil && err.Error() == io.EOF.Error())
}

func isTerminate(msg pgproto3.FrontendMessage) bool {
	_, ok := msg.(*pgproto3.Terminate)
	return ok
}

func (h *replayingHub) handleStartup(ctx context.Context, conn replayingConnection) error {
	logDebug(ctx, "expecting startup message")
	startupMessage, err := conn.pgprotoBackend.ReceiveStartupMessage()
	if err != nil {
		return errtrace.Errorf("error receiving startup message: %w", err)
	}
	logDebug(ctx, "received startup message", slog.Any("startupMessage", startupMessage))

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
		if err != nil {
			return errtrace.Wrap(err)
		}
		logDebug(ctx, "sending AuthenticationOk")
		_, err = conn.conn.Write(buf)
		if err != nil {
			return errtrace.Wrap(err)
		}

		err = h.replayer.ConsumeGreetings(func(msgs []messageWithID) error {
			for _, msg := range msgs {
				conn.pgprotoBackend.Send(msg.Message.(pgproto3.BackendMessage))
			}
			return errtrace.Wrap(conn.pgprotoBackend.Flush())
		})
		if err != nil {
			return errtrace.Wrap(err)
		}
	case *pgproto3.SSLRequest:
		logDebug(ctx, "refusing SSL")
		_, err = conn.conn.Write([]byte("N"))
		if err != nil {
			return errtrace.Wrap(err)
		}
		return errtrace.Wrap(h.handleStartup(ctx, conn))
	default:
		return errtrace.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (h *replayingHub) mainLoop(ctx context.Context) error {
	for ctx.Err() == nil {
		err := h.replayer.ConsumeNext(func(expectedMessage messageWithID) error {
			if expectedMessage.IsIncoming {
				err := h.processIncomingMessage(ctx, expectedMessage)
				if err != nil {
					h.sendErrOnWire(ctx, expectedMessage.ConnectionID, err)
				}
				return errtrace.Wrap(err)
			}

			return errtrace.Wrap(h.processOutgoingMessage(expectedMessage))
		})
		if errors.Is(err, errNoMoreMessages) {
			return nil
		}
		if err != nil {
			return errtrace.Wrap(err)
		}
	}
	return errtrace.Wrap(ctx.Err())
}

func (h *replayingHub) sendErrOnWire(_ context.Context, recordedConnectionID uint64, err error) {
	connection := h.getConnection(recordedConnectionID)
	connection.pgprotoBackend.Send(&pgproto3.ErrorResponse{
		Severity:            "FATAL",
		SeverityUnlocalized: "FATAL",
		Code:                "22000",
		Message:             "pgvcr error",
		Detail:              err.Error(),
		Hint:                "",
		Position:            0,
		InternalPosition:    0,
		InternalQuery:       "",
		Where:               "",
		SchemaName:          "",
		TableName:           "",
		ColumnName:          "",
		DataTypeName:        "",
		ConstraintName:      "",
		File:                "",
		Line:                0,
		Routine:             "",
		UnknownFields:       nil,
	})
	_ = connection.pgprotoBackend.Flush()
}

func (h *replayingHub) processOutgoingMessage(expectedMessage messageWithID) error {
	pgprotoBackend := h.getConnection(expectedMessage.ConnectionID).pgprotoBackend
	pgprotoBackend.Send(expectedMessage.Message.(pgproto3.BackendMessage))
	return errtrace.Wrap(pgprotoBackend.Flush())
}

func (h *replayingHub) getConnection(recordedConnectionID uint64) replayingConnection {
	h.lock.Lock()
	defer h.lock.Unlock()

	connection := h.connections[len(h.connections)-1]
	connectionID, ok := h.connectionIDsMap[recordedConnectionID]
	if ok {
		connectionIndex := slices.IndexFunc(h.connections, func(connection replayingConnection) bool {
			return connection.connectionID == connectionID
		})
		if connectionIndex != -1 {
			connection = h.connections[connectionIndex]
		}
	}
	return connection
}

func (h *replayingHub) processIncomingMessage(_ context.Context, expectedMessage messageWithID) error {
	var actualMessage incomingMessage
	select {
	case <-time.After(time.Minute):
		return errtrace.Errorf("timed out waiting for incoming message")
	case actualMessage = <-h.incomingMessagesChan:
	}

	h.lock.Lock()
	h.connectionIDsMap[expectedMessage.ConnectionID] = actualMessage.connectionID
	h.lock.Unlock()

	var expectedBytes, actualBytes []byte
	var err error
	expectedBytes, err = expectedMessage.Message.Encode(expectedBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	actualBytes, err = actualMessage.message.Encode(actualBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	if !slices.Equal(expectedBytes, actualBytes) {
		return errtrace.Errorf("unexpected SQL message \nexpected:\n%q\n\t!=\nactual:\n%q\n\neither your SQL queries/params are unstable, or you need to regenerate the recording", string(expectedBytes), string(actualBytes))
	}
	return nil
}
