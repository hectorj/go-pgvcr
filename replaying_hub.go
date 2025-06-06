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
	connectionID replayingConnectionID
	message      pgproto3.Message
}

type replayingConnection struct {
	connectionID   replayingConnectionID
	conn           net.Conn
	pgprotoBackend *pgproto3.Backend
	sendLock       *sync.Mutex
}

type recordedConnectionID uint64
type replayingConnectionID uint64

type replayingConnectionMetadata struct {
	id                replayingConnectionID
	commandInProgress bool
}

type replayingHub struct {
	listener                     net.Listener
	replayer                     *replayer
	connections                  []replayingConnection
	incomingMessagesChan         chan incomingMessage
	connectionIDsMap             map[recordedConnectionID]replayingConnectionMetadata
	connectionsLock              sync.Mutex
	queryOrderValidationStrategy QueryOrderValidationStrategy
	stalledMessages              []incomingMessage
}

func startReplayHub(ctx context.Context, r *replayer, listener net.Listener, queryOrderValidationStrategy QueryOrderValidationStrategy) error {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	h := &replayingHub{
		listener:                     listener,
		replayer:                     r,
		connections:                  make([]replayingConnection, 0, 1),
		incomingMessagesChan:         make(chan incomingMessage),
		connectionIDsMap:             make(map[recordedConnectionID]replayingConnectionMetadata),
		connectionsLock:              sync.Mutex{},
		queryOrderValidationStrategy: queryOrderValidationStrategy,
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
	connectionID := replayingConnectionID(0)
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
			sendLock:       &sync.Mutex{},
		}

		h.connectionsLock.Lock()
		h.connections = append(h.connections, rConn)
		h.connectionsLock.Unlock()

		go h.receiveMessagesLoop(ctx, rConn)

		connectionID++
	}
	return errtrace.Wrap(context.Cause(ctx))
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
			conn.sendLock.Lock()
			conn.pgprotoBackend.Send(&pgproto3.EmptyQueryResponse{})
			conn.pgprotoBackend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			err := conn.pgprotoBackend.Flush()
			conn.sendLock.Unlock()
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
	logDebug(ctx, "expecting startup message", slog.Uint64("connectionID", uint64(conn.connectionID)))
	startupMessage, err := conn.pgprotoBackend.ReceiveStartupMessage()
	if err != nil {
		return errtrace.Errorf("error receiving startup message: %w", err)
	}
	logDebug(ctx, "received startup message", slog.Any("startupMessage", startupMessage), slog.Uint64("connectionID", uint64(conn.connectionID)))

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
		if err != nil {
			return errtrace.Wrap(err)
		}
		logDebug(ctx, "sending AuthenticationOk", slog.Uint64("connectionID", uint64(conn.connectionID)))
		_, err = conn.conn.Write(buf)
		if err != nil {
			return errtrace.Wrap(err)
		}

		err = h.replayer.ConsumeGreetings(func(msgs []messageWithID) error {
			conn.sendLock.Lock()
			defer conn.sendLock.Unlock()
			for _, msg := range msgs {
				conn.pgprotoBackend.Send(msg.Message.(pgproto3.BackendMessage))
			}
			return errtrace.Wrap(conn.pgprotoBackend.Flush())
		})
		if err != nil {
			return errtrace.Wrap(err)
		}
	case *pgproto3.SSLRequest:
		logDebug(ctx, "refusing SSL", slog.Uint64("connectionID", uint64(conn.connectionID)))
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
				logDebug(ctx, "expecting incoming message", slog.Any("expectedMessage", expectedMessage))
				err := h.processIncomingMessage(ctx, expectedMessage)
				if err != nil {
					h.sendErrOnWire(ctx, expectedMessage.ConnectionID, err)
				}
				return errtrace.Wrap(err)
			}

			logDebug(ctx, "sending outgoing message", slog.Any("expectedMessage", expectedMessage))
			return errtrace.Wrap(h.processOutgoingMessage(expectedMessage))
		})
		if errors.Is(err, errNoMoreMessages) {
			return nil
		}
		if err != nil {
			return errtrace.Wrap(err)
		}
	}
	return errtrace.Wrap(context.Cause(ctx))
}

func (h *replayingHub) sendErrOnWire(_ context.Context, recordedConnectionID recordedConnectionID, err error) {
	conn := h.getConnection(recordedConnectionID)
	conn.sendLock.Lock()
	defer conn.sendLock.Unlock()
	conn.pgprotoBackend.Send(&pgproto3.ErrorResponse{
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
	_ = conn.pgprotoBackend.Flush()
}

func (h *replayingHub) processOutgoingMessage(expectedMessage messageWithID) error {
	conn := h.getConnection(expectedMessage.ConnectionID)
	conn.sendLock.Lock()
	defer conn.sendLock.Unlock()
	conn.pgprotoBackend.Send(expectedMessage.Message.(pgproto3.BackendMessage))
	return errtrace.Wrap(conn.pgprotoBackend.Flush())
}

func (h *replayingHub) getConnection(recordedConnectionID recordedConnectionID) replayingConnection {
	h.connectionsLock.Lock()
	defer h.connectionsLock.Unlock()

	connection := h.connections[len(h.connections)-1]
	rc, ok := h.connectionIDsMap[recordedConnectionID]
	if ok {
		connectionIndex := slices.IndexFunc(h.connections, func(connection replayingConnection) bool {
			return connection.connectionID == rc.id
		})
		if connectionIndex != -1 {
			connection = h.connections[connectionIndex]
		}
	}
	return connection
}

func (h *replayingHub) processIncomingMessage(ctx context.Context, expectedMessage messageWithID) error {
	actualMessage, err := h.getNextIncomingMessage(ctx)
	if err != nil {
		return errtrace.Wrap(err)
	}

	err = h.checkIfMessagesMatch(expectedMessage, actualMessage)
	if err != nil {
		if errors.Is(err, ErrMessageMismatch) && h.queryOrderValidationStrategy == QueryOrderValidationStrategyStalling {
			if h.tryStalling(ctx, expectedMessage, actualMessage) == nil {
				return nil
			}
		}
		return errtrace.Wrap(err)
	}

	_, isReadyForQuery := actualMessage.message.(*pgproto3.ReadyForQuery)

	h.connectionsLock.Lock()
	h.connectionIDsMap[expectedMessage.ConnectionID] = replayingConnectionMetadata{
		id:                actualMessage.connectionID,
		commandInProgress: !isReadyForQuery,
	}
	h.connectionsLock.Unlock()

	return nil
}

const ErrMessageMismatch constError = "err_message_mismatch"

func (h *replayingHub) checkIfMessagesMatch(expectedMessage messageWithID, actualMessage incomingMessage) error {
	var err error
	var expectedBytes, actualBytes []byte
	expectedBytes, err = expectedMessage.Message.Encode(expectedBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	actualBytes, err = actualMessage.message.Encode(actualBytes)
	if err != nil {
		return errtrace.Wrap(err)
	}
	messagesMatch := slices.Equal(expectedBytes, actualBytes)
	if !messagesMatch {
		return errtrace.Errorf("%w\nunexpected SQL message \nexpected:\n%q\n\t!=\nactual:\n%q\n\neither your SQL queries/params are unstable, or you need to regenerate the recording", ErrMessageMismatch, string(expectedBytes), string(actualBytes))
	}

	h.connectionsLock.Lock()
	rc := h.connectionIDsMap[expectedMessage.ConnectionID]
	h.connectionsLock.Unlock()
	if rc.commandInProgress && rc.id != actualMessage.connectionID {
		return errtrace.Errorf("%w: mismatched connection ID", ErrMessageMismatch)
	}
	return nil
}

func (h *replayingHub) getNextIncomingMessage(ctx context.Context) (incomingMessage, error) {
	var actualMessage incomingMessage
	if len(h.stalledMessages) > 0 {
		actualMessage = h.stalledMessages[len(h.stalledMessages)-1]
		h.stalledMessages = h.stalledMessages[:len(h.stalledMessages)-1]
		//logDebug(ctx, "using stalled message", slog.String("message", fmt.Sprintf("%#v", actualMessage.message)), slog.Int("stalledLeftCount", len(h.stalledMessages)))
		return actualMessage, nil
	}
	ctx, cancelFunc := context.WithTimeoutCause(ctx, time.Minute, errtrace.New("timed out waiting for incoming message"))
	defer cancelFunc()

	select {
	case <-ctx.Done():
		return incomingMessage{}, errtrace.Wrap(context.Cause(ctx))
	case actualMessage = <-h.incomingMessagesChan:
	}
	//logDebug(ctx, "received new message", slog.String("message", fmt.Sprintf("%#v", actualMessage.message)), slog.Int("stalledLeftCount", len(h.stalledMessages)))
	return actualMessage, nil
}

func (h *replayingHub) tryStalling(ctx context.Context, expectedMessage messageWithID, actualMessage incomingMessage) error {
	ctx, cancelFunc := context.WithTimeoutCause(ctx, time.Minute, errtrace.New("stalling for too long"))
	defer cancelFunc()

	err := h.processIncomingMessage(ctx, expectedMessage)
	h.stalledMessages = append(h.stalledMessages, actualMessage)

	return errtrace.Wrap(err)
}
