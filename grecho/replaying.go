package grecho

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"

	"github.com/hectorj/echo/grecho/internal"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

func (s *server) replayingServer(ctx context.Context, listener net.Listener) (func() error, ConnectionString, error) {
	timeline, err := readEchoFile(s.cfg.EchoFilePath)
	if err != nil {
		return nil, "", err
	}

	strictOrdering := s.cfg.QueryOrderValidationStrategy == QueryOrderValidationStrategyStrict
	connectionString := "postgresql://user:password@" + listener.Addr().String() + "/db?sslmode=disable"

	return func() error {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}

			reader := buffer.NewReader(s.cfg.Logger, conn, 0)
			writer := buffer.NewWriter(s.cfg.Logger, conn)

			_, err = reader.ReadUntypedMsg()
			if err != nil {
				return err
			}

			writer.Start(types.ServerAuth)
			writer.AddInt32(0)
			err = writer.End()
			if err != nil {
				return err
			}

			messagesChan := make(chan internal.ClientMessage)

			go func() {
				defer close(messagesChan)
				for {
					mType, _, err := reader.ReadTypedMsg()
					if err != nil {
						if errors.Is(io.EOF, err) {
							return
						}
						s.cfg.Logger.ErrorContext(ctx, err.Error())
					}
					select {
					case messagesChan <- internal.ClientMessage{
						Type:    mType,
						Content: slices.Clone(reader.Msg),
					}:
					case <-ctx.Done():
						return
					}
				}
			}()
			go func() {
				defer conn.Close()
				err := func() error {
					var lastUsedConnectionID uint64
					for {
						echo, err := timeline.Match(ctx, lastUsedConnectionID, messagesChan, strictOrdering)
						if err != nil {
							if errors.Is(io.EOF, err) {
								return nil
							}
							return err
						}
						lastUsedConnectionID = echo.ConnectionID
						isFirst := true
						for _, sequence := range echo.Sequences {
							for _, recordedMessage := range sequence.ClientMessages {
								if isFirst {
									break
								}

								select {
								case actualMessage, ok := <-messagesChan:
									if !ok {
										return nil
									}
									if !actualMessage.Match(recordedMessage) {
										return errors.New("mismatching messages")
									}
								case <-ctx.Done():
									return ctx.Err()
								}
							}
							for _, recordedMessage := range sequence.ServerMessages {
								writer.Start(recordedMessage.Type)
								writer.AddBytes(recordedMessage.Content)
								err = writer.End()
								if err != nil {
									return err
								}
							}
							isFirst = false
						}
					}
				}()

				if err != nil {
					writer.Start(types.ServerErrorResponse)
					writer.AddByte('S')
					writer.AddString("FATAL")
					writer.AddByte(0)
					writer.AddByte('V')
					writer.AddString("FATAL")
					writer.AddByte(0)
					writer.AddByte('C')
					writer.AddString("GRECHO_ERROR")
					writer.AddByte(0)
					writer.AddByte('M')
					writer.AddString(fmt.Sprintf("grecho: %s", err.Error()))
					writer.AddByte(0)
					writer.AddByte(0)
					_ = writer.End()
				}
			}()
		}
	}, connectionString, nil
}

func readEchoFile(echoFilePath string) (*internal.Timeline, error) {
	echoFile, err := os.Open(echoFilePath)
	if err != nil {
		return nil, err
	}
	timeline := &internal.Timeline{Echoes: make([]*internal.ConsumableEcho, 0)}
	decoder := json.NewDecoder(echoFile)
	for {
		var echo internal.Echo
		err = decoder.Decode(&echo)
		if err != nil {
			break
		}
		timeline.Echoes = append(timeline.Echoes, &internal.ConsumableEcho{Echo: echo})
	}
	if !errors.Is(err, io.EOF) {
		return nil, err
	}
	return timeline, nil
}
