package postgrecho

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hectorj/echo/postgrecho/internal"
	"github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"io"
	"os"
)

func (s *server) buildReplayingWireServer(_ context.Context) (*wire.Server, error) {
	var wireServer *wire.Server
	err := func() error {
		echoFile, err := os.Open(s.cfg.EchoFilePath)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(echoFile)
		timeline := &internal.Timeline{
			Echoes:            make([]*internal.ConsumableEcho, 0, 10),
			EchoesBySessionID: make(map[int64][]*internal.ConsumableEcho, 10),
		}
		for {
			echo := &internal.ConsumableEcho{}
			if err = decoder.Decode(&echo.Echo); err != nil {
				break
			}
			timeline.Echoes = append(timeline.Echoes, echo)
			timeline.EchoesBySessionID[echo.Echo.SessionID] = append(timeline.EchoesBySessionID[echo.Echo.SessionID], echo)
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if err != nil {
			return err
		}
		timeline.SessionIDMappings = mapMap(timeline.EchoesBySessionID, func(_ int64, _ []*internal.ConsumableEcho) *int64 {
			return nil
		})

		wireServer, err = wire.NewServer(func(ctx context.Context, query string) (wire.PreparedStatementFn, []oid.Oid, wire.Columns, error) {
			sessionID := ctx.Value(sessionIDCtxKey).(int64)

			echoChan := timeline.MatchPrepare(sessionID, query)
			if echoChan == nil {
				panic(fmt.Errorf("prepare query %q unmatched", query))
			}
			echo := <-echoChan
			prepare := echo.Prepare.Value

			if prepare.Error != "" {
				return nil, nil, nil, errors.New(prepare.Error)
			}

			statement := func(ctx context.Context, writer wire.DataWriter, parameters []string) error {
				echoChan := timeline.MatchExecute(sessionID, query, parameters)
				if echoChan == nil {
					panic(fmt.Errorf("execute query %q(%v) unmatched", query, parameters))
				}
				echo := <-echoChan
				execute := echo.Execute.Value

				for _, row := range execute.Rows {
					if err := writer.Row(row); err != nil {
						return err
					}
				}
				if execute.Error != "" {
					return errors.New(execute.Error)
				}

				return writer.Complete(execute.CommandTag)
			}

			return statement, prepare.Oids, prepare.Columns, nil
		}, wire.Logger(s.cfg.Logger), wire.Session(wireSessionHandler()))
		return err
	}()
	if err != nil {
		return nil, err
	}
	if wireServer == nil {
		panic("should not happen")
	}
	return wireServer, nil
}
