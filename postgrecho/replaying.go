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
	"time"
)

const concurrencyToleranceTimeout = 3 * time.Second

func (s *server) buildReplayingWireServer(_ context.Context) (*wire.Server, error) {
	var wireServer *wire.Server
	err := func() error {
		echoFile, err := os.Open(s.cfg.EchoFilePath)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(echoFile)
		timeline := &internal.Timeline{
			Echoes: make([]*internal.ConsumableEcho, 0, 10),
		}
		for {
			echo := &internal.ConsumableEcho{}
			if err = decoder.Decode(&echo.Echo); err != nil {
				break
			}
			timeline.Echoes = append(timeline.Echoes, echo)
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if err != nil {
			return err
		}

		strictOrdering := s.cfg.QueryOrderValidationStrategy == QueryOrderValidationStrategyStrict

		wireServer, err = wire.NewServer(func(ctx context.Context, query string) (wire.PreparedStatementFn, []oid.Oid, wire.Columns, error) {
			echo, err := waitForEcho(query, nil, s.cfg.QueryOrderValidationStrategy, timeline.MatchPrepare(query, strictOrdering))
			if err != nil {
				return nil, nil, nil, err
			}

			prepare := echo.Prepare.Value

			if prepare.Error != "" {
				return nil, nil, nil, errors.New(prepare.Error)
			}

			statement := func(ctx context.Context, writer wire.DataWriter, parameters []string) error {
				echo, err := waitForEcho(query, parameters, s.cfg.QueryOrderValidationStrategy, timeline.MatchExecute(query, parameters, strictOrdering))
				if err != nil {
					return err
				}
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

func waitForEcho(query string, parameters []string, strat QueryOrderValidationStrategy, echoChan <-chan internal.Echo) (internal.Echo, error) {
	if echoChan == nil {
		if parameters != nil {
			return internal.Echo{}, fmt.Errorf("query %q with parameters (%v) is not in our records ; maybe you need to make a new postgrecho record", query, parameters)
		}
		return internal.Echo{}, fmt.Errorf("query %q is not in our records ; maybe you need to make a new postgrecho record", query)
	}
	var echo internal.Echo
	switch strat {
	case QueryOrderValidationStrategyStalling:
		select {
		case echo = <-echoChan:
			return echo, nil
		case <-time.After(concurrencyToleranceTimeout):
		}
	default:
		select {
		case echo = <-echoChan:
			return echo, nil
		default:
		}
	}
	if parameters != nil {
		return internal.Echo{}, fmt.Errorf("query %q with parameters (%v) is in our records, but not in this order ; maybe you need to make a new postgrecho record", query, parameters)
	}
	return internal.Echo{}, fmt.Errorf("query %q is in our records, but not in this order ; maybe you need to make a new postgrecho record", query)
}
