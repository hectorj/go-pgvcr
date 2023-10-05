package postgrecho

import (
	"context"
	"encoding/json"
	"github.com/hectorj/echo/postgrecho/internal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"os"
	"sync"
)

type echoWriter struct {
	encoder *json.Encoder
	lock    *sync.Mutex
}

func (w *echoWriter) Write(echo internal.Echo) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.encoder.Encode(echo)
}

func (s *server) buildRecordingWireServer(ctx context.Context) (*wire.Server, error) {
	var server *wire.Server
	err := func() error {
		echoFile, err := os.Create(s.cfg.EchoFilePath)
		if err != nil {
			return err
		}

		connString, err := s.cfg.RealPostgresBuilder(ctx, s.cfg)
		if err != nil {
			return err
		}

		pool, err := pgxpool.New(ctx, connString)
		if err != nil {
			return err
		}
		connections := make(map[int64]*pgx.Conn)

		eWriter := &echoWriter{
			encoder: json.NewEncoder(echoFile),
			lock:    &sync.Mutex{},
		}
		eWriter.encoder.SetIndent("", "\t")

		server, err = wire.NewServer(func(ctx context.Context, query string) (wire.PreparedStatementFn, []oid.Oid, wire.Columns, error) {
			sessionID := ctx.Value(sessionIDCtxKey).(int64)
			prepare := internal.Prepare{
				Query: query,
			}
			defer func() {
				panicIfErr(eWriter.Write(internal.Echo{
					SessionID: sessionID,
					Prepare: internal.Nullable[internal.Prepare]{
						Valid: true,
						Value: prepare,
					},
				}))
			}()

			conn := connections[sessionID]
			if conn == nil {
				poolConn, err := pool.Acquire(ctx)
				if err != nil {
					return nil, nil, nil, err
				}
				conn = poolConn.Conn()
				connections[sessionID] = conn
			}
			sd, err := conn.Prepare(ctx, "", query)
			if err != nil {
				prepare.Error = err.Error()
				return nil, nil, nil, err
			}

			oids := mapSlice(sd.ParamOIDs, func(v uint32) oid.Oid {
				return oid.Oid(v)
			})
			prepare.Oids = oids

			columns := wire.Columns(mapSliceWithIndex(sd.Fields, func(i int, v pgconn.FieldDescription) wire.Column {
				return wire.Column{
					Table:        int32(v.TableOID),
					ID:           int32(i),
					Attr:         int16(v.TableAttributeNumber),
					Name:         v.Name,
					AttrNo:       0, // marked as optional
					Oid:          oid.Oid(v.DataTypeOID),
					Width:        v.DataTypeSize,
					TypeModifier: v.TypeModifier,
					Format:       wire.FormatCode(v.Format),
				}
			}))
			prepare.Columns = columns

			statement := func(ctx context.Context, writer wire.DataWriter, parameters []string) error {
				execute := internal.Execute{
					Query:      query,
					Parameters: parameters,
				}
				defer func() {
					panicIfErr(eWriter.Write(internal.Echo{
						SessionID: sessionID,
						Execute: internal.Nullable[internal.Execute]{
							Valid: true,
							Value: execute,
						},
					}))
				}()

				if len(columns) == 0 {
					ct, err := conn.Exec(ctx, sd.SQL, anifySlice(parameters)...)
					if err != nil {
						execute.Error = err.Error()
						return err
					}
					execute.CommandTag = ct.String()
					return writer.Complete(ct.String())
				}

				rows, err := conn.Query(ctx, sd.SQL, anifySlice(parameters)...)
				if err != nil {
					execute.Error = err.Error()
					return err
				}
				for rows.Next() {
					values, err := rows.Values()
					if err != nil {
						execute.Error = err.Error()
						return err
					}
					execute.Rows = append(execute.Rows, values)
					err = writer.Row(values)
					if err != nil {
						return err
					}
				}
				err = rows.Err()
				if err != nil {
					execute.Error = err.Error()
					return err
				}

				execute.CommandTag = rows.CommandTag().String()
				return writer.Complete(rows.CommandTag().String())
			}

			return statement, oids, columns, nil
		}, wire.Logger(s.cfg.Logger), wire.Session(wireSessionHandler()))
		return err
	}()
	if err != nil {
		return nil, err
	}
	if server == nil {
		panic("should never happen")
	}
	return server, nil
}
