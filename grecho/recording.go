package grecho

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"github.com/hectorj/echo/grecho/internal"
	"github.com/hectorj/echo/grecho/internal/reader"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

func (s *server) recordingServer(ctx context.Context, listener net.Listener) (func() error, ConnectionString, error) {
	recorder, err := newEchoRecorder(s.cfg.EchoFilePath)
	if err != nil {
		return nil, "", err
	}

	connectionString, err := s.cfg.RealPostgresBuilder(ctx, s.cfg)
	if err != nil {
		return nil, "", err
	}
	targetURL, err := url.Parse(connectionString)
	if err != nil {
		return nil, "", err
	}
	targetHost := targetURL.Host
	targetURL.Host = listener.Addr().String()
	newConnectionString := targetURL.String()

	return func() error {
		defer recorder.Close()
		connectionCounter := &atomic.Uint64{}
		for {
			clientConn, err := listener.Accept()
			if err != nil {
				return err
			}
			connectionID := connectionCounter.Add(1)

			go func() {
				err := func() error {
					eg, ctx := errgroup.WithContext(ctx)
					serverConn, err := net.Dial("tcp", targetHost)
					if err != nil {
						return err
					}

					clientTee := newBufferedTee(
						bufio.NewReaderSize(clientConn, reader.DefaultBufferSize),
						serverConn,
					)
					serverTee := newBufferedTee(
						bufio.NewReaderSize(serverConn, reader.DefaultBufferSize),
						clientConn,
					)

					clientChan := make(chan internal.ClientMessage)
					serverChan := make(chan internal.ServerMessage)
					readyChan := make(chan struct{})

					clientReader := reader.NewReader(s.cfg.Logger, clientTee, 0)
					serverReader := reader.NewReader(s.cfg.Logger, serverTee, 0)

					_, err = clientReader.ReadUntypedMsg() // not sure what it is
					if err != nil {
						return err
					}
					_, err = clientTee.Flush()
					if err != nil {
						return err
					}

					// client reading loop
					eg.Go(
						func() error {
							defer close(clientChan)
							for {
								select {
								case <-readyChan:
								case <-ctx.Done():
									return ctx.Err()
								}
								t, _, err := clientReader.ReadTypedMsg()
								if errors.Is(io.EOF, err) {
									return nil
								}
								if err != nil {
									return err
								}
								select {
								case clientChan <- internal.ClientMessage{
									Type:    t,
									Content: slices.Clone(clientReader.Msg),
								}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
						},
					)

					// server reading loop
					eg.Go(
						func() error {
							defer close(serverChan)
							for {
								select {
								case <-readyChan:
								case <-ctx.Done():
									return ctx.Err()
								}
								t, _, err := serverReader.ReadTypedMsg()
								if errors.Is(io.EOF, err) {
									return nil
								}
								if err != nil {
									return err
								}
								select {
								case serverChan <- internal.ServerMessage{
									Type:    types.ServerMessage(t),
									Content: slices.Clone(serverReader.Msg),
								}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
						},
					)

					eg.Go(
						func() error {
							defer clientConn.Close()
							defer serverConn.Close()
							currentEcho := internal.NewEcho(connectionID)
							defer func() {
								if len(currentEcho.Sequences[0].ClientMessages) > 0 || len(currentEcho.Sequences[0].ServerMessages) > 0 {
									_ = recorder.Record(*currentEcho)
								}
							}()
							for {
								select {
								case readyChan <- struct{}{}:
								default:
								}
								select {
								case readyChan <- struct{}{}:
								case clientMessage := <-clientChan:
									recordMessage := clientMessage.Type != types.ClientPassword
									endEcho := false
									closeConnection := false

									if recordMessage {
										currentEcho.AddClientMessage(clientMessage)
									}
									if endEcho {
										err := recorder.Record(*currentEcho)
										if err != nil {
											return err
										}
										currentEcho = internal.NewEcho(connectionID)
									}
									_, err = clientTee.Flush()
									if err != nil {
										return err
									}
									if closeConnection {
										return serverConn.Close()
									}
								case serverMessage := <-serverChan:
									recordMessage := serverMessage.Type != types.ServerAuth
									endEcho := slices.Contains(
										[]types.ServerMessage{
											types.ServerReady,
											types.ServerErrorResponse,
										}, serverMessage.Type,
									)
									closeConnection := serverMessage.Type == types.ServerErrorResponse

									if recordMessage {
										currentEcho.AddServerMessage(serverMessage)
									}
									if endEcho {
										err := recorder.Record(*currentEcho)
										if err != nil {
											return err
										}
										currentEcho = internal.NewEcho(connectionID)
									}
									_, err = serverTee.Flush()
									if err != nil {
										return err
									}
									if closeConnection {
										return clientConn.Close()
									}
								case <-ctx.Done():
									return ctx.Err()
								}
							}
						},
					)

					return eg.Wait()
				}()

				if err != nil {
					s.cfg.Logger.ErrorContext(ctx, err.Error())
				}
			}()

		}
	}, newConnectionString, nil
}

func newEchoRecorder(echoFilePath string) (*echoRecorder, error) {
	echoFile, err := os.Create(echoFilePath)
	if err != nil {
		return nil, err
	}
	echoEncoder := json.NewEncoder(echoFile)
	echoEncoder.SetIndent("", "\t")

	return &echoRecorder{
		echoFile: echoFile,
		encoder:  echoEncoder,
		mutex:    sync.Mutex{},
	}, nil
}

type echoRecorder struct {
	echoFile *os.File
	encoder  *json.Encoder
	mutex    sync.Mutex
}

func (r *echoRecorder) Record(echo internal.Echo) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	err := r.encoder.Encode(echo)
	if err != nil {
		return err
	}
	return r.echoFile.Sync()
}

func (r *echoRecorder) Close() error {
	return r.echoFile.Close()
}
