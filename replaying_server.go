package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"net"
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
		return errtrace.Wrap(startReplayHub(ctx, r, listener))
	}

	return serveFn, connectionString, nil
}
