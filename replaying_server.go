package pgvcr

import (
	"braces.dev/errtrace"
	"context"
	"net"
)

func (s *server) replayingServer(ctx context.Context, listener net.Listener, queryOrderValidationStrategy QueryOrderValidationStrategy) (serveFn, error) {
	greetings, records, err := readMessages(s.cfg.EchoFilePath)
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	r := &replayer{
		messages:  records,
		greetings: greetings,
	}

	serveFn := func() error {
		return errtrace.Wrap(startReplayHub(ctx, r, listener, queryOrderValidationStrategy))
	}

	return serveFn, nil
}
