package internal

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

type ConsumableEcho struct {
	reservation chan<- Echo
	consumed    bool
	Echo        Echo
}

type Timeline struct {
	Echoes []*ConsumableEcho
	mutex  sync.Mutex
}

func (t *Timeline) Match(
	ctx context.Context,
	lastUsedConnectionID uint64,
	messagesChan <-chan ClientMessage,
	strictOrdering bool,
) (Echo, error) {
	// if the next sequence doesn't require any client message, send it now
	unpromptedEcho, found := t.getUnpromptedEcho(lastUsedConnectionID)
	if found {
		return unpromptedEcho, nil
	}

	var (
		messages       = make([]ClientMessage, 0, 1)
		message        ClientMessage
		ok             = true
		reservation    <-chan Echo
		isPartialMatch bool
	)

	for ok && len(messages) == 0 {
		select {
		case <-ctx.Done():
			return Echo{}, ctx.Err()
		case message, ok = <-messagesChan:
			if ok {
				messages = append(messages, message)
				reservation, isPartialMatch = t.match(ctx, messages, strictOrdering)
				if !isPartialMatch {
					return Echo{}, errors.New("no match found")
				}
			}
		// special case when no message has been received yet: we poll for an unprompted echo to become ready
		case <-time.After(time.Millisecond * 100):
			unpromptedEcho, found = t.getUnpromptedEcho(lastUsedConnectionID)
			if found {
				return unpromptedEcho, nil
			}
		}
	}

	for ok && reservation == nil {
		select {
		case <-ctx.Done():
			return Echo{}, ctx.Err()
		case message, ok = <-messagesChan:
			if ok {
				messages = append(messages, message)
				reservation, isPartialMatch = t.match(ctx, messages, strictOrdering)
				if !isPartialMatch {
					return Echo{}, errors.New("no match found")
				}
			}
		}

	}

	if reservation != nil {
		select {
		case <-ctx.Done():
			return Echo{}, ctx.Err()
		case echo := <-reservation:
			return echo, nil
		}
	}

	if len(messages) == 0 {
		return Echo{}, io.EOF
	}

	return Echo{}, errors.New("no match found")
}

func (t *Timeline) getUnpromptedEcho(lastUsedConnectionID uint64) (Echo, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for _, echo := range t.Echoes {
		if echo.consumed {
			continue
		}
		if len(echo.Echo.Sequences[0].ClientMessages) == 0 && (lastUsedConnectionID == 0 || lastUsedConnectionID == echo.Echo.ConnectionID) {
			echo.consumed = true

			return echo.Echo, true
		}
		if lastUsedConnectionID != 0 {
			break
		}
	}
	return Echo{}, false
}

func (t *Timeline) match(_ context.Context, messages []ClientMessage, strictOrdering bool) (<-chan Echo, bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	isPartialMatch := false
	for _, echo := range t.Echoes {
		if echo.consumed || echo.reservation != nil {
			continue
		}
		recordedMessages := echo.Echo.Sequences[0].ClientMessages
		if len(recordedMessages) < len(messages) || (isPartialMatch && len(recordedMessages) != len(messages)) {
			continue
		}

		matches := true
		for i, message := range messages {
			if !message.Match(recordedMessages[i]) {
				matches = false
				break
			}
		}

		if matches && len(recordedMessages) != len(messages) {
			isPartialMatch = true
		} else if matches {
			// it's a complete match!
			reservation := make(chan Echo, 1)
			echo.reservation = reservation

			// send echoes if possible
			for _, sendEcho := range t.Echoes {
				// we stopped at the first unconsumed & unreserved echo
				if sendEcho.consumed == true {
					continue
				}
				if sendEcho.reservation == nil {
					break
				}
				sendEcho.consumed = true
				sendEcho.reservation <- sendEcho.Echo
				close(sendEcho.reservation)
			}

			return reservation, true
		}
		if strictOrdering {
			break
		}

	}
	return nil, isPartialMatch
}
