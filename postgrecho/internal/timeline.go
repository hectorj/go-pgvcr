package internal

import (
	"sync"
)

type ConsumableEcho struct {
	reservation chan<- Echo
	consumed    bool
	Echo        Echo
}

type Timeline struct {
	Echoes            []*ConsumableEcho
	EchoesBySessionID map[int64][]*ConsumableEcho
	SessionIDMappings map[int64]*int64
	lock              sync.Mutex
}

func (t *Timeline) MatchPrepare(sessionID int64, query string) <-chan Echo {
	return t.matchEcho(sessionID, func(echo Echo) bool {
		return echo.MatchPrepare(query)
	})
}

func (t *Timeline) matchEcho(sessionID int64, matcher func(Echo) bool) <-chan Echo {
	t.lock.Lock()
	defer t.lock.Unlock()
	for recordedSessionID, echoes := range t.EchoesBySessionID {
		if mappedSessionID := t.SessionIDMappings[recordedSessionID]; mappedSessionID != nil && *mappedSessionID != sessionID {
			continue
		}

		for _, echo := range echoes {
			if echo.consumed {
				continue
			}
			if echo.reservation != nil {
				break
			}
			if !matcher(echo.Echo) {
				break
			}
			t.SessionIDMappings[recordedSessionID] = &sessionID
			reservation := make(chan Echo, 1)
			echo.reservation = reservation

			for _, sendingEcho := range t.Echoes {
				if sendingEcho.consumed {
					continue
				}
				if sendingEcho.reservation == nil {
					break
				}
				sendingEcho.reservation <- sendingEcho.Echo
				close(sendingEcho.reservation)
				sendingEcho.consumed = true
			}

			return reservation
		}
	}

	return nil
}

func (t *Timeline) MatchExecute(sessionID int64, query string, parameters []string) <-chan Echo {
	return t.matchEcho(sessionID, func(echo Echo) bool {
		return echo.MatchExecute(query, parameters)
	})
}
