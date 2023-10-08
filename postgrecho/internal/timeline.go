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
	Echoes []*ConsumableEcho
	lock   sync.Mutex
}

func (t *Timeline) MatchPrepare(query string, strictOrdering bool) <-chan Echo {
	return t.matchEcho(func(echo Echo) bool {
		return echo.MatchPrepare(query)
	}, strictOrdering)
}

func (t *Timeline) matchEcho(matcher func(Echo) bool, strictOrdering bool) <-chan Echo {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, echo := range t.Echoes {
		if echo.consumed {
			continue
		}
		if echo.reservation != nil {
			if strictOrdering {
				break
			}
			continue
		}
		if !matcher(echo.Echo) {
			if strictOrdering {
				break
			}
			continue
		}
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

	return nil
}

func (t *Timeline) MatchExecute(query string, parameters []string, strictOrdering bool) <-chan Echo {
	return t.matchEcho(func(echo Echo) bool {
		return echo.MatchExecute(query, parameters)
	}, strictOrdering)
}
