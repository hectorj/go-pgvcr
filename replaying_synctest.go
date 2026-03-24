package pgvcr

import (
	"context"
	"net"

	"braces.dev/errtrace"
)

// startReplayHubSynctest is the synctest-aware version of startReplayHub.
// It relies on testing/synctest to provide deterministic goroutine scheduling,
// so it doesn't need stalling or timeouts - synctest handles all the synchronization.
func startReplayHubSynctest(ctx context.Context, r *replayer, listener net.Listener) error {
	// For synctest mode, we use the exact same implementation as strict mode
	// because synctest's deterministic scheduling ensures messages arrive in order.
	// The key difference is that synctest controls when goroutines run,
	// making concurrent queries deterministic without any manual coordination.
	return errtrace.Wrap(startReplayHub(ctx, r, listener, QueryOrderValidationStrategyStrict))
}
