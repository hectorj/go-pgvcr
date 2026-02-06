package pgvcr

import (
	"context"
	"net"
)

// pipeConnectionRequest represents a request to accept a new pipe connection
type pipeConnectionRequest struct {
	serverConn net.Conn
}

// pipeAcceptor implements net.Listener using in-memory pipes
type pipeAcceptor struct {
	connChan  chan pipeConnectionRequest
	closeChan <-chan struct{}
}

func newPipeAcceptor(ctx context.Context) *pipeAcceptor {
	return &pipeAcceptor{
		connChan:  make(chan pipeConnectionRequest),
		closeChan: ctx.Done(),
	}
}

func (p *pipeAcceptor) Accept() (net.Conn, error) {
	select {
	case req := <-p.connChan:
		return req.serverConn, nil
	case <-p.closeChan:
		return nil, net.ErrClosed
	}
}

func (p *pipeAcceptor) Close() error {
	// Context cancellation handles cleanup
	return nil
}

func (p *pipeAcceptor) Addr() net.Addr {
	return pipeAddr{}
}

// pipeAddr implements net.Addr for pipe connections
type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pgvcr-pipe" }
