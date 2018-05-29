package server

import (
	"sync"

	"github.com/peonone/gearman"
)

// conn represents a connection in the server side
type conn struct {
	gearman.Conn
	mu               sync.Mutex
	supportFunctions supportFunctions
	option           *connOption
	worker           bool
	clientID         string // the id set by worker side
}

type connOption struct {
	mu               sync.Mutex
	forwardException bool
}

func newServerConn(gconn gearman.Conn) *conn {
	return &conn{
		Conn:             gconn,
		supportFunctions: newSupportFunctions(),
		option:           new(connOption),
	}
}

func (c *conn) setForwardException(forwardException bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.option.forwardException = forwardException
}

func (c *conn) setIsWorker(isWorker bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.worker = isWorker
}

func (c *conn) forwardException() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.option.forwardException
}

func (c *conn) isWorker() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.worker
}

func (c *conn) getClientID() string {
	c.mu.Lock()
	c.mu.Unlock()

	return c.clientID
}

func (c *conn) setClientID(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientID = clientID
}

type mockConn struct {
	srvConn *conn
	*gearman.MockConn
}

func newMockSConn(readCache int, writeCache int) *mockConn {
	conn := gearman.NewMockConn(readCache, writeCache)
	return &mockConn{
		newServerConn(conn), conn,
	}
}
