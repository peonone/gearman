package gearman

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Conn defines the high level interface of a connection
type Conn interface {
	fmt.Stringer
	ReadMsg() (*Message, error)
	WriteMsg(*Message) error
	WriteBin([]byte) error
	Close() error
	ID() *ID
	Closed() <-chan struct{}
	Option() *ConnOption
}

// NetConn is a Conn implementation of the net connection
type NetConn struct {
	conn    net.Conn
	id      *ID
	closed  chan struct{}
	reader  io.Reader
	logger  *log.Logger
	verbose bool
	option  *ConnOption
}

// ConnOption is the option for a connection
type ConnOption struct {
	mu               sync.Mutex
	forwardException bool
}

// SetForwardException sets the forward exception flag
func (o *ConnOption) SetForwardException(forwardException bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.forwardException = forwardException
}

// ForwardException returns the forward exception flag
func (o *ConnOption) ForwardException() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.forwardException
}

// NewNetConn creates a NetConn
func NewNetConn(conn net.Conn, id *ID) *NetConn {
	return &NetConn{
		conn:   conn,
		id:     id,
		closed: make(chan struct{}),
		reader: bufio.NewReader(conn),
		option: new(ConnOption),
	}
}

// ReadMsg reads next Message from the net connection
func (c *NetConn) ReadMsg() (*Message, error) {
	return NextMessage(c.reader)
}

// WriteMsg writes a Message to the net connection
func (c *NetConn) WriteMsg(msg *Message) error {
	_, err := msg.WriteTo(c.conn)
	return err
}

// WriteBin writes an encoded binary message to the net connection
func (c *NetConn) WriteBin(binData []byte) error {
	_, err := c.conn.Write(binData)
	return err
}

// Close closes the connection
func (c *NetConn) Close() error {
	close(c.closed)
	return c.conn.Close()
}

// Closed returns the closed channel
func (c *NetConn) Closed() <-chan struct{} {
	return c.closed
}

// ID returns the identity of the net connection
func (c *NetConn) ID() *ID {
	return c.id
}

// Option returns the connection option
func (c *NetConn) Option() *ConnOption {
	return c.option
}

func (c *NetConn) String() string {
	return fmt.Sprintf("%s(%s)", c.conn.RemoteAddr(), c.id)
}

// MockConn is a Conn implementation by channel
// the main purpose of this is for unit test
type MockConn struct {
	ReadCh  chan *Message
	WriteCh chan *Message
	Timeout time.Duration
	ConnID  *ID
	closed  chan struct{}
	option  *ConnOption
}

// NewMockConn creates a new MockConn
func NewMockConn(readCache int, writeCache int) *MockConn {
	return &MockConn{
		ReadCh:  make(chan *Message, readCache),
		WriteCh: make(chan *Message, writeCache),
		ConnID:  NewIDGenerator().Generate(),
		closed:  make(chan struct{}),
		option:  new(ConnOption),
	}
}

// ReadMsg reads next Message from the channel
func (c *MockConn) ReadMsg() (*Message, error) {
	select {
	case m := <-c.ReadCh:
		if m == nil {
			return nil, io.EOF
		}
		return m, nil
	case <-time.After(c.Timeout):
		return nil, errors.New("timeout")
	}
}

// WriteMsg writes a Message to the channel
func (c *MockConn) WriteMsg(m *Message) error {
	c.WriteCh <- m
	return nil
}

// WriteBin writes an encoded binary message to the channel
func (c *MockConn) WriteBin(binData []byte) error {
	buf := bytes.NewReader(binData)
	msg, err := NextMessage(buf)
	if err != nil {
		return err
	}
	c.WriteCh <- msg
	return nil
}

func (c *MockConn) closeChan(ch chan *Message) {
	defer func() {
		recover()
	}()
	close(ch)
}

// Close closes the mock connection
func (c *MockConn) Close() error {
	close(c.closed)
	c.closeChan(c.ReadCh)
	c.closeChan(c.WriteCh)
	return nil
}

// Closed returns the closed channel of the mock connection
func (c *MockConn) Closed() <-chan struct{} {
	return c.closed
}

// ID returns the identity of the mock connection
func (c *MockConn) ID() *ID {
	return c.ConnID
}

// Option returns option of the channel connection
func (c *MockConn) Option() *ConnOption {
	return c.option
}

func (c *MockConn) String() string {
	return fmt.Sprintf("channel(%s)", c.ID())
}
