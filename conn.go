package gearman

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// Conn defines the high level interface of a connection
type Conn interface {
	fmt.Stringer
	ReadMsg() (*Message, string, error)
	WriteMsg(*Message) error
	WriteTxtMsg(string) error
	WriteBin([]byte) error
	Close() error
	ID() *ID
	Closed() <-chan struct{}
}

// NetConn is a Conn implementation of the net connection
type NetConn struct {
	conn    net.Conn
	id      *ID
	closed  chan struct{}
	reader  *bufio.Reader
	logger  *log.Logger
	verbose bool
}

// NewNetConn creates a NetConn
func NewNetConn(conn net.Conn, id *ID) *NetConn {
	return &NetConn{
		conn:   conn,
		id:     id,
		closed: make(chan struct{}),
		reader: bufio.NewReader(conn),
	}
}

// ReadMsg reads next Message from the net connection
func (c *NetConn) ReadMsg() (*Message, string, error) {
	return NextMessage(c.reader)
}

// WriteMsg writes a Message to the net connection
func (c *NetConn) WriteMsg(msg *Message) error {
	_, err := msg.WriteTo(c.conn)
	return err
}

// WriteTxtMsg writes a Message to the net connection
func (c *NetConn) WriteTxtMsg(content string) error {
	_, err := c.conn.Write([]byte(content))
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

func (c *NetConn) String() string {
	return fmt.Sprintf("%s(%s)", c.conn.RemoteAddr(), c.id)
}

// MockConn is a Conn implementation by channel
// the main purpose of this is for unit test
type MockConn struct {
	ReadCh     chan *Message
	WriteCh    chan *Message
	ReadTxtCh  chan string
	WriteTxtCh chan string
	Timeout    time.Duration
	ConnID     *ID
	closed     chan struct{}
}

// NewMockConn creates a new MockConn
func NewMockConn(readCache int, writeCache int) *MockConn {
	return &MockConn{
		ReadCh:     make(chan *Message, readCache),
		WriteCh:    make(chan *Message, writeCache),
		ReadTxtCh:  make(chan string, readCache),
		WriteTxtCh: make(chan string, writeCache),
		ConnID:     NewIDGenerator().Generate(),
		closed:     make(chan struct{}),
	}
}

// ReadMsg reads next Message from the channel
func (c *MockConn) ReadMsg() (*Message, string, error) {
	select {
	case m := <-c.ReadCh:
		if m == nil {
			return nil, "", io.EOF
		}
		return m, "", nil
	case txt := <-c.ReadTxtCh:
		return nil, txt, nil
	case <-time.After(c.Timeout):
		return nil, "", errors.New("timeout")
	}
}

// WriteMsg writes a Message to the channel
func (c *MockConn) WriteMsg(m *Message) error {
	// copy the Message struct as we will the original one to the pool for re-use
	msgCopy := *m
	c.WriteCh <- &msgCopy
	return nil
}

// WriteTxtMsg writes a text message to the channel
func (c *MockConn) WriteTxtMsg(content string) error {
	c.WriteTxtCh <- content
	return nil
}

// WriteBin writes an encoded binary message to the channel
func (c *MockConn) WriteBin(binData []byte) error {
	buf := bufio.NewReader(bytes.NewReader(binData))
	msg, _, err := NextMessage(buf)
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

func (c *MockConn) String() string {
	return fmt.Sprintf("channel(%s)", c.ID())
}
