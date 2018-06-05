package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/mock"
)

var errInvaldPacketType = errors.New("Invalid packet type")

// serverMessageHandler is the interface of server message handler
type serverMessageHandler interface {
	supportPacketTypes() []gearman.PacketType
	handle(context.Context, *gearman.Message, *conn) (bool, error)
}

// serverMessageHandlerManager holds all available message handlers
type serverMessageHandlerManager struct {
	mu         sync.Mutex
	handlers   map[gearman.PacketType]serverMessageHandler
	reqTimeout time.Duration
}

// newServerHandlerManager creates an empty handler manager
func newServerHandlerManager(reqTimeout time.Duration) *serverMessageHandlerManager {
	return &serverMessageHandlerManager{
		handlers:   make(map[gearman.PacketType]serverMessageHandler),
		reqTimeout: reqTimeout,
	}
}

// registerHandler registers a handler
func (m *serverMessageHandlerManager) registerHandler(packetType gearman.PacketType, handler serverMessageHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.handlers[packetType] = handler
}

// handleMessage process one message for the connection
// first it checks the validity of the message and return the error if fails
// then it dispatch to the approciate handler to process the message
func (m *serverMessageHandlerManager) handleMessage(msg *gearman.Message, conn *conn) (bool, error) {
	err := msg.Validate(gearman.RoleServer)
	if err != nil {
		return true, err
	}
	m.mu.Lock()
	handler, ok := m.handlers[msg.PacketType]
	m.mu.Unlock()
	if !ok {
		return true, errInvaldPacketType
	}
	ctx := context.Background()
	if m.reqTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, m.reqTimeout)
	}
	return handler.handle(ctx, msg, conn)
}

// MockHandler is an implementation for unit test
type MockHandler struct {
	mock.Mock
}

func (h *MockHandler) handle(ctx context.Context, msg *gearman.Message, conn *conn) (bool, error) {
	returnVals := h.Called(ctx, msg, conn)
	return returnVals.Bool(0), returnVals.Error(1)
}

func (h *MockHandler) supportPacketTypes() []gearman.PacketType {
	return h.Called().Get(0).([]gearman.PacketType)
}
