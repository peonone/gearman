package gearman

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
)

// MessageHandler is the interface of a message handler
type MessageHandler interface {
	SupportPacketTypes() []PacketType
	Handle(context.Context, *Message, Conn) (bool, error)
}

// MessageHandlerManager holds all available message handlers
type MessageHandlerManager struct {
	mu         sync.Mutex
	role       RoleType
	handlers   map[PacketType]MessageHandler
	reqTimeout time.Duration
}

// NewHandlerManager creates an empty handler manager
func NewHandlerManager(role RoleType, reqTimeout time.Duration) *MessageHandlerManager {
	return &MessageHandlerManager{
		role:       role,
		handlers:   make(map[PacketType]MessageHandler),
		reqTimeout: reqTimeout,
	}
}

// RegisterHandler registers a handler
func (m *MessageHandlerManager) RegisterHandler(packetType PacketType, handler MessageHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.handlers[packetType] = handler
}

// HandleMessage process one message for the connection
// first it checks the validity of the message and return the error if fails
// then it dispatch to the approciate handler to process the message
func (m *MessageHandlerManager) HandleMessage(msg *Message, conn Conn) (bool, error) {
	err := msg.Validate(m.role)
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	handler, ok := m.handlers[msg.PacketType]
	m.mu.Unlock()
	if !ok {
		return false, errInvaldPacketType
	}
	ctx := context.Background()
	if m.reqTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, m.reqTimeout)
	}
	return handler.Handle(ctx, msg, conn)
}

// MockHandler is an implementation for unit test
type MockHandler struct {
	mock.Mock
}

func (h *MockHandler) Handle(ctx context.Context, msg *Message, conn Conn) (bool, error) {
	returnVals := h.Called(ctx, msg, conn)
	return returnVals.Bool(0), returnVals.Error(1)
}

func (h *MockHandler) SupportPacketTypes() []PacketType {
	return h.Called().Get(0).([]PacketType)
}
