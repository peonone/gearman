package gearman

import (
	"sync"
)

// MessagePool keeps the recyclable Message objects to prevent re-allocate
// It wraps sync.Pool
type MessagePool struct {
	pool *sync.Pool
}

// NewMessagePool creates a new MessagePool object
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// Get returns a free Message object from the pool or creates a new one if the pool is empty
func (p *MessagePool) Get() *Message {
	return p.pool.Get().(*Message)
}

// Put puts a free Message object back to the pool
func (p *MessagePool) Put(msg *Message) {
	msg.Arguments = nil
	p.pool.Put(msg)
}

// MessageHeaderPool keeps the recyclable message header slice to prevent re-allocate
// It wraps sync.Pool
type MessageHeaderPool struct {
	pool *sync.Pool
}

// NewMessageHeaderPool creates a new MessageHeaderPool object
func NewMessageHeaderPool() *MessageHeaderPool {
	return &MessageHeaderPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, headerSize)
			},
		},
	}
}

// Get returns a free Message header slice from the pool or creates a new one if the pool is empty
func (p *MessageHeaderPool) Get() []byte {
	return p.pool.Get().([]byte)
}

// Put puts a free Message header slice back to the pool
func (p *MessageHeaderPool) Put(header []byte) {
	p.pool.Put(header)
}
