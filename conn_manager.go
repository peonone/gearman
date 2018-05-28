package gearman

import (
	"sync"
)

// ConnManager manages connections
type ConnManager struct {
	mu    sync.Mutex
	conns map[ID]Conn
}

//NewConnManager creates a new conn manager
func NewConnManager() *ConnManager {
	return &ConnManager{
		conns: make(map[ID]Conn),
	}
}

// AddConn adds a conn to the manager
func (m *ConnManager) AddConn(conn Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[*conn.ID()] = conn
}

// GetConn returns the connection by given id
// return nil if the id not found in the manager
func (m *ConnManager) GetConn(id *ID) Conn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conns[*id]
}

// RemoveConn removes the connection from manager by id
func (m *ConnManager) RemoveConn(id *ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, *id)
}
