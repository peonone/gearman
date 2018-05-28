package server

import (
	"sync"
	"time"

	"github.com/peonone/gearman"
)

// supportFunctionsManager holds the supported functions for each worker
type supportFunctionsManager struct {
	mu            sync.Mutex
	connFunctions map[gearman.ID]supportFunctions
}

type supportFunctions map[string]time.Duration

func (sf supportFunctions) timeout(function string) time.Duration {
	return sf[function]
}

func (sf supportFunctions) support(function string) bool {
	_, ok := sf[function]
	return ok
}

func (sf supportFunctions) canDo(function string, timeout time.Duration) {
	sf[function] = timeout
}

func (sf supportFunctions) cantDo(function string) {
	delete(sf, function)
}

func (sf supportFunctions) reset() {
	for function := range sf {
		delete(sf, function)
	}
}

func (sf supportFunctions) toSlice() []string {
	ret := make([]string, len(sf))
	i := 0
	for function := range sf {
		ret[i] = function
		i++
	}
	return ret
}

func newSupportFunctionsManager() *supportFunctionsManager {
	return &supportFunctionsManager{
		connFunctions: make(map[gearman.ID]supportFunctions),
	}
}

func (m *supportFunctionsManager) canDo(id *gearman.ID, function string, timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	funcs, ok := m.connFunctions[*id]
	if !ok {
		funcs = make(map[string]time.Duration)
		m.connFunctions[*id] = funcs
	}
	funcs.canDo(function, timeout)
}

func (m *supportFunctionsManager) cantDo(id *gearman.ID, function string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	funcs, ok := m.connFunctions[*id]
	if !ok {
		return
	}
	funcs.cantDo(function)
}

func (m *supportFunctionsManager) reset(id *gearman.ID) {
	m.mu.Lock()
	m.mu.Unlock()

	supportedFunctions, ok := m.connFunctions[*id]
	if ok {
		supportedFunctions.reset()
	}
}

func (m *supportFunctionsManager) supportFunctionsSlice(id *gearman.ID) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	funcs, ok := m.connFunctions[*id]
	if !ok {
		return nil
	}
	return funcs.toSlice()
}

func (m *supportFunctionsManager) supportFunctions(id *gearman.ID) supportFunctions {
	m.mu.Lock()
	defer m.mu.Unlock()

	funcs := m.connFunctions[*id]
	return funcs
}
