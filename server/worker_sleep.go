package server

import (
	"sync"

	"github.com/peonone/gearman"
)

type sleepManager struct {
	mu           sync.Mutex
	sleepConnIDs map[gearman.ID]struct{}
}

func newSleepManager() *sleepManager {
	return &sleepManager{
		sleepConnIDs: make(map[gearman.ID]struct{}),
	}
}

func (m *sleepManager) addSleepWorker(connID *gearman.ID) {
	m.mu.Lock()
	m.mu.Unlock()

	m.sleepConnIDs[*connID] = struct{}{}
}

func (m *sleepManager) removeSleepWorker(connID *gearman.ID) {
	m.mu.Lock()
	m.mu.Unlock()

	delete(m.sleepConnIDs, *connID)
}

func (m *sleepManager) allSleepingConnIDs() []*gearman.ID {
	m.mu.Lock()
	m.mu.Unlock()
	ret := make([]*gearman.ID, len(m.sleepConnIDs))
	i := 0
	for k := range m.sleepConnIDs {
		// declare a new variable to prevent return the same pointer for all elements
		x := k
		ret[i] = &x
		i++
	}
	return ret
}
