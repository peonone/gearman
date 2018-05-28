package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestSupportFunctions(t *testing.T) {
	manager := newSupportFunctionsManager()
	worker1 := gearman.NewMockConn(10, 10)
	worker2 := gearman.NewMockConn(10, 10)
	assert.Equal(t, 0, len(manager.supportFunctions(worker1.ID())))

	manager.canDo(worker1.ID(), "echo", 0)
	manager.canDo(worker1.ID(), "wc", time.Second)

	manager.canDo(worker2.ID(), "echo", time.Millisecond*50)

	worker1Funcs := manager.supportFunctions(worker1.ID())
	assert.Equal(t, 2, len(worker1Funcs))
	assert.Contains(t, worker1Funcs, "echo")
	assert.Contains(t, worker1Funcs, "wc")

	assert.Equal(t, time.Duration(0), worker1Funcs.timeout("echo"))
	assert.Equal(t, time.Second, worker1Funcs.timeout("wc"))

	worker2Funcs := manager.supportFunctions(worker2.ID())
	assert.Equal(t, 1, len(worker2Funcs))
	assert.Contains(t, worker2Funcs, "echo")

	assert.Equal(t, time.Millisecond*50, worker2Funcs.timeout("echo"))

	manager.reset(worker1.ID())
	assert.Equal(t, 0, len(manager.supportFunctions(worker1.ID())))
	assert.NotEqual(t, 0, len(manager.supportFunctions(worker2.ID())))
}
