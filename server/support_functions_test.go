package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSupportFunctions(t *testing.T) {
	worker1 := newMockSConn(10, 10)
	worker2 := newMockSConn(10, 10)
	assert.Equal(t, 0, len(worker1.srvConn.supportFunctions))

	worker1.srvConn.supportFunctions.canDo("echo", 0)
	worker1.srvConn.supportFunctions.canDo("wc", time.Second)

	worker2.srvConn.supportFunctions.canDo("echo", time.Millisecond*50)

	assert.Equal(t, 2, len(worker1.srvConn.supportFunctions))
	assert.Contains(t, worker1.srvConn.supportFunctions, "echo")
	assert.Contains(t, worker1.srvConn.supportFunctions, "wc")

	assert.Equal(t, time.Duration(0), worker1.srvConn.supportFunctions.timeout("echo"))
	assert.Equal(t, time.Second, worker1.srvConn.supportFunctions.timeout("wc"))

	assert.Equal(t, 1, len(worker2.srvConn.supportFunctions))
	assert.Contains(t, worker2.srvConn.supportFunctions, "echo")

	assert.Equal(t, time.Millisecond*50, worker2.srvConn.supportFunctions.timeout("echo"))

	worker1.srvConn.supportFunctions.reset()
	assert.Equal(t, 0, len(worker1.srvConn.supportFunctions))
	assert.NotEqual(t, 0, len(worker2.srvConn.supportFunctions))
}
