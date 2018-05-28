package server

import (
	"context"
	"testing"

	"github.com/peonone/gearman"

	"github.com/stretchr/testify/assert"
)

func TestSleepHandler(t *testing.T) {
	sleepMng := newSleepManager()
	h := sleepHandler{sleepMng}

	assert.Equal(t, 0, len(sleepMng.allSleepingConnIDs()))
	worker1 := gearman.NewMockConn(10, 10)
	worker2 := gearman.NewMockConn(10, 10)
	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.PRE_SLEEP,
	}
	ctx := context.Background()
	h.Handle(ctx, msg, worker1)
	sleepIDs := sleepMng.allSleepingConnIDs()
	assert.Equal(t, 1, len(sleepIDs))
	assert.Contains(t, sleepIDs, worker1.ID())
	assert.NotContains(t, sleepIDs, worker2.ID())

	h.Handle(ctx, msg, worker2)
	sleepIDs = sleepMng.allSleepingConnIDs()
	assert.Equal(t, 2, len(sleepIDs))
	assert.Contains(t, sleepIDs, worker1.ID())
	assert.Contains(t, sleepIDs, worker2.ID())

}
