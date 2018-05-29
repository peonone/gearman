package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestGrabJobHandler(t *testing.T) {
	jobsManager := new(mockJobsManager)

	h := &grabJobHandler{jobsManager}

	workerConn := gearman.NewMockConn(10, 10)
	workerSrvConn := newServerConn(workerConn)
	ctx := context.Background()

	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.GRAB_JOB,
	}
	msgRecyclable, err := h.handle(ctx, msg, workerSrvConn)
	assert.True(t, msgRecyclable)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workerConn.WriteCh))
	assert.Equal(t, noJobMsg, <-workerConn.WriteCh)

	workerSrvConn.supportFunctions.canDo("echo", 0)
	workerSrvConn.supportFunctions.canDo("wc", time.Second)
	j := &job{
		function: "echo",
		data:     "data",
		handle:   testIdGen.Generate(),
		uniqueID: "12345",
		reducer:  "reduce1",
	}
	for _, packet := range h.supportPacketTypes() {
		msg := &gearman.Message{
			MagicType:  gearman.MagicReq,
			PacketType: packet,
		}
		jobsManager.On("grabJob", ctx, workerSrvConn.supportFunctions).Return(nil, nil).Once()
		msgRecyclable, err = h.handle(ctx, msg, workerSrvConn)
		assert.Equal(t, 1, len(workerConn.WriteCh))
		assert.Equal(t, noJobMsg, <-workerConn.WriteCh)

		jobsManager.On("grabJob", ctx, workerSrvConn.supportFunctions).Return(j, nil).Once()
		msgRecyclable, err = h.handle(ctx, msg, workerSrvConn)
		assert.True(t, msgRecyclable)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(workerConn.WriteCh))
		sentMsg := <-workerConn.WriteCh
		assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
		switch packet {
		case gearman.GRAB_JOB:
			assert.Equal(t, gearman.JOB_ASSIGN, sentMsg.PacketType)
			assert.Equal(t, j.handle.String(), sentMsg.Arguments[0])
			assert.Equal(t, j.function, sentMsg.Arguments[1])
			assert.Equal(t, j.data, sentMsg.Arguments[2])
		case gearman.GRAB_JOB_ALL:
			assert.Equal(t, gearman.JOB_ASSIGN_ALL, sentMsg.PacketType)
			assert.Equal(t, j.handle.String(), sentMsg.Arguments[0])
			assert.Equal(t, j.function, sentMsg.Arguments[1])
			assert.Equal(t, j.uniqueID, sentMsg.Arguments[2])
			assert.Equal(t, j.reducer, sentMsg.Arguments[3])
			assert.Equal(t, j.data, sentMsg.Arguments[4])
		}
	}

	jobsManager.On("grabJob", ctx, workerSrvConn.supportFunctions).Return(nil, errors.New("internal error")).Once()
	msgRecyclable, err = h.handle(ctx, msg, workerSrvConn)
	assert.NotNil(t, err)
}
