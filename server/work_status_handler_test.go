package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	gearman "github.com/peonone/gearman"
)

func TestWorkStatusHandler(t *testing.T) {
	jobsManager := new(mockJobsManager)
	connManager := gearman.NewConnManager()
	worker := newMockSConn(10, 10)
	h := &workStatusHandler{jobsManager, connManager}

	handle := testIdGen.Generate()
	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.WORK_COMPLETE,
		Arguments:  []string{handle.String(), ""},
	}

	ctx := context.Background()
	jobsManager.On("updateJobStatus", ctx, handle, msg).Return(true).Once()
	msgRecyclable, err := h.handle(ctx, msg, worker.srvConn)
	assert.False(t, msgRecyclable)
	assert.Nil(t, err)
	jobsManager.AssertExpectations(t)
}
