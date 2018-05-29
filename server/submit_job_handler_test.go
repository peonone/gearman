package server

import (
	"context"
	"testing"

	gearman "github.com/peonone/gearman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type submitTestData struct {
	packet gearman.PacketType
	args   []string
}

var submitTestDatas = []*submitTestData{
	&submitTestData{
		gearman.SUBMIT_JOB,
		[]string{"echo", "123456", "hello world"},
	},
	&submitTestData{
		gearman.SUBMIT_JOB_LOW_BG,
		[]string{"echo", "1234567", "hello world2"},
	},
	&submitTestData{
		gearman.SUBMIT_REDUCE_JOB,
		[]string{"echo", "12345678", "redu1", "hello world3"},
	},
	&submitTestData{
		gearman.SUBMIT_REDUCE_JOB_BACKGROUND,
		[]string{"echo", "12345678", "redu2", "hello world4"},
	},
}

func TestSubmitJobHandler(t *testing.T) {
	client := newMockSConn(10, 10)
	sleepManager := newSleepManager()
	jobsManager := new(mockJobsManager)
	connManager := gearman.NewConnManager()
	handler := &submitJobHandler{testIdGen, sleepManager, jobsManager, connManager}
	ctx := context.Background()
	for i, testData := range submitTestDatas {
		submitMsg := &gearman.Message{
			MagicType:  gearman.MagicReq,
			PacketType: testData.packet,
			Arguments:  testData.args,
		}

		var priority priority

		var listenConn *conn
		switch testData.packet {
		case gearman.SUBMIT_JOB_BG, gearman.SUBMIT_JOB_HIGH_BG, gearman.SUBMIT_JOB_LOW_BG, gearman.SUBMIT_REDUCE_JOB_BACKGROUND:
		default:
			listenConn = client.srvConn
		}

		switch testData.packet {
		case gearman.SUBMIT_JOB_HIGH, gearman.SUBMIT_JOB_HIGH_BG:
			priority = priorityHigh
		case gearman.SUBMIT_JOB_LOW, gearman.SUBMIT_JOB_LOW_BG:
			priority = priorityLow
		default:
			priority = priorityMid
		}
		dataIdx := 2
		if testData.packet == gearman.SUBMIT_REDUCE_JOB || testData.packet == gearman.SUBMIT_REDUCE_JOB_BACKGROUND {
			dataIdx = 3
		}

		jobsManager.On("submitJob", ctx, mock.Anything, listenConn).Return("in-param", nil)
		msgRecyclable, err := handler.handle(ctx, submitMsg, client.srvConn)
		assert.True(t, msgRecyclable)
		assert.Nil(t, err)
		j := jobsManager.Calls[i].Arguments[1].(*job)
		assert.Equal(t, testData.args[0], j.function)
		assert.Equal(t, testData.args[1], j.uniqueID)
		assert.Equal(t, priority, j.priority)

		assert.Equal(t, 1, len(client.WriteCh))
		sentClientMsg := <-client.WriteCh
		assert.Equal(t, gearman.MagicRes, sentClientMsg.MagicType)
		assert.Equal(t, gearman.JOB_CREATED, sentClientMsg.PacketType)
		assert.Equal(t, j.handle.String(), sentClientMsg.Arguments[0])

		assert.Equal(t, testData.args[dataIdx], j.data)
		jobsManager.AssertExpectations(t)
	}
}

func TestSubmitJobHandlerNoopSleep(t *testing.T) {
	client := newMockSConn(10, 10)
	sleepManager := newSleepManager()
	jobsManager := new(mockJobsManager)
	connManager := gearman.NewConnManager()

	handler := &submitJobHandler{testIdGen, sleepManager, jobsManager, connManager}
	worker := newMockSConn(10, 10)
	connManager.AddConn(worker.srvConn)
	sleepManager.addSleepWorker(worker.ID())
	worker.srvConn.supportFunctions.canDo("echo1", 0)
	ctx := context.Background()
	m := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB,
		Arguments:  []string{"echo", "123456", "hello world"},
	}
	jobsManager.On("submitJob", ctx, mock.Anything, mock.Anything).Return("in-param", nil).Once()
	handler.handle(ctx, m, client.srvConn)
	assert.Equal(t, 0, len(worker.WriteCh))
	worker.srvConn.supportFunctions.canDo("echo", 0)
	jobsManager.On("submitJob", ctx, mock.Anything, mock.Anything).Return("in-param", nil).Once()
	handler.handle(ctx, m, client.srvConn)
	assert.Equal(t, 1, len(worker.WriteCh))
	msg := <-worker.WriteCh
	assert.Equal(t, gearman.MagicRes, msg.MagicType)
	assert.Equal(t, gearman.NOOP, msg.PacketType)
}
