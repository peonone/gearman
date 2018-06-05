package server

import (
	"errors"
	"testing"
	"time"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestHandler(t *testing.T) {
	mng := newServerHandlerManager(time.Second * 3)

	submitHandler := new(MockHandler)
	workCompleteHandler := new(MockHandler)
	mng.registerHandler(gearman.SUBMIT_JOB, submitHandler)
	mng.registerHandler(gearman.SUBMIT_JOB_BG, submitHandler)
	mng.registerHandler(gearman.WORK_COMPLETE, workCompleteHandler)

	conn := newMockSConn(10, 10)
	submitMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB,
		Arguments:  []string{"echo", "123", "hello world"},
	}

	submitHandler.On("handle", mock.Anything, submitMsg, conn.srvConn).Return(true, nil)
	_, err := mng.handleMessage(submitMsg, conn.srvConn)
	assert.Nil(t, err)

	submitBgMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB_BG,
		Arguments:  []string{"echo", "123", "hello world"},
	}
	submitHandler.On("handle", mock.Anything, submitBgMsg, conn.srvConn).Return(true, nil)
	_, err = mng.handleMessage(submitBgMsg, conn.srvConn)
	assert.Nil(t, err)

	workCompleteMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.WORK_COMPLETE,
		Arguments:  []string{"echo", "123"},
	}
	workCompleteErr := errors.New("hihi")
	workCompleteHandler.On("handle", mock.Anything, workCompleteMsg, conn.srvConn).Return(true, workCompleteErr)
	_, err = mng.handleMessage(workCompleteMsg, conn.srvConn)
	assert.Equal(t, workCompleteErr, err)

	submitHandler.AssertExpectations(t)
	workCompleteHandler.AssertExpectations(t)
}
