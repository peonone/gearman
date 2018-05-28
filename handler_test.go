package gearman

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestHandler(t *testing.T) {
	mng := NewHandlerManager(RoleServer, time.Second*3)

	submitHandler := new(MockHandler)
	workCompleteHandler := new(MockHandler)
	mng.RegisterHandler(SUBMIT_JOB, submitHandler)
	mng.RegisterHandler(SUBMIT_JOB_BG, submitHandler)
	mng.RegisterHandler(WORK_COMPLETE, workCompleteHandler)

	conn := new(MockConn)
	submitMsg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "123", "hello world"},
	}

	submitHandler.On("Handle", mock.Anything, submitMsg, conn).Return(true, nil)
	_, err := mng.HandleMessage(submitMsg, conn)
	assert.Nil(t, err)

	submitBgMsg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB_BG,
		Arguments:  []string{"echo", "123", "hello world"},
	}
	submitHandler.On("Handle", mock.Anything, submitBgMsg, conn).Return(true, nil)
	_, err = mng.HandleMessage(submitBgMsg, conn)
	assert.Nil(t, err)

	workCompleteMsg := &Message{
		MagicType:  MagicReq,
		PacketType: WORK_COMPLETE,
		Arguments:  []string{"echo", "123"},
	}
	workCompleteErr := errors.New("hihi")
	workCompleteHandler.On("Handle", mock.Anything, workCompleteMsg, conn).Return(true, workCompleteErr)
	_, err = mng.HandleMessage(workCompleteMsg, conn)
	assert.Equal(t, workCompleteErr, err)

	submitHandler.AssertExpectations(t)
	workCompleteHandler.AssertExpectations(t)
}
