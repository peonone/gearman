package server

import (
	"errors"
	"log"
	"os"
	"testing"

	gearman "github.com/peonone/gearman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestServe(t *testing.T) {
	cfg := &Config{}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Ltime)
	q := &mockQueue{}
	s := &Server{
		cfg:                cfg,
		logger:             logger,
		logf:               nil,
		queue:              q,
		jobHandleGenerator: testIdGen,
		clientIDGenerator:  testIdGen,
		handlersMng:        newServerHandlerManager(0),
		connManager:        gearman.NewConnManager(),
	}
	submitHandler := &MockHandler{}
	echoHandler := &MockHandler{}

	s.handlersMng.registerHandler(gearman.SUBMIT_JOB, submitHandler)
	s.handlersMng.registerHandler(gearman.ECHO_REQ, echoHandler)
	conn := newMockSConn(10, 10)

	echoMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.ECHO_REQ,
		Arguments:  []string{"hello world"},
	}
	conn.ReadCh <- echoMsg
	echoHandler.On("handle", mock.Anything, echoMsg, conn.srvConn).Return(true, nil).Once()

	submitMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB,
		Arguments:  []string{"echo", "123", "hihi"},
	}
	conn.ReadCh <- submitMsg
	submitHandler.On("handle", mock.Anything, submitMsg, conn.srvConn).Return(true, errors.New("err")).Once()

	submitMsg2 := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB,
		Arguments:  []string{"echo", "1234", "hihi"},
	}
	conn.ReadCh <- submitMsg2
	serverErr := &serverError{
		code: "0001",
		err:  errors.New("first error"),
	}
	submitHandler.On("handle", mock.Anything, submitMsg2, conn.srvConn).Return(true, serverErr).Once()
	close(conn.ReadCh)
	s.serve(conn.srvConn)

	assert.Equal(t, 1, len(conn.WriteCh))
	sentMsg := <-conn.WriteCh
	assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
	assert.Equal(t, gearman.ERROR, sentMsg.PacketType)
	assert.Equal(t, []string{serverErr.code, serverErr.err.Error()}, sentMsg.Arguments)
}
