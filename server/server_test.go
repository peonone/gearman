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
		cfg:                     cfg,
		logger:                  logger,
		logf:                    nil,
		queue:                   q,
		jobHandleGenerator:      testIdGen,
		clientIDGenerator:       testIdGen,
		supportFunctionsManager: newSupportFunctionsManager(),
		handlersMng:             gearman.NewHandlerManager(gearman.RoleServer, 0),
		connManager:             gearman.NewConnManager(),
	}
	submitHandler := &gearman.MockHandler{}
	echoHandler := &gearman.MockHandler{}

	s.handlersMng.RegisterHandler(gearman.SUBMIT_JOB, submitHandler)
	s.handlersMng.RegisterHandler(gearman.ECHO_REQ, echoHandler)
	conn := gearman.NewMockConn(5, 5)

	echoMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.ECHO_REQ,
		Arguments:  []string{"hello world"},
	}
	conn.ReadCh <- echoMsg
	echoHandler.On("Handle", mock.Anything, echoMsg, conn).Return(true, nil).Once()

	submitMsg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SUBMIT_JOB,
		Arguments:  []string{"echo", "123", "hihi"},
	}
	conn.ReadCh <- submitMsg
	submitHandler.On("Handle", mock.Anything, submitMsg, conn).Return(true, errors.New("err")).Once()

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
	submitHandler.On("Handle", mock.Anything, submitMsg2, conn).Return(true, serverErr).Once()
	close(conn.ReadCh)
	s.serve(conn)

	assert.Equal(t, 1, len(conn.WriteCh))
	sentMsg := <-conn.WriteCh
	assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
	assert.Equal(t, gearman.ERROR, sentMsg.PacketType)
	assert.Equal(t, []string{serverErr.code, serverErr.err.Error()}, sentMsg.Arguments)
}
