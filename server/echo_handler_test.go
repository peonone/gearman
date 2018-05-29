package server

import (
	"context"
	"testing"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/assert"
)

func TestEchoHandler(t *testing.T) {
	h := &echoHandler{}

	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.ECHO_REQ,
		Arguments:  []string{"hello"},
	}
	conn := newMockSConn(10, 10)
	ctx := context.Background()
	msgRecyclable, err := h.handle(ctx, msg, conn.srvConn)
	assert.True(t, msgRecyclable)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(conn.WriteCh))

	sentMsg := <-conn.WriteCh
	assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
	assert.Equal(t, gearman.ECHO_RES, sentMsg.PacketType)
	assert.Equal(t, msg.Arguments, sentMsg.Arguments)
}
