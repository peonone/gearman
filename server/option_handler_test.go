package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestOptionHandler(t *testing.T) {
	h := new(optionHandler)
	conn := gearman.NewMockConn(10, 10)
	srvConn := newServerConn(conn)
	assert.False(t, srvConn.forwardException())
	m := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: gearman.OPTION_REQ,
		Arguments:  []string{"exceptions"},
	}
	h.handle(context.Background(), m, srvConn)
	assert.True(t, srvConn.forwardException())
	assert.Equal(t, 1, len(conn.WriteCh))
	sentMsg := <-conn.WriteCh
	assert.Equal(t, gearman.OPTION_RES, sentMsg.PacketType)
	assert.Contains(t, sentMsg.Arguments[0], "exceptions")
}
