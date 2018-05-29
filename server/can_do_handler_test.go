package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestCanDoHandler(t *testing.T) {
	h := &canDoHandler{}
	conn := newServerConn(gearman.NewMockConn(10, 10))

	assert.Equal(t, 0, len(conn.supportFunctions))

	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO,
		Arguments:  []string{"echo"},
	}
	ctx := context.Background()
	msgRecyclable, err := h.handle(ctx, msg, conn)
	assert.True(t, msgRecyclable)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(conn.supportFunctions))
	assert.Equal(t, time.Duration(0), conn.supportFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, conn.supportFunctions, "echo")
	assert.NotContains(t, conn.supportFunctions, "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO_TIMEOUT,
		Arguments:  []string{"echo", "32"},
	}
	msgRecyclable, err = h.handle(ctx, msg, conn)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(conn.supportFunctions))
	assert.Equal(t, time.Duration(32)*time.Millisecond, conn.supportFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, conn.supportFunctions, "echo")
	assert.NotContains(t, conn.supportFunctions, "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO,
		Arguments:  []string{"reverse"},
	}
	msgRecyclable, err = h.handle(ctx, msg, conn)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(conn.supportFunctions))
	assert.Equal(t, time.Duration(0), conn.supportFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, conn.supportFunctions, "echo")
	assert.Contains(t, conn.supportFunctions, "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.RESET_ABILITIES,
	}
	h.handle(ctx, msg, conn)
	assert.Equal(t, 0, len(conn.supportFunctions))
}
