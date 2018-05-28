package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestCanDoHandler(t *testing.T) {
	supportFunctionsManager := newSupportFunctionsManager()
	h := &canDoHandler{supportFunctionsManager}
	conn := gearman.NewMockConn(10, 10)

	assert.Equal(t, 0, len(supportFunctionsManager.supportFunctions(conn.ID())))

	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO,
		Arguments:  []string{"echo"},
	}
	ctx := context.Background()
	msgRecyclable, err := h.Handle(ctx, msg, conn)
	assert.True(t, msgRecyclable)
	assert.Nil(t, err)
	supportedFunctions := supportFunctionsManager.supportFunctions(conn.ID())
	assert.Equal(t, 1, len(supportedFunctions))
	assert.Equal(t, time.Duration(0), supportedFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "echo")
	assert.NotContains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO_TIMEOUT,
		Arguments:  []string{"echo", "32"},
	}
	msgRecyclable, err = h.Handle(ctx, msg, conn)
	assert.Nil(t, err)
	supportedFunctions = supportFunctionsManager.supportFunctions(conn.ID())
	assert.Equal(t, 1, len(supportedFunctions))
	assert.Equal(t, time.Duration(32)*time.Millisecond, supportedFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "echo")
	assert.NotContains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.CAN_DO,
		Arguments:  []string{"reverse"},
	}
	msgRecyclable, err = h.Handle(ctx, msg, conn)
	assert.Nil(t, err)
	supportedFunctions = supportFunctionsManager.supportFunctions(conn.ID())
	assert.Equal(t, 2, len(supportedFunctions))
	assert.Equal(t, time.Duration(0), supportedFunctions.timeout(msg.Arguments[0]))
	assert.Contains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "echo")
	assert.Contains(t, supportFunctionsManager.supportFunctionsSlice(conn.ID()), "reverse")

	msg = &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.RESET_ABILITIES,
	}
	h.Handle(ctx, msg, conn)
	supportedFunctions = supportFunctionsManager.supportFunctions(conn.ID())
	assert.Equal(t, 0, len(supportedFunctions))
}
