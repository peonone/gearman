package server

import (
	"context"

	"github.com/peonone/gearman"
)

type echoHandler struct {
}

func (h *echoHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.ECHO_REQ,
	}
}

func (h *echoHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	msg := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: gearman.ECHO_RES,
		Arguments:  m.Arguments,
	}
	return true, conn.WriteMsg(msg)
}
