package server

import (
	"context"

	"github.com/peonone/gearman"
)

type echoHandler struct {
}

func (h *echoHandler) SupportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.ECHO_REQ,
	}
}

func (h *echoHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	msg := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: gearman.ECHO_RES,
		Arguments:  m.Arguments,
	}
	return true, conn.WriteMsg(msg)
}
