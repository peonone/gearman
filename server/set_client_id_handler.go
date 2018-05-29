package server

import (
	"context"

	"github.com/peonone/gearman"
)

type setClientIDHandler struct {
}

func (h *setClientIDHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.SET_CLIENT_ID,
	}
}

func (h *setClientIDHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	conn.setClientID(m.Arguments[0])
	return true, nil
}
