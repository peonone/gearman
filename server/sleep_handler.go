package server

import (
	"context"

	"github.com/peonone/gearman"
)

type sleepHandler struct {
	sleepManager *sleepManager
}

func (h *sleepHandler) SupportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.PRE_SLEEP,
	}
}

func (h *sleepHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	h.sleepManager.addSleepWorker(conn.ID())
	return true, nil
}
