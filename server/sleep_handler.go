package server

import (
	"context"

	"github.com/peonone/gearman"
)

type sleepHandler struct {
	sleepManager *sleepManager
}

func (h *sleepHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.PRE_SLEEP,
	}
}

func (h *sleepHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	h.sleepManager.addSleepWorker(conn.ID())
	return true, nil
}
