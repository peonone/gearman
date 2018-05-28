package server

import (
	"context"
	"strconv"
	"time"

	"github.com/peonone/gearman"
)

type resetAbilitiesHandler struct {
	supportFunctionsManager *supportFunctionsManager
}

func (h *resetAbilitiesHandler) SupportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.RESET_ABILITIES,
	}
}

func (h *resetAbilitiesHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	switch m.PacketType {
	case gearman.CAN_DO:
		h.supportFunctionsManager.canDo(conn.ID(), m.Arguments[0], 0)
	case gearman.CAN_DO_TIMEOUT:
		timeoutMili, err := strconv.Atoi(m.Arguments[1])
		if err != nil {
			return true, err
		}
		h.supportFunctionsManager.canDo(conn.ID(), m.Arguments[0], time.Duration(timeoutMili)*time.Millisecond)
	case gearman.CANT_DO:
		h.supportFunctionsManager.cantDo(conn.ID(), m.Arguments[0])
	}
	return true, nil
}
