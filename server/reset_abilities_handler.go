package server

import (
	"context"
	"strconv"
	"time"

	"github.com/peonone/gearman"
)

type resetAbilitiesHandler struct {
}

func (h *resetAbilitiesHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.RESET_ABILITIES,
	}
}

func (h *resetAbilitiesHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	switch m.PacketType {
	case gearman.CAN_DO:
		conn.supportFunctions.canDo(m.Arguments[0], 0)
	case gearman.CAN_DO_TIMEOUT:
		timeoutMili, err := strconv.Atoi(m.Arguments[1])
		if err != nil {
			return true, err
		}
		conn.supportFunctions.canDo(m.Arguments[0], time.Duration(timeoutMili)*time.Millisecond)
	case gearman.CANT_DO:
		conn.supportFunctions.cantDo(m.Arguments[0])
	case gearman.RESET_ABILITIES:
		conn.supportFunctions.reset()
	}
	return true, nil
}
