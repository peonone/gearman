package server

import (
	"context"
	"strings"

	"github.com/peonone/gearman"
)

const exceptionsOption = "exceptions"

type optionHandler struct {
}

func (h *optionHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.OPTION_REQ,
	}
}

func (h *optionHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	optionsSet := ""
	if strings.Contains(m.Arguments[0], exceptionsOption) {
		conn.setForwardException(true)
		optionsSet = exceptionsOption
	}
	msg := gearman.MsgPool.Get()
	defer gearman.MsgPool.Put(msg)
	msg.MagicType = gearman.MagicRes
	msg.PacketType = gearman.OPTION_RES
	msg.Arguments = []string{optionsSet}
	return true, conn.WriteMsg(msg)
}
