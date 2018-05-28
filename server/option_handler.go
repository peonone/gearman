package server

import (
	"context"
	"strings"

	"github.com/peonone/gearman"
)

const exceptionsOption = "exceptions"

type optionHandler struct {
}

func (h *optionHandler) SupportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.OPTION_REQ,
	}
}

func (h *optionHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	optionsSet := ""
	if strings.Contains(m.Arguments[0], exceptionsOption) {
		conn.Option().SetForwardException(true)
		optionsSet = exceptionsOption
	}
	msg := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: gearman.OPTION_RES,
		Arguments:  []string{optionsSet},
	}
	return true, conn.WriteMsg(msg)
}
