package server

import (
	"context"

	gearman "github.com/peonone/gearman"
)

// TODO add support for gearman.GRAB_JOB_UNIQ,

var noJobMsg = &gearman.Message{
	MagicType:  gearman.MagicRes,
	PacketType: gearman.NO_JOB,
}

type grabJobHandler struct {
	jobsManager jobsManager
}

func (h *grabJobHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.GRAB_JOB, gearman.GRAB_JOB_ALL,
	}
}

func (h *grabJobHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	functions := conn.supportFunctions
	if len(functions) == 0 {
		return true, conn.WriteMsg(noJobMsg)
	}
	j, err := h.jobsManager.grabJob(ctx, functions)
	if j != nil {
		var args []string
		var packet gearman.PacketType
		switch m.PacketType {
		case gearman.GRAB_JOB:
			packet = gearman.JOB_ASSIGN
			args = []string{j.handle.String(), j.function, j.data}
		case gearman.GRAB_JOB_ALL:
			packet = gearman.JOB_ASSIGN_ALL
			args = []string{j.handle.String(), j.function, j.uniqueID, j.reducer, j.data}
		}
		msg := gearman.MsgPool.Get()
		defer gearman.MsgPool.Put(msg)
		msg.MagicType = gearman.MagicRes
		msg.PacketType = packet
		msg.Arguments = args
		return true, conn.WriteMsg(msg)
	} else if err != nil {
		return true, &serverError{"job_manager", err}
	} else {
		return true, conn.WriteMsg(noJobMsg)
	}
}
