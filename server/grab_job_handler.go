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
	sfManager   *supportFunctionsManager
	jobsManager jobsManager
}

func (h *grabJobHandler) SupportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.GRAB_JOB, gearman.GRAB_JOB_ALL,
	}
}

func (h *grabJobHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	functions := h.sfManager.supportFunctions(conn.ID())
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
		msg := &gearman.Message{
			MagicType:  gearman.MagicRes,
			PacketType: packet,
			Arguments:  args,
		}
		return true, conn.WriteMsg(msg)
	} else if err != nil {
		return true, &serverError{"job_manager", err}
	} else {
		return true, conn.WriteMsg(noJobMsg)
	}
}
