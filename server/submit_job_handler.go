package server

import (
	"context"

	"github.com/peonone/gearman"
)

type submitJobHandler struct {
	handleGen    *gearman.IDGenerator
	sleepManager *sleepManager
	jobsManager  jobsManager
	connManager  *gearman.ConnManager
}

func (h *submitJobHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.SUBMIT_JOB,
		gearman.SUBMIT_JOB_BG,
		gearman.SUBMIT_JOB_HIGH,
		gearman.SUBMIT_JOB_HIGH_BG,
		gearman.SUBMIT_JOB_LOW,
		gearman.SUBMIT_JOB_LOW_BG,
		gearman.SUBMIT_REDUCE_JOB,
		gearman.SUBMIT_REDUCE_JOB_BACKGROUND,
	}
}

func (h *submitJobHandler) handle(ctx context.Context, m *gearman.Message, con *conn) (bool, error) {
	var bg jobBackgroud
	var priority priority

	switch m.PacketType {
	case gearman.SUBMIT_JOB_BG, gearman.SUBMIT_JOB_HIGH_BG, gearman.SUBMIT_JOB_LOW_BG, gearman.SUBMIT_REDUCE_JOB_BACKGROUND:
		bg = backgroud
	default:
		bg = nonBackgroud
	}

	switch m.PacketType {
	case gearman.SUBMIT_JOB_HIGH, gearman.SUBMIT_JOB_HIGH_BG:
		priority = priorityHigh
	case gearman.SUBMIT_JOB_LOW, gearman.SUBMIT_JOB_LOW_BG:
		priority = priorityLow
	default:
		priority = priorityMid
	}
	jobH := h.handleGen.Generate()

	j := &job{
		function: m.Arguments[0],
		handle:   jobH,
		uniqueID: m.Arguments[1],
		priority: priority,
	}
	var listenConn *conn
	if bg == nonBackgroud {
		listenConn = con
	}
	if m.PacketType == gearman.SUBMIT_REDUCE_JOB || m.PacketType == gearman.SUBMIT_REDUCE_JOB_BACKGROUND {
		j.reducer = m.Arguments[2]
		j.data = m.Arguments[3]
	} else {
		j.data = m.Arguments[2]
	}
	jobH, err := h.jobsManager.submitJob(ctx, j, listenConn)
	if err != nil {
		return false, err
	}
	for _, sleepID := range h.sleepManager.allSleepingConnIDs() {
		workerConn := h.connManager.GetConn(sleepID)
		if workerConn == nil {
			continue
		}
		workerConnSrv := workerConn.(*conn)
		if workerConnSrv.supportFunctions.support(j.function) {
			msg := &gearman.Message{
				MagicType:  gearman.MagicRes,
				PacketType: gearman.NOOP,
			}
			workerConn.WriteMsg(msg)
			break
		}
	}

	respMsg := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: gearman.JOB_CREATED,
		Arguments:  []string{jobH.String()},
	}
	return true, con.WriteMsg(respMsg)
}
