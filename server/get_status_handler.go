package server

import (
	"context"
	"strconv"

	"github.com/peonone/gearman"
)

type getStatusHandler struct {
	jobsManager jobsManager
}

func (h *getStatusHandler) supportPacketTypes() []gearman.PacketType {
	return []gearman.PacketType{
		gearman.GET_STATUS, gearman.GET_STATUS_UNIQUE,
	}
}

func (h *getStatusHandler) handle(ctx context.Context, m *gearman.Message, conn *conn) (bool, error) {
	var handle *gearman.ID
	var uniqueID string
	var err error
	var packet gearman.PacketType
	var argsLen int

	switch m.PacketType {
	case gearman.GET_STATUS:
		handle, err = gearman.UnmarshalID(m.Arguments[0])
		if err != nil {
			return true, err
		}
		packet = gearman.STATUS_RES
		argsLen = 5
	case gearman.GET_STATUS_UNIQUE:
		uniqueID = m.Arguments[0]
		packet = gearman.STATUS_RES_UNIQUE
		argsLen = 6
	}
	jobStatus := h.jobsManager.getJobStatus(ctx, handle, uniqueID)
	args := make([]string, argsLen)

	knownStr, runningStr, numStr, denStr, waitingCntStr := "0", "0", "0", "0", "0"
	if jobStatus.known {
		knownStr = "1"
		if jobStatus.running {
			runningStr = "1"
			numStr = strconv.Itoa(jobStatus.numerator)
			denStr = strconv.Itoa(jobStatus.denominator)
		}
		waitingCntStr = strconv.Itoa(jobStatus.waitingCount)
	}
	if jobStatus.handle != nil {
		args[0] = jobStatus.handle.String()
	}
	args[1] = knownStr
	args[2] = runningStr
	args[3] = numStr
	args[4] = denStr
	if m.PacketType == gearman.GET_STATUS_UNIQUE {
		args[5] = waitingCntStr
	}
	msg := &gearman.Message{
		MagicType:  gearman.MagicRes,
		PacketType: packet,
		Arguments:  args,
	}
	return true, conn.WriteMsg(msg)
}
