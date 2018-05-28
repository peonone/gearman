package server

import (
	"context"

	gearman "github.com/peonone/gearman"
)

var wsSupportPacketTypes = []gearman.PacketType{
	gearman.WORK_STATUS, gearman.WORK_WARNING, gearman.WORK_DATA,
	gearman.WORK_COMPLETE, gearman.WORK_FAIL, gearman.WORK_EXCEPTION,
}

type workStatusHandler struct {
	jobsManager jobsManager
	connManager *gearman.ConnManager
}

func (h *workStatusHandler) SupportPacketTypes() []gearman.PacketType {
	return wsSupportPacketTypes
}

func (h *workStatusHandler) Handle(ctx context.Context, m *gearman.Message, conn gearman.Conn) (bool, error) {
	handle, err := gearman.UnmarshalID(m.Arguments[0])
	if err != nil {
		return false, err
	}
	h.jobsManager.updateJobStatus(ctx, handle, m)
	return false, nil
}
