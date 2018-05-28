package server

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

type getStatusTestData struct {
	packet     gearman.PacketType
	known      bool
	running    bool
	num        int
	den        int
	waitingCnt int
}

func TestGetStatusHandler(t *testing.T) {
	jobsManager := new(mockJobsManager)
	conn := gearman.NewMockConn(10, 10)

	handler := &getStatusHandler{jobsManager}

	ctx := context.Background()
	for _, testData := range []*getStatusTestData{
		&getStatusTestData{gearman.GET_STATUS, true, true, 10, 100, 3},
		&getStatusTestData{gearman.GET_STATUS_UNIQUE, true, false, 0, 0, 2},
	} {
		handle := testIdGen.Generate()
		uniqueID := testIdGen.Generate().String()
		var id string

		js := &jobStatus{
			handle:       handle,
			known:        testData.known,
			running:      testData.running,
			numerator:    testData.num,
			denominator:  testData.den,
			waitingCount: testData.waitingCnt,
		}

		var sentPacket gearman.PacketType
		var argsLen int
		switch testData.packet {
		case gearman.GET_STATUS:
			id = handle.String()
			jobsManager.On("getJobStatus", ctx, handle, "").Return(js).Once()
			sentPacket = gearman.STATUS_RES
			argsLen = 5
		case gearman.GET_STATUS_UNIQUE:
			id = uniqueID
			jobsManager.On("getJobStatus", ctx, (*gearman.ID)(nil), uniqueID).Return(js).Once()
			sentPacket = gearman.STATUS_RES_UNIQUE
			argsLen = 6
		}
		msg := &gearman.Message{
			MagicType:  gearman.MagicReq,
			PacketType: testData.packet,
			Arguments:  []string{id},
		}

		handler.Handle(ctx, msg, conn)
		assert.Equal(t, 1, len(conn.WriteCh))
		sentMsg := <-conn.WriteCh
		assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
		assert.Equal(t, sentPacket, sentMsg.PacketType)
		assert.Equal(t, argsLen, len(sentMsg.Arguments))
		assert.Equal(t, handle.String(), sentMsg.Arguments[0])
		switch testData.known {
		case true:
			assert.Equal(t, "1", sentMsg.Arguments[1])
		case false:
			assert.Equal(t, "0", sentMsg.Arguments[1])
		}
		switch testData.running {
		case true:
			assert.Equal(t, "1", sentMsg.Arguments[2])
		case false:
			assert.Equal(t, "0", sentMsg.Arguments[2])
		}
		if testData.running {
			assert.Equal(t, strconv.Itoa(testData.num), sentMsg.Arguments[3])
			assert.Equal(t, strconv.Itoa(testData.den), sentMsg.Arguments[4])
		}
		if testData.packet == gearman.GET_STATUS_UNIQUE {
			assert.Equal(t, strconv.Itoa(testData.waitingCnt), sentMsg.Arguments[5])
		}
	}

	js := &jobStatus{running: false, handle: nil}
	notExistsUniqueID := "not-exists"
	jobsManager.On("getJobStatus", ctx, (*gearman.ID)(nil), notExistsUniqueID).Return(js).Once()
	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.GET_STATUS_UNIQUE,
		Arguments:  []string{notExistsUniqueID},
	}
	handler.Handle(ctx, msg, conn)
	assert.Equal(t, 1, len(conn.WriteCh))
	sentMsg := <-conn.WriteCh
	assert.Equal(t, gearman.MagicRes, sentMsg.MagicType)
	assert.Equal(t, gearman.STATUS_RES_UNIQUE, sentMsg.PacketType)
	assert.Equal(t, "", sentMsg.Arguments[0])
	jobsManager.AssertExpectations(t)
}
