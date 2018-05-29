package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/peonone/gearman"
)

func TestSetClientIDHandler(t *testing.T) {
	conn1 := newMockSConn(10, 10)
	conn2 := newMockSConn(10, 10)
	h := new(setClientIDHandler)
	assert.Equal(t, "", conn1.srvConn.getClientID())
	clientID := "12345678"
	m := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.SET_CLIENT_ID,
		Arguments:  []string{clientID},
	}
	h.handle(context.Background(), m, conn1.srvConn)
	assert.Equal(t, clientID, conn1.srvConn.getClientID())
	assert.Equal(t, "", conn2.srvConn.getClientID())
}
