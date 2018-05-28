package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMagicType(t *testing.T) {
	req := MagicReq
	assert.Equal(t, MagicReqValue, req.String())
	assert.True(t, req.Valid())

	res := MagicRes
	assert.True(t, res.Valid())
	assert.Equal(t, MagicResValue, res.String())

	invalid := MagicType(MagicReq + MagicRes)
	assert.False(t, invalid.Valid())
}

func TestPacket(t *testing.T) {
	submit := SUBMIT_JOB
	assert.True(t, submit.Valid())
	assert.Equal(t, "SUBMIT_JOB", submit.String())

	invalid := PacketType(PacketTypeMax + 1)
	assert.False(t, invalid.Valid())
}
