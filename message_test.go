package gearman

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func longString(n int) string {
	buff := make([]byte, n)
	for i := 0; i < n; i++ {
		buff[i] = 'a'
	}
	return string(buff)
}

// encode the message without arguments size validation
func (m *Message) encodeWithoutValidation() []byte {
	body := strings.Join(m.Arguments, separator)
	buff := bytes.NewBuffer(make([]byte, 0, len(body)+12))
	buff.WriteString(m.MagicType.String())
	binary.Write(buff, byteOrder, uint32(m.PacketType))
	// must convert to uint32 as encoding/binary dose not work with int
	binary.Write(buff, byteOrder, uint32(len(body)))
	buff.WriteString(body)
	return buff.Bytes()
}

func TestEncode(t *testing.T) {
	// normal case
	msg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "111", "hello world"},
	}
	encodedBytes, err := msg.Encode()
	assert.Nil(t, err)
	assert.NotNil(t, encodedBytes)

	//Invalid magic type
	msg.MagicType = 32
	encodedBytes, err = msg.Encode()
	assert.Equal(t, errInvalidMagic, err)
	assert.Nil(t, encodedBytes)

	//Invalid packet type
	msg.MagicType = MagicReq
	msg.PacketType = PacketTypeMax + 5
	encodedBytes, err = msg.Encode()
	assert.Equal(t, errInvaldPacketType, err)
	assert.Nil(t, encodedBytes)

	// Arguments too long
	msg.PacketType = SUBMIT_JOB
	msg.Arguments = []string{"echo", "1234", longString(65)}
	encodedBytes, err = msg.Encode()
	assert.Equal(t, errArgumentsTooLong, err)
	assert.Nil(t, encodedBytes)
}

func TestDecode(t *testing.T) {
	msg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "111", "hello world"},
	}
	encodedBytes, err := msg.Encode()
	assert.Nil(t, err)
	reader := bufio.NewReader(bytes.NewReader(encodedBytes))
	decodedMsg, err := NextMessage(reader)
	assert.Equal(t, msg, decodedMsg)
	assert.Nil(t, err)
	decodedMsg, err = NextMessage(reader)
	assert.Nil(t, decodedMsg)
	assert.Equal(t, io.EOF, err)

	//Invalid magic type
	msg.MagicType = 32
	encodedBytes = msg.encodeWithoutValidation()
	reader = bufio.NewReader(bytes.NewReader(encodedBytes))
	decodedMsg, err = NextMessage(reader)
	assert.Nil(t, decodedMsg)
	assert.Equal(t, errInvalidMagic, err)

	//Invalid packet type
	msg.MagicType = MagicReq
	msg.PacketType = 232
	encodedBytes = msg.encodeWithoutValidation()
	reader = bufio.NewReader(bytes.NewReader(encodedBytes))
	decodedMsg, err = NextMessage(reader)
	assert.Nil(t, decodedMsg)
	assert.Equal(t, errInvaldPacketType, err)
}

func TestValidate(t *testing.T) {
	// Request
	msg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "111", "hello world"},
	}
	assert.Nil(t, msg.Validate(RoleServer))
	assert.Equal(t, errInvalidMsgRole, msg.Validate(RoleWorker))

	// Response
	msg = &Message{
		MagicType:  MagicRes,
		PacketType: JOB_ASSIGN,
		Arguments:  []string{"1111", "echo", "hello world"},
	}
	assert.Nil(t, msg.Validate(RoleWorker))
	assert.Error(t, errInvalidMsgRole, msg.Validate(RoleClient))
	assert.Error(t, errInvalidMsgRole, msg.Validate(RoleServer))

	// Two roles
	msg = &Message{
		MagicType:  MagicRes,
		PacketType: ECHO_RES,
		Arguments:  []string{"hello world"},
	}
	assert.Nil(t, msg.Validate(RoleClient))
	assert.Nil(t, msg.Validate(RoleWorker))
	assert.Equal(t, errInvalidMsgRole, msg.Validate(RoleServer))

	// Args length
	msg = &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{},
	}
	assert.Equal(t, errInvalidArgsLen, msg.Validate(RoleServer))
}

func TestContinousRead(t *testing.T) {
	msg := &Message{
		MagicType:  MagicRes,
		PacketType: 243,
		Arguments:  []string{"echo", "111", "hello world"},
	}
	encodededMsgs := make([][]byte, 0, 3)
	// an invalid message first
	encodededMsgs = append(encodededMsgs, msg.encodeWithoutValidation())
	msg.PacketType = WORK_COMPLETE
	// valid one
	encodededMsgs = append(encodededMsgs, msg.encodeWithoutValidation())

	buff := bytes.NewBuffer(bytes.Join(encodededMsgs, nil))
	decodedMsg, err := NextMessage(buff)
	assert.Nil(t, decodedMsg)
	assert.Equal(t, errInvaldPacketType, err)

	// unread data size should equals with the second packet size
	assert.Equal(t, len(encodededMsgs[1]), len(buff.Bytes()))

	decodedMsg, err = NextMessage(buff)
	assert.Nil(t, err)
	assert.Equal(t, msg, decodedMsg)
}

type repeatReader struct {
	content []byte
	pos     int
}

func (r *repeatReader) Read(b []byte) (int, error) {
	readCnt := 0
	needCnt := len(b)
	contentSize := len(r.content)
	for readCnt < needCnt {
		n := copy(b[readCnt:], r.content[r.pos:])
		readCnt += n
		r.pos += n

		if r.pos >= contentSize {
			r.pos %= contentSize
		}
	}
	return readCnt, nil
}

func BenchmarkDecode(b *testing.B) {
	msg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "1234567890123456", "hello world, blabla blabla"},
	}
	encodedBytes, err := msg.Encode()
	if err != nil {
		b.FailNow()
		return
	}
	reader := bufio.NewReader(&repeatReader{content: encodedBytes})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NextMessage(reader)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	msg := &Message{
		MagicType:  MagicReq,
		PacketType: SUBMIT_JOB,
		Arguments:  []string{"echo", "1234567890123456", "hello world, blabla blabla"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := msg.Encode()
		if err != nil {
			b.Fail()
		}
	}
}
