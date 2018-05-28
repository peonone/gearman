package gearman

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
)

const separator = "\000"

// PacketType is the type of the packet, such as SUBMIT_JOB / GET_STATUS
type PacketType byte

const (
	// PacketTypeMin is the min packet type value
	PacketTypeMin = 1
	// PacketTypeMax is the max packet type value
	PacketTypeMax = 42
)

// MaxBodySize is the max body size
const MaxBodySize = 63

const headerSize = 12

var (
	errInvalidMagic     = errors.New("Invalid magic code")
	errInvaldPacketType = errors.New("Invalid packet type")
	errInvalidArgsSize  = errors.New("Invalid arguments size")
	errInvalidMsgRole   = errors.New("The message type is unexpected for this role")
	errInvalidArgsLen   = errors.New("The length of arguments is incorrect")
	errArgumentsTooLong = errors.New("Arguments too long")
)

// byte order of the encoding
var byteOrder binary.ByteOrder = binary.BigEndian

// Message represents a REQ/RES packet
type Message struct {
	MagicType  MagicType
	PacketType PacketType
	Arguments  []string
}

// Validate checks the validity of the message and return an error if has
// It validates -
// 1. If the packet type is expected for the current role (Eg. SUBMIT_JOB sent from a client to server is invalid)
// 2. If the length of the arguments is expected
func (m *Message) Validate(role RoleType) error {
	if (role == RoleServer && m.MagicType == MagicRes) || (role != RoleServer && m.MagicType == MagicReq) {
		return errInvalidMsgRole
	}
	allowedRoles, ok := msgAllowedRoles[calcMsgType(m.MagicType, m.PacketType)]
	if !ok {
		return errInvalidMsgRole
	}
	if role != RoleServer {
		if !allowedRoles.hasType(role) {
			return errInvalidMsgRole
		}
	}

	argsLen, ok := msgArgsLens[m.PacketType]
	if ok && len(m.Arguments) != argsLen {
		return errInvalidArgsLen
	}
	return nil
}

// Encode encodes the message to bytes in the gearman official protocol format
func (m *Message) Encode() ([]byte, error) {
	body := strings.Join(m.Arguments, separator)
	for _, arg := range m.Arguments {
		if len(arg) > MaxBodySize {
			return nil, errArgumentsTooLong
		}
	}
	if !m.MagicType.Valid() {
		return nil, errInvalidMagic
	}
	if !m.PacketType.Valid() {
		return nil, errInvaldPacketType
	}
	buff := bytes.NewBuffer(make([]byte, 0, len(body)+headerSize))
	buff.WriteString(m.MagicType.String())
	binary.Write(buff, byteOrder, uint32(m.PacketType))
	// must convert to uint32 as encoding/binary dose not work with int
	binary.Write(buff, byteOrder, uint32(len(body)))
	buff.WriteString(body)
	return buff.Bytes(), nil
}

//WriteTo writes the message data to a Writer
func (m *Message) WriteTo(writer io.Writer) (int64, error) {
	payload, err := m.Encode()
	if err != nil {
		return 0, err
	}
	n, err := writer.Write(payload)
	return int64(n), err
}

// NextMessage reads next message from a Reader
// It reads the massage from reader and decodes it in the gearman official protocol format
// it returns the message and nil if no any error
// otherwise return nil and the error occurred
// it treats error for such cases -
// 1.read error from the reader
// 2. invalid magic code / packet type / body size
// (it will read the full message from the reader in this case, so the next message can be read as expected)
// It dose not care about the validity of the message, message.Validate() should be called for it
func NextMessage(reader io.Reader) (*Message, error) {
	// TODO re-use headers to prevent allocate mem every time
	headers := make([]byte, headerSize)
	_, err := io.ReadFull(reader, headers)
	if err != nil {
		return nil, err
	}
	var magicType MagicType
	var magicErr bool
	switch string(headers[:4]) {
	case MagicReqValue:
		magicType = MagicReq
	case MagicResValue:
		magicType = MagicRes
	default:
		magicErr = true
	}

	var packetType PacketType
	var packetTypeErr bool
	packetType = PacketType(binary.BigEndian.Uint32(headers[4:8]))
	if !magicErr && !packetType.Valid() {
		packetTypeErr = true
	}

	bodySize := byteOrder.Uint32(headers[8:])

	var arguments []string
	var bodySizeErr bool
	if !magicErr && !packetTypeErr {
		if bodySize == 0 {
			arguments = nil
		} else {
			body := make([]byte, bodySize)
			_, err = io.ReadFull(reader, body)
			if err != nil {
				return nil, err
			}
			arguments = strings.Split(string(body), separator)
		}
	} else {
		// we don't need the arguments but still need read it from the connection
		// to make sure the next message can be read properly
		_, err = io.CopyN(ioutil.Discard, reader, int64(bodySize))
		if err != nil {
			return nil, err
		}
		bodySizeErr = true
	}
	if magicErr {
		return nil, errInvalidMagic
	}
	if packetTypeErr {
		return nil, errInvaldPacketType
	}
	if bodySizeErr {
		return nil, errInvalidArgsSize
	}

	// TODO re-use Message struct to prevent allocate mem every time
	return &Message{
		MagicType:  magicType,
		PacketType: packetType,
		Arguments:  arguments,
	}, nil
}

func (m *Message) String() string {
	return fmt.Sprintf("%s.%s", m.MagicType.String()[1:], m.PacketType)
}
