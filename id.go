package gearman

import (
	"encoding/hex"

	uuid "github.com/satori/go.uuid"
)

// ID is the type of identity
// The underlying type is byte array, so use ID as key of a map,
// but use it's pointer for assignments and arguments to avoid memery copy
type ID uuid.UUID

// IDGenerator is a generator of ID
type IDGenerator struct {
}

// IDStrLength is the length of ID string
const IDStrLength = 32

// NewIDGenerator creates a new ID generator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

// Generate generates a new ID
func (g *IDGenerator) Generate() *ID {
	id := ID(uuid.NewV4())
	return &id
}

func (id *ID) String() string {
	buf := make([]byte, IDStrLength)
	hex.Encode(buf, (*id)[:])
	return string(buf)
}

//UnmarshalID unmarshal an ID from string
func UnmarshalID(str string) (*ID, error) {
	uuidID, err := uuid.FromString(str)
	if err != nil {
		return nil, err
	}
	id := ID(uuidID)
	return &id, nil
}
