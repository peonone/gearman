package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"

	uuid "github.com/satori/go.uuid"
)

func TestID(t *testing.T) {
	idGen := NewIDGenerator()

	id1 := idGen.Generate()
	assert.Equal(t, 32, len(id1.String()))
	id1Array := [uuid.Size]byte(*id1)

	id2 := idGen.Generate()
	id2Array := [uuid.Size]byte(*id2)

	assert.NotEqual(t, *id1, *id2)
	assert.NotEqual(t, id1Array, id2Array)
}
