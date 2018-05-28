package server

import (
	"errors"

	gearman "github.com/peonone/gearman"
)

type priority byte

const (
	priorityHigh priority = iota
	priorityMid
	priorityLow
)

type jobBackgroud bool

const (
	backgroud    jobBackgroud = true
	nonBackgroud jobBackgroud = false
)

var errInvalidClientIDs = errors.New("invalid clients IDs")

func (b jobBackgroud) intValue() int {
	if b == backgroud {
		return 1
	}
	return 0
}

// job is the structure of job received from client and stored in the queue
type job struct {
	function string
	data     string
	handle   *gearman.ID //The identity generated on the server side
	uniqueID string      //The identity from the client (for coalescing)
	priority priority
	reducer  string
}
