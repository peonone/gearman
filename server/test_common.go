package server

import (
	"log"
	"os"

	"github.com/peonone/gearman"
)

var testIdGen = gearman.NewIDGenerator()
var testLogger = log.New(os.Stderr, "", log.LstdFlags)
