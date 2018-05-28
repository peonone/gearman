package server

import (
	"time"
)

type Config struct {
	BindAddr        string
	LogFilePath     string
	LogToStderr     bool
	Verbose         bool
	QueueType       string
	QueueDriver     string
	QueueDataSource string
	QueueTableName  string
	RequestTimeout  time.Duration
}
