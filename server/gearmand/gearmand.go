package main

import (
	"flag"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/peonone/gearman/server"
)

var bindAddr = flag.String("bind-addr", ":4730", "Addr the server should listen on.")
var logFile = flag.String("log-file", "/usr/local/var/log/gearmand.log", "the log file")
var logToStdErr = flag.Bool("log-stderr", true, "print logs to stderr")
var verbose = flag.Bool("verbose", false, "enable verbose mode")
var queueType = flag.String("queue-type", server.QueueSQL, "queue type")
var sqlQueueDriver = flag.String("sql-queue-driver", server.QueueSqlite3Driver, "sql queue driver")
var sqlQueueDataSource = flag.String("sql-queue-datasource", "gearmand.dat", "sql queue datasource")
var requestTimeout = flag.Duration("request-timeout", time.Second*1, "request timeout")

func main() {
	flag.Parse()
	cfg := &server.Config{
		BindAddr:        *bindAddr,
		LogFilePath:     *logFile,
		LogToStderr:     *logToStdErr,
		Verbose:         *verbose,
		QueueType:       *queueType,
		QueueDriver:     *sqlQueueDriver,
		QueueTableName:  "queue",
		QueueDataSource: *sqlQueueDataSource,
		RequestTimeout:  *requestTimeout,
	}
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Printf("failed to initialize server: %s", err)
		return
	}
	srv.Run()
}
