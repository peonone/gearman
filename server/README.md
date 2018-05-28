## Introduction
An execuable app which implements a subset of the [gearman protocol](http://gearman.org/protocol/)
## Not implemented features
### Requests
- GRAB_JOB_UNIQ
- SET_CLIENT_ID
- ALL_YOURS
- SUBMIT_JOB_SCHED (no plan to add)
- SUBMIT_JOB_EPOCH (no plan to add)
### Administrative Protocol
## Usage

    go get github.com/peonone/gearman
    $GOPATH/bin/gearmand
### command line options

    -bind-addr string
    	Addr the server should listen on. (default ":4730")
    -httptest.serve string
        if non-empty, httptest.NewServer serves on this address and blocks
    -log-file string
        the log file (default "/usr/local/var/log/gearmand.log")
    -log-stderr
        print logs to stderr (default true)
    -queue-type string
        queue type (default "sql")
    -request-timeout duration
        request timeout (default 1s)
    -sql-queue-datasource string
        sql queue datasource (default "gearmand.dat")
    -sql-queue-driver string
        sql queue driver (default "sqlite3")
    -verbose
        enable verbose mode

## Internals
### queue
For now only SQLite3 queue is supported(will add more in future)
