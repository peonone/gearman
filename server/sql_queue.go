package server

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// sqlQueue is a queue implementation with RDBMS
// TODO add prefetch mechanism to prevent run sql query for every dequeue call
type sqlQueue struct {
	dialect    sqlQueueDialiect
	driver     string
	dataSource string
	table      string
	db         *sql.DB
	mu         sync.Mutex
}

// QueueSQL is the name of the sql queue
const QueueSQL = "sql"

var errUnsupportedDialiet = errors.New("Unsupported SQL dialect")

func newSQLQueue(driver string, ds string, table string) (*sqlQueue, error) {
	db, err := sql.Open(driver, ds)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dialectParam := &sqlQueueDialectParam{
		table: table,
		db:    db,
	}
	var dialect sqlQueueDialiect
	switch driver {
	case QueueSqlite3Driver:
		dialect = newSqlite3Dialect(dialectParam)
	default:
		return nil, errUnsupportedDialiet
	}

	err = dialect.createQueueTable()
	if err != nil {
		return nil, err
	}
	return &sqlQueue{
		dialect:    dialect,
		driver:     driver,
		dataSource: ds,
		table:      table,
		db:         db,
	}, nil
}

func (q *sqlQueue) enqueue(ctx context.Context, j *job) error {
	return q.dialect.insertItem(ctx, j)
}

func (q *sqlQueue) size(ctx context.Context) (int, error) {
	return q.dialect.querySize(ctx)
}

func (q *sqlQueue) dequeue(ctx context.Context, functions []string) (j *job, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, err = q.dialect.peekJob(ctx, functions)
	if err != nil {
		return nil, err
	}
	if j != nil {
		err = q.dialect.deleteByhandle(ctx, j.handle.String())
		if err != nil {
			return nil, err
		}
	}
	return
}

func (q *sqlQueue) dispose() error {
	return q.db.Close()
}
