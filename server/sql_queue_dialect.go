package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	gearman "github.com/peonone/gearman"
)

// The sql queue is designed to support multiple RDBMS
// We need an implementation of sqlQueueDialiect for each supported RDBMS
// sqlQueueDialiectSimple is an abstract implementation with ANSI sql
// the concrete implementation can just use the instance of sqlQueueDialiectSimple or nest it

var (
	queueCreateTableTmpl = `
	CREATE TABLE %s
	(
		function VARCHAR(32),
		handle VARCHAR(32), 
		unique_id VARCHAR(32),
		priority SMALLINT,
		data BIT VARYING(64),
		reducer VARCHAR(64),
		PRIMARY KEY (handle)
	);
	CREATE INDEX idx_queue_priority ON %s (priority);
	CREATE INDEX idx_queue_function ON %s (function);
	CREATE INDEX idx_unique_id ON %s (unique_id);
	`

	queueInsertTmpl = `
	INSERT INTO %s 
	(function, handle, unique_id, priority, data, reducer)
	VALUES($1, $2, $3, $4, $5, $6)
	`

	queueCountTmpl = "SELECT COUNT(1) FROM %s"

	queueMaxHandleTmpl = "SELECT MAX(handle) FROM %s"

	queueDeleteTmpl = `
	DELETE FROM %s WHERE handle=$1
	`

	queueAppendClientTmpl = `
	UPDATE %s set client_ids=client_ids || $1 WHERE handle=$2
	`
)

type sqlQueueDialiect interface {
	createQueueTable() error
	insertItem(ctx context.Context, j *job) error
	peekJob(ctx context.Context, functions []string) (*job, error)
	querySize(ctx context.Context) (int, error)
	deleteByHandle(ctx context.Context, handle string) error
}

type sqlQueueDialectParam struct {
	table string
	db    *sql.DB
}

type sqlQueueDialiectSimple struct {
	param *sqlQueueDialectParam
}

func (ds *sqlQueueDialiectSimple) hasQueueTable() bool {
	// TODO need a better way to check if table exists
	query := fmt.Sprintf("SELECT 1 FROM %s", ds.param.table)
	_, err := ds.param.db.Exec(query)
	if err != nil {
		return false
	}
	return true
}

func (ds *sqlQueueDialiectSimple) createQueueTable() error {
	if ds.hasQueueTable() {
		return nil
	}
	query := fmt.Sprintf(queueCreateTableTmpl,
		ds.param.table, ds.param.table, ds.param.table, ds.param.table)
	_, err := ds.param.db.Exec(query)
	return err
}

func (ds *sqlQueueDialiectSimple) peekJob(ctx context.Context, functions []string) (j *job, err error) {
	query := fmt.Sprintf(`SELECT 
		function, handle, unique_id, priority, data, reducer
		FROM %s 
		WHERE function in (?%s)
		order by priority LIMIT 1
		`, ds.param.table, strings.Repeat(",?", len(functions)-1))

	args := make([]interface{}, len(functions))
	for i, f := range functions {
		args[i] = f
	}
	tx, err := ds.param.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var function, handleStr, uniqueID, data, reducer string
	var priority priority

	err = rows.Scan(&function, &handleStr, &uniqueID, &priority, &data, &reducer)
	if err != nil {
		return nil, err
	}

	handle, err := gearman.UnmarshalID(handleStr)
	if err != nil {
		return nil, err
	}

	j = &job{
		function: function,
		data:     data,
		handle:   handle,
		uniqueID: uniqueID,
		priority: priority,
		reducer:  reducer,
	}
	return
}

func (ds *sqlQueueDialiectSimple) insertItem(ctx context.Context, j *job) (err error) {
	tx, err := ds.param.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	query := fmt.Sprintf(queueInsertTmpl, ds.param.table)

	_, err = tx.ExecContext(ctx, query,
		j.function, j.handle.String(), j.uniqueID,
		j.priority, j.data, j.reducer)
	return
}

func (ds *sqlQueueDialiectSimple) querySize(ctx context.Context) (size int, err error) {
	query := fmt.Sprintf(queueCountTmpl, ds.param.table)
	tx, err := ds.param.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	row := tx.QueryRowContext(ctx, query)
	err = row.Scan(&size)
	if err != nil {
		return 0, err
	}
	return
}

func (ds *sqlQueueDialiectSimple) deleteByHandle(ctx context.Context, handle string) (err error) {
	query := fmt.Sprintf(queueDeleteTmpl, ds.param.table)
	tx, err := ds.param.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	_, err = tx.ExecContext(ctx, query, handle)
	return
}

func (ds *sqlQueueDialiectSimple) marshalClientIDs(clientIDs []*gearman.ID) (interface{}, error) {
	clientsLen := len(clientIDs)
	clientsStr := ""
	if clientsLen > 0 {
		clientStrs := make([]string, clientsLen)
		for i, clientID := range clientIDs {
			clientStrs[i] = clientID.String()
		}
		clientsStr = strings.Join(clientStrs, "")
	}
	return clientsStr, nil
}

func (ds *sqlQueueDialiectSimple) unmarshalClientIds(clientsStr string) ([]*gearman.ID, error) {
	if len(clientsStr)%gearman.IDStrLength != 0 {
		return nil, errInvalidClientIDs
	}
	size := len(clientsStr) / gearman.IDStrLength
	clientIDs := make([]*gearman.ID, size)
	for i := 0; i < size; i++ {
		id, err := gearman.UnmarshalID(clientsStr[i*gearman.IDStrLength : (i+1)*gearman.IDStrLength])
		if err != nil {
			return nil, err
		}
		clientIDs[i] = id
	}
	return clientIDs, nil
}
