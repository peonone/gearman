package server

const QueueSqlite3Driver = "sqlite3"

func newSqlite3Dialect(param *sqlQueueDialectParam) sqlQueueDialiect {
	return &sqlQueueDialiectSimple{param}
}
