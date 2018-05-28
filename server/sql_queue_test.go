package server

import (
	"context"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var unittestDbFile = "unittest.db"

var jobs = []*job{
	&job{
		function: "echo",
		data:     "peonone",
		handle:   testIdGen.Generate(),
		uniqueID: "echoJob1",
		priority: priorityLow,
	},
	&job{
		function: "echo",
		data:     "peonone",
		handle:   testIdGen.Generate(),
		uniqueID: "echoJob2",
		priority: priorityHigh,
	},
	&job{
		function: "reverse",
		data:     "peonone",
		handle:   testIdGen.Generate(),
		uniqueID: "reverseJob1",
		priority: priorityHigh,
	},
	&job{
		function: "reverse",
		data:     "peonone",
		handle:   testIdGen.Generate(),
		uniqueID: "reverseJob2",
		priority: priorityMid,
	},
}

func TestQueueSqlite3(t *testing.T) {
	_, err := os.Stat(unittestDbFile)
	if err == nil {
		err = os.Remove(unittestDbFile)
		assert.Nil(t, err)
	}
	testQueue(t, QueueSqlite3Driver, unittestDbFile, "gearman_queue")
}

func testQueue(t *testing.T, driver string, datasource string, table string) {
	q, err := newSQLQueue(driver, unittestDbFile, "gearman_queue")
	defer func() {
		if q != nil {
			q.dispose()
		}
	}()
	assert.Nil(t, err)
	bgCtx := context.Background()
	size, err := q.size(bgCtx)
	assert.Nil(t, err)
	assert.Equal(t, 0, size)

	for i, job := range jobs {
		err = q.enqueue(bgCtx, job)
		assert.Nil(t, err)

		size, err = q.size(bgCtx)
		assert.Nil(t, err)
		assert.Equal(t, i+1, size)
	}

	job, err := q.dequeue(bgCtx, []string{"nonexist"})
	assert.Nil(t, err)
	assert.Nil(t, job)

	job, err = q.dequeue(bgCtx, []string{"reverse", "hello"})
	assert.Nil(t, err)
	assert.Equal(t, jobs[2], job)
	size, err = q.size(bgCtx)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs)-1, size)

	job, err = q.dequeue(bgCtx, []string{"echo", "reverse"})
	assert.Nil(t, err)
	assert.Equal(t, jobs[1], job)
	size, err = q.size(bgCtx)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs)-2, size)

	job, err = q.dequeue(bgCtx, []string{"echo", "reverse"})
	assert.Nil(t, err)
	assert.Equal(t, jobs[3], job)
	size, err = q.size(bgCtx)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs)-3, size)

	assert.Nil(t, q.dispose())

	q, err = newSQLQueue(driver, unittestDbFile, "gearman_queue")
	assert.Nil(t, err)
	size, err = q.size(bgCtx)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs)-3, size)
}
