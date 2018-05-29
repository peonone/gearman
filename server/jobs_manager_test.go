package server

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func makeJobsManagerForTest() (*srvJobsManager, *mockQueue) {
	q := &mockQueue{}
	return newjobsManager(testLogger, q, new(Config)), q
}

func addPendingJob(manager *srvJobsManager, pJob *pendingJob) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if pJob.cfg == nil {
		pJob.cfg = new(Config)
	}
	if pJob.logger == nil {
		pJob.logger = testLogger
	}
	manager.pendingJobs[*pJob.handle] = pJob
	manager.pendingJobsUnique[pJob.uniqueID] = pJob
}

func loadPendingJob(manager *srvJobsManager, handle *gearman.ID) *pendingJob {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.pendingJobs[*handle]
}

func getPJobConns(pJob *pendingJob) map[gearman.ID]*conn {
	ch := make(chan map[gearman.ID]*conn)
	pJob.connectionsQueryChan <- ch
	return <-ch
}

func TestSubmitJob(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	j := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	q.On("enqueue", ctx, j).Return(nil).Once()
	manager.submitJob(ctx, j, client1.srvConn)
	job1Conns := loadPendingJob(manager, j.handle).clientConns
	assert.Equal(t, 1, len(job1Conns))
	assert.Contains(t, job1Conns, *client1.ID())
	j2 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo2",
		priority: priorityHigh,
	}
	q.On("enqueue", ctx, j2).Return(nil).Once()
	manager.submitJob(ctx, j2, nil)
	assert.Equal(t, 0, len(loadPendingJob(manager, j2.handle).clientConns))
	q.AssertExpectations(t)
}

func TestSubmitJobCoalescingNotdispatched(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	client2 := newMockSConn(10, 10)
	client3 := newMockSConn(10, 10)
	j1 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	j2 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	j3 := &job{
		function: "wc",
		handle:   testIdGen.Generate(),
		uniqueID: "echo2",
		priority: priorityMid,
	}
	j4 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityMid,
	}

	q.On("enqueue", ctx, mock.Anything).Return(nil).Twice()
	wg := &sync.WaitGroup{}
	for i, pair := range []struct {
		j      *job
		client *conn
	}{{j1, client1.srvConn}, {j2, client2.srvConn}, {j3, client3.srvConn}} {
		if i == 0 {
			manager.submitJob(ctx, pair.j, pair.client)
		} else {
			wg.Add(1)
			go func(i int, j *job, client *conn) {
				handle, err := manager.submitJob(ctx, j, client)
				assert.Nil(t, err)
				if i == 1 {
					assert.Equal(t, j1.handle, handle)
				}
				wg.Done()
			}(i, pair.j, pair.client)
		}
	}
	wg.Wait()
	manager.submitJob(ctx, j4, nil)
	job1Conns := manager.pendingJobsUnique[j1.uniqueID].clientConns
	assert.Equal(t, 2, len(job1Conns))

	assert.Contains(t, job1Conns, *client1.ID())
	assert.Contains(t, job1Conns, *client2.ID())
	q.AssertExpectations(t)
	enqueuedUniqs := make([]string, 0, 2)
	for _, call := range q.Calls {
		enqueuedUniqs = append(enqueuedUniqs, call.Arguments[1].(*job).uniqueID)
	}
	assert.Equal(t, 2, len(enqueuedUniqs))
}

func TestSubmitJobCoalescingdispatched(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	client2 := newMockSConn(10, 10)
	client3 := newMockSConn(10, 10)

	j1 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	j2 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	j3 := &job{
		function: "wc",
		handle:   testIdGen.Generate(),
		uniqueID: "echo2",
		priority: priorityMid,
	}

	q.On("enqueue", ctx, j1).Return(nil).Once()
	manager.submitJob(ctx, j1, client1.srvConn)
	functions := make(map[string]time.Duration)
	functions["echo"] = time.Second * 5
	q.On("dequeue", mock.Anything, mock.Anything).Return(j1, nil).Once()
	grabedJob, err := manager.grabJob(ctx, functions)
	assert.Equal(t, j1, grabedJob)
	assert.Nil(t, err)

	j2Handle, err := manager.submitJob(ctx, j2, client2.srvConn)
	assert.Equal(t, j1.handle, j2Handle)
	q.On("enqueue", ctx, j3).Return(nil).Once()
	manager.submitJob(ctx, j3, client3.srvConn)
	q.AssertExpectations(t)
}

func TestGrabJob(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	client2 := newMockSConn(10, 10)

	j := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	pJob := &pendingJob{
		handle:      j.handle,
		uniqueID:    j.uniqueID,
		clientConns: make(map[gearman.ID]*conn),
	}
	pJob.clientConns[*client1.ID()] = client1.srvConn
	pJob.clientConns[*client2.ID()] = client2.srvConn
	addPendingJob(manager, pJob)

	functions := make(map[string]time.Duration)
	functions["echo"] = time.Second * 5
	functions["wc"] = 0

	q.On("dequeue", mock.Anything).Return(nil, nil).Once()
	grabedJob, err := manager.grabJob(ctx, supportFunctions(functions))
	assert.Nil(t, err)
	assert.Nil(t, grabedJob)

	loadedPJob := loadPendingJob(manager, j.handle)

	assert.False(t, loadedPJob.dispatched)

	q.On("dequeue", mock.Anything).Return(j, nil).Once()

	client2.Close()
	grabedJob, err = manager.grabJob(ctx, supportFunctions(functions))
	assert.Nil(t, err)
	assert.NotNil(t, grabedJob)

	loadedPJob = loadPendingJob(manager, j.handle)
	assert.True(t, loadedPJob.dispatched)
	clientConns := getPJobConns(pJob)
	assert.Equal(t, 1, len(clientConns))
	assert.Contains(t, clientConns, *client1.ID())
	assert.Equal(t, 1, manager.activeRoutineCount())
	q.AssertExpectations(t)
}

func TestJobClientClosed(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	client2 := newMockSConn(10, 10)
	client3 := newMockSConn(10, 10)
	j := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}

	pJob := &pendingJob{
		handle:      j.handle,
		uniqueID:    j.uniqueID,
		clientConns: make(map[gearman.ID]*conn),
	}
	pJob.clientConns[*client1.ID()] = client1.srvConn
	pJob.clientConns[*client2.ID()] = client2.srvConn
	pJob.clientConns[*client3.ID()] = client3.srvConn
	addPendingJob(manager, pJob)

	functions := make(map[string]time.Duration)
	functions["echo"] = time.Second * 5
	functions["wc"] = 0

	q.On("dequeue", mock.Anything).Return(j, nil).Once()
	client1.Close()
	grabedJob, err := manager.grabJob(ctx, supportFunctions(functions))
	assert.Nil(t, err)
	assert.NotNil(t, grabedJob)

	loadedPJob := loadPendingJob(manager, j.handle)
	assert.Equal(t, 1, manager.activeRoutineCount())

	clientConns := getPJobConns(loadedPJob)
	assert.Equal(t, 2, len(clientConns))

	client3.Close()
	time.Sleep(time.Millisecond * 100)
	clientConns = getPJobConns(loadedPJob)
	assert.Equal(t, 1, len(clientConns))
	assert.Contains(t, clientConns, *client2.ID())
	q.AssertExpectations(t)
}

func TestJobTimeout(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	client1 := newMockSConn(10, 10)
	job1 := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}
	job2 := &job{
		function: "wc",
		handle:   testIdGen.Generate(),
		uniqueID: "wc1",
		priority: priorityHigh,
	}

	pJob1 := &pendingJob{
		handle:      job1.handle,
		uniqueID:    job1.uniqueID,
		clientConns: make(map[gearman.ID]*conn),
	}
	pJob1.clientConns[*client1.ID()] = client1.srvConn
	addPendingJob(manager, pJob1)
	pJob2 := &pendingJob{
		handle:      job2.handle,
		uniqueID:    job2.uniqueID,
		clientConns: make(map[gearman.ID]*conn),
	}
	pJob2.clientConns[*client1.ID()] = client1.srvConn
	addPendingJob(manager, pJob2)

	functions := supportFunctions(make(map[string]time.Duration))
	functions["echo"] = time.Millisecond * 100
	functions["wc"] = 0

	q.On("dequeue", mock.Anything).Return(job1, nil).Once()
	grabedJob, err := manager.grabJob(ctx, functions)
	assert.Nil(t, err)
	assert.Equal(t, job1, grabedJob)

	q.On("dequeue", mock.Anything).Return(job2, nil).Once()
	grabedJob, err = manager.grabJob(ctx, functions)
	assert.Nil(t, err)
	assert.Equal(t, job2, grabedJob)

	assert.Equal(t, 2, manager.activeRoutineCount())

	assert.NotNil(t, loadPendingJob(manager, job1.handle))
	assert.NotNil(t, loadPendingJob(manager, job2.handle))
	assert.Equal(t, 2, manager.activeRoutineCount())

	time.Sleep(functions["echo"] + time.Millisecond*10)
	assert.Equal(t, 1, manager.activeRoutineCount())
	manager.mu.Lock()
	assert.Equal(t, 1, len(manager.pendingJobs))
	assert.Equal(t, 1, len(manager.pendingJobsUnique))
	manager.mu.Unlock()
	assert.Nil(t, loadPendingJob(manager, job1.handle))
	assert.NotNil(t, loadPendingJob(manager, job2.handle))
	assert.Equal(t, 1, len(client1.WriteCh))
	msg := <-client1.WriteCh
	assert.Equal(t, gearman.MagicRes, msg.MagicType)
	assert.Equal(t, gearman.WORK_EXCEPTION, msg.PacketType)
	assert.Equal(t, job1.handle.String(), msg.Arguments[0])
	q.AssertExpectations(t)
}

func TestJobStatus(t *testing.T) {
	manager, q := makeJobsManagerForTest()
	ctx := context.Background()
	handle := testIdGen.Generate()
	js := manager.getJobStatus(ctx, handle, "")
	assert.Equal(t, handle, js.handle)
	assert.False(t, js.known)
	assert.False(t, js.running)

	js = manager.getJobStatus(ctx, nil, "123")
	assert.Equal(t, (*gearman.ID)(nil), js.handle)
	assert.False(t, js.known)
	assert.False(t, js.running)

	client1 := newMockSConn(10, 10)
	client2 := newMockSConn(10, 10)
	client3 := newMockSConn(10, 10)
	j := &job{
		function: "echo",
		handle:   testIdGen.Generate(),
		uniqueID: "echo1",
		priority: priorityHigh,
	}

	pJob := &pendingJob{
		handle:      j.handle,
		uniqueID:    j.uniqueID,
		clientConns: make(map[gearman.ID]*conn),
	}
	pJob.clientConns[*client1.ID()] = client1.srvConn
	pJob.clientConns[*client2.ID()] = client2.srvConn
	pJob.clientConns[*client3.ID()] = client3.srvConn
	addPendingJob(manager, pJob)
	js = manager.getJobStatus(ctx, j.handle, "")
	assert.Equal(t, pJob.handle, js.handle)
	assert.True(t, js.known)
	assert.False(t, js.running)
	assert.Equal(t, 3, js.waitingCount)

	client3.Close()
	js = manager.getJobStatus(ctx, j.handle, "")
	assert.Equal(t, 2, js.waitingCount)
	assert.Equal(t, pJob.handle, js.handle)
	functions := make(map[string]time.Duration)
	functions["echo"] = 0

	q.On("dequeue", mock.Anything).Return(j, nil).Once()
	manager.grabJob(ctx, supportFunctions(functions))

	js = manager.getJobStatus(ctx, nil, j.uniqueID)
	assert.True(t, js.known)
	assert.True(t, js.running)
	assert.Zero(t, js.numerator)
	assert.Equal(t, 2, js.waitingCount)
	assert.Equal(t, pJob.handle, js.handle)

	msg := &gearman.Message{
		MagicType:  gearman.MagicReq,
		PacketType: gearman.WORK_STATUS,
		Arguments:  []string{j.handle.String(), "10", "100"},
	}
	assert.True(t, manager.updateJobStatus(ctx, j.handle, msg))
	time.Sleep(time.Millisecond * 50)
	js = manager.getJobStatus(ctx, j.handle, "")
	assert.Equal(t, pJob.handle, js.handle)
	assert.True(t, js.known)
	assert.True(t, js.running)
	assert.Equal(t, 10, js.numerator)
	assert.Equal(t, 100, js.denominator)
	assert.Equal(t, 1, len(client1.WriteCh))
	receivedMsg := <-client1.WriteCh
	assert.Equal(t, msg.PacketType, receivedMsg.PacketType)
	assert.Equal(t, msg.Arguments, receivedMsg.Arguments)
	for _, packet := range []gearman.PacketType{gearman.WORK_DATA, gearman.WORK_WARNING} {
		msg = &gearman.Message{
			MagicType:  gearman.MagicReq,
			PacketType: packet,
			Arguments:  []string{j.handle.String(), "12345"},
		}
		assert.True(t, manager.updateJobStatus(ctx, j.handle, msg))
		js = manager.getJobStatus(ctx, nil, j.uniqueID)
		time.Sleep(time.Millisecond * 5)
		assert.Equal(t, pJob.handle, js.handle)
		assert.True(t, js.known)
		assert.True(t, js.running)
		assert.Equal(t, 1, len(client1.WriteCh))
		receivedMsg = <-client1.WriteCh
		assert.Equal(t, msg.PacketType, receivedMsg.PacketType)
		assert.Equal(t, msg.Arguments, receivedMsg.Arguments)
	}

	client2.Close()
	time.Sleep(time.Millisecond * 10)
	js = manager.getJobStatus(ctx, nil, j.uniqueID)
	assert.Equal(t, 1, js.waitingCount)

	client1.Close()
	time.Sleep(time.Millisecond * 10)
	js = manager.getJobStatus(ctx, nil, j.uniqueID)
	assert.Equal(t, 0, js.waitingCount)
	q.AssertExpectations(t)
}

func TestJobDone(t *testing.T) {
	ctx := context.Background()
	for i, packet := range []gearman.PacketType{gearman.WORK_COMPLETE, gearman.WORK_FAIL, gearman.WORK_EXCEPTION} {
		manager, q := makeJobsManagerForTest()
		client1 := newMockSConn(10, 10)
		client1.srvConn.setForwardException(true)
		client2 := newMockSConn(10, 10)
		j := &job{
			function: "echo",
			handle:   testIdGen.Generate(),
			uniqueID: "echo" + strconv.Itoa(i),
			priority: priorityHigh,
		}

		pJob := &pendingJob{
			handle:      j.handle,
			uniqueID:    j.uniqueID,
			clientConns: make(map[gearman.ID]*conn),
		}
		pJob.clientConns[*client1.ID()] = client1.srvConn
		pJob.clientConns[*client2.ID()] = client2.srvConn
		addPendingJob(manager, pJob)
		functions := make(map[string]time.Duration)
		functions["echo"] = 0

		q.On("dequeue", mock.Anything).Return(j, nil).Once()
		manager.grabJob(ctx, supportFunctions(functions))
		var args []string
		switch packet {
		case gearman.WORK_COMPLETE:
			args = []string{j.handle.String(), ""}
		case gearman.WORK_FAIL:
			args = []string{j.handle.String()}
		case gearman.WORK_EXCEPTION:
			args = []string{j.handle.String(), "exception"}
		}
		msg := &gearman.Message{
			MagicType:  gearman.MagicReq,
			PacketType: packet,
			Arguments:  args,
		}
		assert.True(t, manager.updateJobStatus(ctx, j.handle, msg))
		js := manager.getJobStatus(ctx, nil, j.uniqueID)
		assert.False(t, js.known)

		respMsg := &gearman.Message{
			MagicType:  gearman.MagicRes,
			PacketType: packet,
			Arguments:  args,
		}
		assert.Equal(t, 1, len(client1.WriteCh))
		assert.Equal(t, respMsg, <-client1.WriteCh)

		if packet == gearman.WORK_EXCEPTION {
			respMsg := &gearman.Message{
				MagicType:  gearman.MagicRes,
				PacketType: gearman.WORK_FAIL,
				Arguments:  []string{j.handle.String()},
			}
			assert.Equal(t, 1, len(client2.WriteCh))
			assert.Equal(t, respMsg, <-client2.WriteCh)
		}
	}
}
