package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peonone/gearman"
	"github.com/stretchr/testify/mock"
)

// jobs manager manages all jobs,
//such as submit, grab a job, and query or update the job status

type jobStatus struct {
	known        bool
	running      bool
	numerator    int
	denominator  int
	handle       *gearman.ID
	waitingCount int
}

type jobsManager interface {
	submitJob(ctx context.Context, j *job, clientConn *conn) (*gearman.ID, error)
	grabJob(ctx context.Context, functions supportFunctions) (*job, error)
	getJobStatus(ctx context.Context, handle *gearman.ID, uniqueID string) *jobStatus
	updateJobStatus(ctx context.Context, handle *gearman.ID, msg *gearman.Message) bool
}

var _ jobsManager = &srvJobsManager{}
var _ jobsManager = &mockJobsManager{}

type srvJobsManager struct {
	mu                sync.Mutex
	wg                sync.WaitGroup
	q                 queue
	pendingJobs       map[gearman.ID]*pendingJob
	pendingJobsUnique map[string]*pendingJob
	logger            *log.Logger
	cfg               *Config
	activeRoutineCnt  *int32
}

var errJobNotFound = errors.New("Job not found")

func newjobsManager(logger *log.Logger, q queue, cfg *Config) *srvJobsManager {
	var cnt int32
	return &srvJobsManager{
		q:                 q,
		pendingJobs:       make(map[gearman.ID]*pendingJob),
		pendingJobsUnique: make(map[string]*pendingJob),
		activeRoutineCnt:  &cnt,
		logger:            logger,
		cfg:               cfg,
	}
}

func (m *srvJobsManager) submitJob(ctx context.Context, j *job, clientConn *conn) (*gearman.ID, error) {
	m.mu.Lock()
	pJob, hitByUniq := m.pendingJobsUnique[j.uniqueID]
	dispatched := hitByUniq && pJob.dispatched
	if dispatched && clientConn != nil {
		newConnCtx, _ := context.WithTimeout(ctx, time.Millisecond*100)
		registered, err := pJob.registerNewConn(newConnCtx, clientConn)
		if err != nil {
			return nil, err
		}
		if !registered {
			// failed to register conn to a running job
			// then start a new one
			hitByUniq = false
			dispatched = false
		}
	}
	if !hitByUniq {
		pJob = &pendingJob{
			handle:      j.handle,
			uniqueID:    j.uniqueID,
			clientConns: make(map[gearman.ID]*conn),
			logger:      m.logger,
			cfg:         m.cfg,
		}
	}
	m.pendingJobs[*pJob.handle] = pJob
	m.pendingJobsUnique[j.uniqueID] = pJob
	if !dispatched && clientConn != nil {
		pJob.clientConns[*clientConn.ID()] = clientConn
	}
	m.mu.Unlock()
	if !hitByUniq {
		return j.handle, m.q.enqueue(ctx, j)
	}

	return pJob.handle, nil
}

func (m *srvJobsManager) grabJob(ctx context.Context, functions supportFunctions) (*job, error) {
	j, err := m.q.dequeue(ctx, functions.toSlice())
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	pj, ok := m.pendingJobs[*j.handle]
	if !ok {
		return nil, errJobNotFound
	}
	timeout := functions.timeout(j.function)

	pj.dispatched = true
	pj.newConnChan = make(chan *newConnReq)
	pj.statusUpdateChan = make(chan *statusUpdateReq)
	pj.statusQueryChan = make(chan chan *jobStatus)
	pj.connectionsQueryChan = make(chan chan map[gearman.ID]*conn)
	pj.done = make(chan struct{})

	for id, conn := range pj.clientConns {
		select {
		case <-conn.Closed():
			delete(pj.clientConns, id)
		default:
		}
	}

	atomic.AddInt32(m.activeRoutineCnt, 1)
	go pj.run(timeout, m)
	return j, nil
}

func (m *srvJobsManager) getJobStatus(ctx context.Context, handle *gearman.ID, uniqueID string) (ret *jobStatus) {
	m.mu.Lock()
	var pJob *pendingJob
	var ok bool
	var dispacthed bool
	if handle != nil {
		pJob, ok = m.pendingJobs[*handle]
	} else {
		pJob, ok = m.pendingJobsUnique[uniqueID]
	}
	if ok {
		dispacthed = pJob.dispatched
	}
	var replyCh chan *jobStatus
	var waitingCount int
	if dispacthed {
		replyCh = make(chan *jobStatus)
		select {
		case <-pJob.done:
			ok = false
		case pJob.statusQueryChan <- replyCh:
		}
	} else if ok {
		for id, conn := range pJob.clientConns {
			select {
			case <-conn.Closed():
				delete(pJob.clientConns, id)
			default:
			}
		}
		waitingCount = len(pJob.clientConns)
	}
	m.mu.Unlock()
	if !ok {
		return &jobStatus{known: false, handle: handle}
	}
	if !dispacthed {
		return &jobStatus{known: true, running: false, waitingCount: waitingCount, handle: handle}
	}

	select {
	case result := <-replyCh:
		return result
	case <-ctx.Done():
		return &jobStatus{known: false, handle: handle}
	}
}

func (m *srvJobsManager) updateJobStatus(ctx context.Context, handle *gearman.ID, msg *gearman.Message) (succeed bool) {
	m.mu.Lock()
	pJob, ok := m.pendingJobs[*handle]
	defer m.mu.Unlock()
	if !ok {
		return false
	}

	req := &statusUpdateReq{
		handle: handle,
		msg:    msg,
	}
	select {
	case pJob.statusUpdateChan <- req:
		succeed = true
	case <-pJob.done:
		succeed = false
	case <-ctx.Done():
		succeed = false
	}
	return
}

func (m *srvJobsManager) removeJob(handle *gearman.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	pJob, ok := m.pendingJobs[*handle]
	if !ok {
		return false
	}
	delete(m.pendingJobs, *handle)
	delete(m.pendingJobsUnique, pJob.uniqueID)
	return true
}

func (m *srvJobsManager) jobRoutineDone() {
	atomic.AddInt32(m.activeRoutineCnt, -1)
}

func (m *srvJobsManager) activeRoutineCount() int {
	return int(atomic.LoadInt32(m.activeRoutineCnt))
}

type mockJobsManager struct {
	mock.Mock
}

func (m *mockJobsManager) submitJob(ctx context.Context, j *job, clientConn *conn) (*gearman.ID, error) {
	returnVals := m.Called(ctx, j, clientConn)
	var handle *gearman.ID
	if returnVals.Get(0) != "by-param" {
		handle = j.handle
	} else if returnVals.Get(0) != nil {
		handle = returnVals.Get(0).(*gearman.ID)
	}
	return handle, returnVals.Error(1)
}

func (m *mockJobsManager) grabJob(ctx context.Context, functions supportFunctions) (*job, error) {
	returnVals := m.Called(ctx, functions)
	var j *job
	if returnVals.Get(0) != nil {
		j = returnVals.Get(0).(*job)
	}
	return j, returnVals.Error(1)
}

func (m *mockJobsManager) getJobStatus(ctx context.Context, handle *gearman.ID, uniqueID string) *jobStatus {
	returnVals := m.Called(ctx, handle, uniqueID)
	return returnVals.Get(0).(*jobStatus)
}

func (m *mockJobsManager) updateJobStatus(ctx context.Context, handle *gearman.ID, msg *gearman.Message) (succeed bool) {
	returnVals := m.Called(ctx, handle, msg)
	return returnVals.Bool(0)
}
