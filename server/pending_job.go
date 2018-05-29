package server

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	gearman "github.com/peonone/gearman"
)

// pendingJob represents a pending job managed by the server
// a pending job has two states: dispatched(to a worker) and un-dispatched
// for dispatched pending job, we have a dedicate goroutine for it
// the goroutine handles the status update, status query, complete, listening client disconnect and timeout

const jobTimeoutErrMsg = "Job execution timeout"

const (
	selectIdxStatusUpdate = iota
	selectIdxTimeout
	selectIdxNewConn
	selectIdxQueryStatus
	selectIdxGetConnections

	staticSelectCnt
)

type newConnReq struct {
	conn  *conn
	reply chan bool
}

type statusUpdateReq struct {
	handle *gearman.ID
	msg    *gearman.Message
}

type pendingJob struct {
	newConnChan          chan *newConnReq
	statusUpdateChan     chan *statusUpdateReq
	statusQueryChan      chan chan *jobStatus
	connectionsQueryChan chan chan map[gearman.ID]*conn
	done                 chan struct{}
	handle               *gearman.ID
	uniqueID             string
	clientConns          map[gearman.ID]*conn
	prgNumerator         int
	prgDenominator       int
	timeouted            bool
	completed            bool
	dispatched           bool
	logger               *log.Logger
	cfg                  *Config
}

func (j *pendingJob) sendToListenClients(msg *gearman.Message) error {
	binData, err := msg.Encode()
	if err != nil {
		return err
	}
	for _, conn := range j.clientConns {
		writeErr := conn.WriteBin(binData)
		if writeErr != nil {
			err = writeErr
		}
	}
	return err
}

func (j *pendingJob) handleStatusUpdate(req *statusUpdateReq) bool {
	completed := false
	switch req.msg.PacketType {
	case gearman.WORK_COMPLETE, gearman.WORK_FAIL, gearman.WORK_EXCEPTION:
		completed = true
	case gearman.WORK_STATUS:
		num, numErr := strconv.Atoi(req.msg.Arguments[1])
		den, denErr := strconv.Atoi(req.msg.Arguments[2])
		if numErr == nil && denErr == nil {
			j.prgNumerator = num
			j.prgDenominator = den
		}
	}
	req.msg.MagicType = gearman.MagicRes
	if req.msg.PacketType == gearman.WORK_EXCEPTION {
		var msgBin, excBin, failBin []byte
		var err error

		for _, conn := range j.clientConns {
			if conn.forwardException() {
				if excBin == nil {
					excBin, err = req.msg.Encode()
					if err != nil {
						j.logger.Printf("encode msg %s failed", req.msg)
						continue
					}
				}
				msgBin = excBin
			} else {
				if failBin == nil {
					failMsg := &gearman.Message{
						MagicType:  gearman.MagicRes,
						PacketType: gearman.WORK_FAIL,
						Arguments:  []string{j.handle.String()},
					}
					failBin, err = failMsg.Encode()
					if err != nil {
						j.logger.Printf("encode msg %s failed", req.msg)
						continue
					}
					msgBin = failBin
				}
			}
			conn.WriteBin(msgBin)
		}
	} else {
		j.sendToListenClients(req.msg)
	}

	return completed
}

func (j *pendingJob) String() string {
	return fmt.Sprintf("%s-%s", j.handle, j.uniqueID)
}

func (j *pendingJob) run(timeout time.Duration, manager *srvJobsManager) {
	if j.cfg.Verbose {
		j.logger.Printf("job %s started", j)
	}
	defer func() {
		// remove the job from jobs manager
		if j.cfg.Verbose {
			j.logger.Printf("job %s done", j)
		}
		close(j.done)
		manager.removeJob(j.handle)
		close(j.newConnChan)
		close(j.statusUpdateChan)
		close(j.statusQueryChan)
		close(j.connectionsQueryChan)
		manager.jobRoutineDone()
	}()
	var timeoutTimer *time.Timer
	var emptyChan chan struct{}

	if timeout > 0 {
		timeoutTimer = time.NewTimer(timeout)
	} else {
		emptyChan = make(chan struct{})
	}

LOOP:
	for {
		cases := make([]reflect.SelectCase, len(j.clientConns)+staticSelectCnt)
		casesConnMap := make(map[int]gearman.Conn)

		cases[selectIdxStatusUpdate] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(j.statusUpdateChan),
		}
		if timeoutTimer != nil {
			cases[selectIdxTimeout] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(timeoutTimer.C),
			}
		} else {
			cases[selectIdxTimeout] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(emptyChan),
			}
		}

		cases[selectIdxNewConn] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(j.newConnChan),
		}
		cases[selectIdxQueryStatus] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(j.statusQueryChan),
		}
		cases[selectIdxGetConnections] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(j.connectionsQueryChan),
		}

		i := staticSelectCnt
		for _, conn := range j.clientConns {
			closedChan := conn.Closed()
			casesConnMap[i] = conn
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(closedChan)}
			i++
		}
		chosen, value, _ := reflect.Select(cases)
		switch chosen {
		case selectIdxStatusUpdate:
			if j.handleStatusUpdate(value.Interface().(*statusUpdateReq)) {
				// job completed
				if timeoutTimer != nil {
					timeoutTimer.Stop()
				}
				break LOOP
			}
		case selectIdxTimeout:
			// job timeout
			if j.cfg.Verbose {
				j.logger.Printf("job %s timeouted", j)
			}
			j.timeouted = true
			if len(j.clientConns) > 0 {
				msg := &gearman.Message{
					MagicType:  gearman.MagicRes,
					PacketType: gearman.WORK_EXCEPTION,
					Arguments:  []string{j.handle.String(), jobTimeoutErrMsg},
				}
				j.sendToListenClients(msg)
			}
			break LOOP
		case selectIdxNewConn:
			connReq := value.Interface().(*newConnReq)
			j.clientConns[*connReq.conn.ID()] = connReq.conn
			connReq.reply <- true
		case selectIdxQueryStatus:
			replyCh := value.Interface().(chan *jobStatus)
			replyCh <- &jobStatus{
				handle:       j.handle,
				known:        true,
				running:      true,
				numerator:    j.prgNumerator,
				denominator:  j.prgDenominator,
				waitingCount: len(j.clientConns),
			}
		case selectIdxGetConnections:
			replyCh := value.Interface().(chan map[gearman.ID]*conn)
			connsCopy := make(map[gearman.ID]*conn)

			for k, v := range j.clientConns {
				connsCopy[k] = v
			}
			replyCh <- connsCopy
		default:
			closedConn := casesConnMap[chosen]
			delete(j.clientConns, *closedConn.ID())
		}
	}
}

func (j *pendingJob) registerNewConn(ctx context.Context, clientConn *conn) (bool, error) {
	replyCh := make(chan bool)
	select {
	case <-j.done:
		return false, nil
	case j.newConnChan <- &newConnReq{clientConn, replyCh}:
	}

	select {
	case <-replyCh:
		return true, nil
	case <-j.done:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
