package server

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"reflect"

	"github.com/peonone/gearman"
)

var errUnknownQueueType = errors.New("Unknown queue type")

// Server represents a gearman server instance
type Server struct {
	cfg                     *Config
	logger                  *log.Logger
	queue                   queue
	jobHandleGenerator      *gearman.IDGenerator
	clientIDGenerator       *gearman.IDGenerator
	logf                    *os.File
	handlersMng             *gearman.MessageHandlerManager
	supportFunctionsManager *supportFunctionsManager
	jobsManager             jobsManager
	connManager             *gearman.ConnManager
	sleepManager            *sleepManager
}

func (s *Server) initHandlerManager() {
	s.handlersMng = gearman.NewHandlerManager(gearman.RoleServer, s.cfg.RequestTimeout)
	echoHandler := &echoHandler{}
	submitJobHandler := &submitJobHandler{
		s.jobHandleGenerator,
		s.sleepManager,
		s.supportFunctionsManager,
		s.jobsManager,
		s.connManager,
	}
	canDoHandler := &canDoHandler{s.supportFunctionsManager}
	grabJobHandler := &grabJobHandler{s.supportFunctionsManager, s.jobsManager}
	workStatusHandler := &workStatusHandler{s.jobsManager, s.connManager}
	sleepHandler := &sleepHandler{s.sleepManager}
	optionHandler := &optionHandler{}

	handlers := []gearman.MessageHandler{
		echoHandler,
		submitJobHandler,
		canDoHandler,
		grabJobHandler,
		workStatusHandler,
		sleepHandler,
		optionHandler,
	}

	registeredTypes := make(map[gearman.PacketType]gearman.MessageHandler)
	for _, h := range handlers {
		for _, pType := range h.SupportPacketTypes() {
			s.handlersMng.RegisterHandler(pType, h)
			existingH, ok := registeredTypes[pType]
			if ok && existingH != h {
				s.logger.Printf("warn: registering packet duplicately: %s: %s and %s",
					pType, reflect.TypeOf(existingH), reflect.TypeOf(h))
			}
			registeredTypes[pType] = h
		}
	}
}

func NewServer(cfg *Config) (*Server, error) {
	f, err := os.OpenFile(cfg.LogFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("failed to open log file: %s", err)
		return nil, err
	}
	var logWriter io.Writer = f
	if cfg.LogToStderr {
		logWriter = io.MultiWriter(os.Stderr, f)
	}
	logger := log.New(logWriter, "", log.Ltime|log.LstdFlags)
	var queue queue
	switch cfg.QueueType {
	case QueueSQL:
		queue, err = newSQLQueue(cfg.QueueDriver, cfg.QueueDataSource, cfg.QueueTableName)
	default:
		err = errUnknownQueueType
	}

	if err != nil {
		return nil, err
	}

	connManager := gearman.NewConnManager()
	s := &Server{
		cfg:                     cfg,
		logger:                  logger,
		logf:                    f,
		queue:                   queue,
		jobHandleGenerator:      gearman.NewIDGenerator(),
		clientIDGenerator:       gearman.NewIDGenerator(),
		supportFunctionsManager: newSupportFunctionsManager(),
		jobsManager:             newjobsManager(logger, queue, cfg),
		connManager:             connManager,
		sleepManager:            newSleepManager(),
	}
	s.initHandlerManager()
	return s, nil
}

// Run runs the gearman server
func (s *Server) Run() error {
	defer func() {
		if s.logf != nil {
			s.logf.Close()
		}
	}()
	listener, err := net.Listen("tcp", s.cfg.BindAddr)
	if err != nil {
		s.logger.Printf("failed to listen server connection:%s", err)
		return err
	}
	defer func() {
		listener.Close()
	}()
	for {
		netConn, err := listener.Accept()
		if err != nil {
			return err
		}
		conn := gearman.NewNetConn(netConn, s.clientIDGenerator.Generate())
		go s.serve(conn)
	}
}

func (s *Server) serve(conn gearman.Conn) {
	defer func() {
		s.connManager.RemoveConn(conn.ID())
		conn.Close()
	}()
	if s.cfg.Verbose {
		s.logger.Printf("established with client: %s", conn)
	}
	s.connManager.AddConn(conn)
	for {
		msg, err := conn.ReadMsg()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if s.cfg.Verbose {
				s.logger.Printf("client closed: %s", conn)
			}
			break
		} else if err != nil {
			s.logger.Printf("read packet failed from %s: %s", conn, err)
			continue
		}
		_, err = s.handlersMng.HandleMessage(msg, conn)
		if err != nil {
			s.logger.Printf("failed to process message %s for %s: %s", msg, conn, err)
			if serverErr, ok := err.(*serverError); ok {
				errMsg := &gearman.Message{
					MagicType:  gearman.MagicRes,
					PacketType: gearman.ERROR,
					Arguments:  serverErr.toArguments(),
				}
				conn.WriteMsg(errMsg)
			}
		} else if s.cfg.Verbose {
			s.logger.Printf("processed message %s for %s", msg, conn)
		}
	}
}
