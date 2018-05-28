package gearman

// MagicType is the type of the magic code
type MagicType byte

const (
	_ MagicType = iota
	// MagicReq is for REQ magic type
	MagicReq
	// MagicRes is for RES magic type
	MagicRes
)

const (
	// MagicReqValue holds the magic code text value of REQ
	MagicReqValue = "\000REQ"
	// MagicResValue holds the magic code text value of RES
	MagicResValue = "\000RES"
)

func (magicType MagicType) String() string {
	switch magicType {
	case MagicReq:
		return MagicReqValue
	case MagicRes:
		return MagicResValue
	default:
		return "\000UNK"
	}
}

// Valid checks if the magic type is valid
func (magicType MagicType) Valid() bool {
	return magicType == MagicReq || magicType == MagicRes
}

// RoleType is the type of role(worker/client/server)
type RoleType byte

const (
	// RoleWorker is the role of worker
	RoleWorker RoleType = 1 << iota
	// RoleClient is the role of client
	RoleClient
	// RoleServer is the role of server
	RoleServer
	// RoleWorkerAndClient presents role worker and client
	RoleWorkerAndClient = RoleWorker | RoleClient
)

func (r RoleType) hasType(r1 RoleType) bool {
	return r&r1 == r1
}

const (
	_ PacketType = iota
	CAN_DO
	CANT_DO
	RESET_ABILITIES
	PRE_SLEEP
	_
	NOOP
	SUBMIT_JOB
	JOB_CREATED
	GRAB_JOB
	NO_JOB
	JOB_ASSIGN
	WORK_STATUS
	WORK_COMPLETE
	WORK_FAIL
	GET_STATUS
	ECHO_REQ
	ECHO_RES
	SUBMIT_JOB_BG
	ERROR
	STATUS_RES
	SUBMIT_JOB_HIGH
	SET_CLIENT_ID
	CAN_DO_TIMEOUT
	ALL_YOURS
	WORK_EXCEPTION
	OPTION_REQ
	OPTION_RES
	WORK_DATA
	WORK_WARNING
	GRAB_JOB_UNIQ
	JOB_ASSIGN_UNIQ
	SUBMIT_JOB_HIGH_BG
	SUBMIT_JOB_LOW
	SUBMIT_JOB_LOW_BG
	SUBMIT_JOB_SCHED
	SUBMIT_JOB_EPOCH
	SUBMIT_REDUCE_JOB
	SUBMIT_REDUCE_JOB_BACKGROUND
	GRAB_JOB_ALL
	JOB_ASSIGN_ALL
	GET_STATUS_UNIQUE
	STATUS_RES_UNIQUE
)

var packetTypeNames = map[PacketType]string{
	CAN_DO:                       "CAN_DO",
	CANT_DO:                      "CANT_DO",
	RESET_ABILITIES:              "RESET_ABILITIES",
	PRE_SLEEP:                    "PRE_SLEEP",
	NOOP:                         "NOOP",
	SUBMIT_JOB:                   "SUBMIT_JOB",
	JOB_CREATED:                  "JOB_CREATED",
	GRAB_JOB:                     "GRAB_JOB",
	NO_JOB:                       "NO_JOB",
	JOB_ASSIGN:                   "JOB_ASSIGN",
	WORK_STATUS:                  "WORK_STATUS",
	WORK_COMPLETE:                "WORK_COMPLETE",
	WORK_FAIL:                    "WORK_FAIL",
	GET_STATUS:                   "GET_STATUS",
	ECHO_REQ:                     "ECHO_REQ",
	ECHO_RES:                     "ECHO_RES",
	SUBMIT_JOB_BG:                "SUBMIT_JOB_BG",
	ERROR:                        "ERROR",
	STATUS_RES:                   "STATUS_RES",
	SUBMIT_JOB_HIGH:              "SUBMIT_JOB_HIGH",
	SET_CLIENT_ID:                "SET_CLIENT_ID",
	CAN_DO_TIMEOUT:               "CAN_DO_TIMEOUT",
	ALL_YOURS:                    "ALL_YOURS",
	WORK_EXCEPTION:               "WORK_EXCEPTION",
	OPTION_REQ:                   "OPTION_REQ",
	OPTION_RES:                   "OPTION_RES",
	WORK_DATA:                    "WORK_DATA",
	WORK_WARNING:                 "WORK_WARNING",
	GRAB_JOB_UNIQ:                "GRAB_JOB_UNIQ",
	JOB_ASSIGN_UNIQ:              "JOB_ASSIGN_UNIQ",
	SUBMIT_JOB_HIGH_BG:           "SUBMIT_JOB_HIGH_BG",
	SUBMIT_JOB_LOW:               "SUBMIT_JOB_LOW",
	SUBMIT_JOB_LOW_BG:            "SUBMIT_JOB_LOW_BG",
	SUBMIT_JOB_SCHED:             "SUBMIT_JOB_SCHED",
	SUBMIT_JOB_EPOCH:             "SUBMIT_JOB_EPOCH",
	SUBMIT_REDUCE_JOB:            "SUBMIT_REDUCE_JOB",
	SUBMIT_REDUCE_JOB_BACKGROUND: "SUBMIT_REDUCE_JOB_BACKGROUND",
	GRAB_JOB_ALL:                 "GRAB_JOB_ALL",
	JOB_ASSIGN_ALL:               "JOB_ASSIGN_ALL",
	GET_STATUS_UNIQUE:            "GET_STATUS_UNIQUE",
	STATUS_RES_UNIQUE:            "STATUS_RES_UNIQUE",
}

// Valid checks if the packet type is valid
func (packetType PacketType) Valid() bool {
	return packetType >= PacketTypeMin && packetType <= PacketTypeMax
}

func (packetType PacketType) String() string {
	name, ok := packetTypeNames[packetType]
	if !ok {
		return "UNKNOWN"
	}
	return name
}

// msgType is the combination of magic code and packet type
type msgType uint16

// msgAllowedRoles holds valid role types for each msg type
var msgAllowedRoles = make(map[msgType]RoleType)

var msgArgsLens map[PacketType]int

func calcMsgType(magicType MagicType, packetType PacketType) msgType {
	return msgType(magicType)<<8 + msgType(packetType)
}

func putAllowedRoles(magicType MagicType, packetType PacketType, roles RoleType) {
	msgType := calcMsgType(magicType, packetType)
	msgAllowedRoles[msgType] = roles
}

func init() {
	putAllowedRoles(MagicReq, CAN_DO, RoleWorker)
	putAllowedRoles(MagicReq, CANT_DO, RoleWorker)
	putAllowedRoles(MagicReq, RESET_ABILITIES, RoleWorker)
	putAllowedRoles(MagicReq, PRE_SLEEP, RoleWorker)
	putAllowedRoles(MagicRes, NOOP, RoleWorker)
	putAllowedRoles(MagicReq, SUBMIT_JOB, RoleClient)
	putAllowedRoles(MagicRes, JOB_CREATED, RoleClient)
	putAllowedRoles(MagicReq, GRAB_JOB, RoleWorker)
	putAllowedRoles(MagicRes, NO_JOB, RoleWorker)
	putAllowedRoles(MagicRes, JOB_ASSIGN, RoleWorker)
	putAllowedRoles(MagicReq, WORK_STATUS, RoleWorker)
	putAllowedRoles(MagicRes, WORK_STATUS, RoleClient)
	putAllowedRoles(MagicReq, WORK_COMPLETE, RoleWorker)
	putAllowedRoles(MagicRes, WORK_COMPLETE, RoleClient)
	putAllowedRoles(MagicReq, WORK_FAIL, RoleWorker)
	putAllowedRoles(MagicRes, WORK_FAIL, RoleClient)
	putAllowedRoles(MagicReq, GET_STATUS, RoleClient)
	putAllowedRoles(MagicReq, ECHO_REQ, RoleWorkerAndClient)
	putAllowedRoles(MagicRes, ECHO_RES, RoleWorkerAndClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_BG, RoleClient)
	putAllowedRoles(MagicRes, ERROR, RoleWorkerAndClient)
	putAllowedRoles(MagicRes, STATUS_RES, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_HIGH, RoleClient)
	putAllowedRoles(MagicReq, SET_CLIENT_ID, RoleWorker)
	putAllowedRoles(MagicReq, CAN_DO_TIMEOUT, RoleWorker)
	putAllowedRoles(MagicReq, ALL_YOURS, RoleWorker)
	putAllowedRoles(MagicReq, WORK_EXCEPTION, RoleWorker)
	putAllowedRoles(MagicRes, WORK_EXCEPTION, RoleClient)
	putAllowedRoles(MagicReq, OPTION_REQ, RoleWorkerAndClient)
	putAllowedRoles(MagicRes, OPTION_RES, RoleWorkerAndClient)
	putAllowedRoles(MagicReq, WORK_DATA, RoleWorker)
	putAllowedRoles(MagicRes, WORK_DATA, RoleClient)
	putAllowedRoles(MagicReq, WORK_WARNING, RoleWorker)
	putAllowedRoles(MagicRes, WORK_WARNING, RoleClient)
	putAllowedRoles(MagicReq, GRAB_JOB_UNIQ, RoleWorker)
	putAllowedRoles(MagicReq, JOB_ASSIGN_UNIQ, RoleWorker)
	putAllowedRoles(MagicReq, SUBMIT_JOB_HIGH_BG, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_LOW, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_LOW_BG, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_SCHED, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_JOB_EPOCH, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_REDUCE_JOB, RoleClient)
	putAllowedRoles(MagicReq, SUBMIT_REDUCE_JOB_BACKGROUND, RoleClient)
	putAllowedRoles(MagicReq, GRAB_JOB_ALL, RoleWorker)
	putAllowedRoles(MagicRes, JOB_ASSIGN_ALL, RoleWorker)
	putAllowedRoles(MagicReq, GET_STATUS_UNIQUE, RoleClient)
	putAllowedRoles(MagicRes, STATUS_RES_UNIQUE, RoleClient)

	msgArgsLens = map[PacketType]int{
		SUBMIT_JOB:                   3,
		SUBMIT_JOB_BG:                3,
		SUBMIT_JOB_HIGH:              3,
		SUBMIT_JOB_HIGH_BG:           3,
		SUBMIT_JOB_LOW:               3,
		SUBMIT_JOB_LOW_BG:            3,
		SUBMIT_REDUCE_JOB:            4,
		SUBMIT_REDUCE_JOB_BACKGROUND: 4,
		SUBMIT_JOB_SCHED:             8,
		SUBMIT_JOB_EPOCH:             4,
		GET_STATUS:                   1,
		GET_STATUS_UNIQUE:            1,
		OPTION_REQ:                   1,
		JOB_CREATED:                  1,
		WORK_DATA:                    2,
		WORK_WARNING:                 2,
		WORK_STATUS:                  3,
		WORK_COMPLETE:                2,
		WORK_FAIL:                    1,
		WORK_EXCEPTION:               2,
		STATUS_RES:                   5,
		STATUS_RES_UNIQUE:            6,
		OPTION_RES:                   1,
		CAN_DO:                       1,
		CAN_DO_TIMEOUT:               2,
		CANT_DO:                      1,
		RESET_ABILITIES:              0,
		PRE_SLEEP:                    0,
		GRAB_JOB:                     0,
		GRAB_JOB_UNIQ:                0,
		GRAB_JOB_ALL:                 0,
		SET_CLIENT_ID:                1,
		ALL_YOURS:                    0,
	}
}
