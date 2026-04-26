package genericsmrproto

import (
	"state"
)

const (
	PROPOSE uint8 = iota
	PROPOSE_WITH_EXEC_TIME
	PROPOSE_REPLY
	READ
	READ_REPLY
	PROPOSE_AND_READ
	PROPOSE_AND_READ_REPLY
	GENERIC_SMR_BEACON
	GENERIC_SMR_BEACON_REPLY
	REGISTER_CLIENT_ID
	REGISTER_CLIENT_ID_REPLY
	GET_VIEW
	GET_VIEW_REPLY
	CLIENT_RTT_TABLE
	CLIENT_PING_REPLY
	// LATENCY
	REAL_COMMITTED
	MOCK_COMMITTED
	COMMIT_LATENCY_FEEDBACK
	REAL_COMMIT_AT_LEAST
	GHOST_COMMIT_AT_LEAST
)

type ClientRttTable struct {
	ClientId uint32
	Rtts     []int64
}

type Propose struct {
	CommandId int32
	Command   state.Command
	Timestamp int64 // never read, I'm hijacking this
}

type EndToEndLatency_ struct {
	Latency   int64
	CommandId int32
}

type MockExecTime_ struct {
	ExecTime  int64
	DoMock    bool
	CommandId state.CommandId
}

type ProposeWithExecTime struct {
	CommandId       int32
	Command         state.Command
	EndToEndLatency EndToEndLatency_
}

type ProposeReply struct {
	OK uint8
	// ClientId  uint32
	CommandId int32
}

type ProposeReplyTS struct {
	OK uint8
	// ClientId  uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type ProposeReplyTSMock struct {
	OK           uint8
	CommandId    int32
	Value        state.Value
	Timestamp    int64
	MockInstruct bool
}

// type CommandId struct {
// 	ClientId uint32
// 	OpId     int32
// }

type RealCommitted struct {
	Instance int32
	OpId     int32
	Commands []state.CommandAvi
}

// mr99rsm messages
type RealCommitAtLeast struct {
	ClientId  uint32
	CommandId int32 // TODO change to OpId...
	Timestamp int64
}

type CommittedFromClient struct {
	Instance  int32
	ClientId  uint32
	OpId      int32
	Timestamp int64
	Commands  []state.Command // unused
}

// don't need CommandIds for Mock since it isn't forwarding
type MockCommitted struct {
	Instance int32
	OpId     int32
	Commands []state.CommandAvi
}

type CommitLatencyFeedback struct {
	CommandId          state.CommandId
	RealInstance       int32
	GhostInstance      int32
	RealCommitLatency  int64
	GhostCommitLatency int64
	RealInstCmds       []state.CommandAvi
	GhostInstCmds      []state.CommandAvi
}

// type MockCommittedFromClient struct {
// 	Instance   int32
// 	ClientId   uint32
// 	OpId       int32
// 	Timestamp  int64
// 	CommandIds []CommandId
// }

type Read struct {
	CommandId int32
	Key       state.Key
}

type ReadReply struct {
	CommandId int32
	Value     state.Value
}

type ProposeAndRead struct {
	CommandId int32
	Command   state.Command
	Key       state.Key
}

type ProposeAndReadReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
}

// // mr99rsm messages
// type Latency struct {
// 	MockOrAtLeast uint8
// 	ClientId      uint32
// 	CommandId     int32
// 	Timestamp     int64 // never read, I'm hijacking this
// }

// handling stalls and failures

type Beacon struct {
	Timestamp uint64
}

type BeaconReply struct {
	Timestamp uint64
}

type PingArgs struct {
	ActAsLeader uint8
}

type PingReply struct {
}

type BeTheLeaderArgs struct {
}

type BeTheLeaderReply struct {
}

type RegisterClientIdArgs struct {
	ClientId uint32
}

type RegisterClientIdReply struct {
	OK uint8
}

type GetView struct {
	PilotId int32
}

type GetViewReply struct {
	OK        uint8 // 1: ACTIVE; 0: PENDING
	ViewId    int32
	PilotId   int32 // index of this pilot
	ReplicaId int32 // unique id of this pilot replica
}
