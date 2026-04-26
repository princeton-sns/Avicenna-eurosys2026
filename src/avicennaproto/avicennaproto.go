package avicennaproto

import (
	"genericsmrproto"
	"state"
	"time"
)

// statuses
const (
	ACCEPT uint8 = iota
	MOCKACCEPT
	COMMIT
	MOCKCOMMIT
	// COMMIT_SHORT
	RECEIVED // TODO add requesting the client command when receiving accept for a command I haven't yet received.
	NOT_RECEIVED
	COMMITTED
	GHOSTCOMMITTED
	ACCEPTED
	GHOSTACCEPTED
	CLIENTPING
	CLIENTPINGREPLY
	CLIENTRTTTABLE
	CLIENTMOCKREPLY
)

// type CommandId struct {
// 	ClientId uint32
// 	OpId     int32
// }

type ClientRttTable struct {
	ClientId int32
	Rtts     []int64
}

type ClientLatency struct {
	CmdId   state.CommandId
	Latency int64
}

type ClientCommitLatency struct {
	CmdId              state.CommandId
	RealCommitLatency  int64
	GhostCommitLatency int64
}

type GhostExecLatency struct {
	CmdId    state.CommandId
	ExecTime int64
}

type ClientLatencyWithExecTime struct {
	ClientAndLatency ClientLatency
	ExecTime         int64
}

type ClientLatencyWithTimestamp struct {
	ClientAndLatency ClientLatency
	Timestamp        time.Time
}

// we don't need to send the commands to other replicas if they are receiving
// them from clients
type InstanceCommands struct {
	Instance int32
	Status   uint8
	Commands []state.CommandAvi
	Phase    int32
	DoGhost  uint8
}

type InstanceCommandIdsOLD struct {
	Instance   int32
	Status     uint8
	CommandIds []state.CommandId
	DoMock     uint8
}

// type InstanceCommands struct {
// 	Instance int32
// 	Status   uint8
// 	Commands []state.Command
// }

// type CoordinatorAccept struct {
// 	ReplicaId int32
// 	Instance  int32
// 	Phase     int32
// 	Commands  []state.Command
// }

type Accept struct {
	ReplicaId int32
	Phase     int32
	Instances []InstanceCommands
}

type AcceptExecTime struct {
	ReplicaId     int32
	Phase         int32
	Instances     []InstanceCommands
	MockExecTimes []genericsmrproto.MockExecTime_
}

type Ping struct {
	ReplicaId int32
	// ReplicaId int32
	// Phase     int32
	// Instances []InstanceCommandIds
}

type PingReply struct {
	ReplicaId int32
}

type RttTable struct {
	RttTable []int64
}

type AcceptReply struct {
	Phase    int32
	Instance int32
	OK       uint8
}

type Rotate struct {
	ReplicaId     int32
	Phase         int32
	Instances     []InstanceCommands
	MockInstances []InstanceCommands
}

type Commit struct {
	ReplicaId       int32
	Phase           int32
	Mock            uint8
	Instances       []InstanceCommands
	GlobalMaxCommit int32
}

type CommitExecTime struct {
	ReplicaId     int32
	Phase         int32
	Mock          uint8
	Instances     []InstanceCommands
	MockExecTimes []genericsmrproto.MockExecTime_
}

// type MockCommit struct {
// 	ReplicaId int32
// 	Phase     int32
// 	Instances []InstanceCommandIds
// }

type CommitShort struct {
	Id       int32
	Instance int32
	Count    int32
}
