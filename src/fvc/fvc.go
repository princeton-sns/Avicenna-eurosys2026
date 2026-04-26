package fvc

import (
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"paxosproto"
	"slowdowntimers"
	"state"
	"sync"
	"time"
)

const INJECT_TRANSIENT_SLOWDOWN = false
const INJECT_LONGLIVED_SLOWDOWN = true
const INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT = false
const INJECT_SSD_SLOWDOWN = true
const EXECUTE_WAKEUP_INTERVAL = 100 * time.Microsecond
const RECORD_EXEC_STATS = false

const CHAN_BUFFER_SIZE = 200000
const FALSE = uint8(0)
const TRUE = uint8(1)
const TRUE_AND_INFINITY_TRUE = uint8(2)
const FALSE_AND_INFINITY_TRUE = uint8(3)
const TRUE_AND_INFINITY_FALSE = uint8(4)
const FALSE_AND_INFINITY_FALSE = uint8(5)

const MAX_BATCH = 10000

const ALL_ACCEPT = false

// const BATCH_INTERVAL = 160 * time.Microsecond // 10 * time.Millisecond
const BATCH_INTERVAL = 250 * time.Microsecond // 10 * time.Millisecond
// const BATCH_INTERVAL = 10 * time.Millisecond // 10 * time.Millisecond

// Stats
const PRINT_STATS = true
const PRINT_STATS_INTERVAL = 5 * time.Second
const GC_DEBUG_ENABLED = false

type ReplicaStats struct {
	nBatches int32
	total    int32
}

type BatchedCmds struct {
	cmds      []state.Command
	proposals []*genericsmr.Propose
}

var totalMsgParseLat time.Duration
var averMsgParseLat time.Duration
var totalMsgCount int

func (r *Replica) printStats() {
	log.Println("-----------------------------------")
	// log.Printf("nBatches %v, avgBatchSize %v\n", r.stats.nBatches, float32(r.stats.total)/float32(r.stats.nBatches))
	// if GC_DEBUG_ENABLED {
	// 	var garC debug.GCStats
	// 	debug.ReadGCStats(&garC)
	// 	log.Printf("NumGC: %v; PauseTotal: %v; Pause: %v; LastGC: %v\n", garC.NumGC, garC.PauseTotal, garC.Pause, garC.LastGC)
	// 	log.Printf("Average GC pause: %v\n", time.Duration(int64(garC.PauseTotal)/garC.NumGC))
	// }
	chanSize := len(r.MsgParseLatChan)
	for i := 0; i < chanSize; i++ {
		msgParseLat := <-r.MsgParseLatChan
		totalMsgParseLat += *msgParseLat
		totalMsgCount++
	}
	if chanSize > 0 {
		averMsgParseLat = time.Duration(int(totalMsgParseLat) / totalMsgCount)
		log.Printf("Total msg parsing latency: %v, average parsing latency: %v.\n", totalMsgParseLat, averMsgParseLat)
	}
}

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	commitShortChan     chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	IsLeader            bool        // does this replica think it is the leader
	instanceSpace       []*Instance // the space of all instances (used and not yet used)
	crtInstance         int32       // highest active instance number that this replica knows about
	defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	Shutdown            bool
	counter             int
	flush               bool
	committedUpTo       int32
	stats               ReplicaStats
	slowdownTimers      *slowdowntimers.SlowdownTimers
	batchedCmdsChan     chan BatchedCmds
	replicaMu           []sync.Mutex
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type Instance struct {
	cmds   []state.Command
	ballot int32
	status InstanceStatus
	lb     *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, slowdownDuration time.Duration) *Replica {
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0,
		false,
		make([]*Instance, 15*1024*1024),
		0,
		-1,
		false,
		0,
		true,
		-1,
		ReplicaStats{},
		&slowdowntimers.SlowdownTimers{},
		make(chan BatchedCmds, genericsmr.CHAN_BUFFER_SIZE),
		make([]sync.Mutex, 7)}

	r.SlowdownDuration = slowdownDuration
	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(paxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(paxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(paxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(paxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(paxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(paxosproto.AcceptReply), r.acceptReplyChan)

	go r.run()

	return r
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

// sync with the stable store
func (r *Replica) sync() {

	// start slowdown injection
	// slowdownTimers := &slowdowntimers.SlowdownTimers{}
	if r.IsSlowdownReplica {
		// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
		if INJECT_TRANSIENT_SLOWDOWN {
			r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
			r.slowdownTimers.CheckAndDoSlowdown()
		} else if INJECT_LONGLIVED_SLOWDOWN && INJECT_SSD_SLOWDOWN {
			// log.Printf("Checking slowdown injection\n")
			r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
			r.slowdownTimers.CheckAndDoLongLivedSlowdown()
		}
	}
	// end slowdown injection

	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	// r.IsLeader = true // was commented but I think this is fine?
	go func() {
		r.startViewChange()
	}()
	return nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
	// r.replicaMu[replicaId].Lock()
	// defer r.replicaMu[replicaId].Unlock()

	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
	// r.replicaMu[replicaId].Lock()
	// defer r.replicaMu[replicaId].Unlock()

	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* ============= */

var clockChan chan bool

func (r *Replica) batcher() {
	cmds := []state.Command{}
	proposals := []*genericsmr.Propose{}

	for !r.Shutdown {
		// select {
		// case <-clockChan:
		// 	if len(cmds) != 0 {
		// 		// log.Printf("Proposing this batch. cmds %v\n", cmds)
		// 		r.batchedCmdsChan <- BatchedCmds{cmds: cmds, proposals: proposals}
		// 	}
		// 	cmds = []state.Command{}
		// 	proposals = []*genericsmr.Propose{}

		// 	break

		// default:
		// 	break
		// }

		select {
		case <-clockChan:
			if len(cmds) != 0 {
				// log.Printf("Proposing this batch. cmds %v\n", cmds)
				r.batchedCmdsChan <- BatchedCmds{cmds: cmds, proposals: proposals}
			}
			cmds = []state.Command{}
			proposals = []*genericsmr.Propose{}

			break

		case propose := <-r.ProposeChan:
			// newProp := genericsmr.Propose(*propose)
			// newCmd := state.Command(newProp.Command)
			// log.Printf("Receive propose from client %d, command id %d\n", propose.Command.ClientId, propose.Command.OpId)
			if !r.IsLeader {
				dlog.Printf("Replying not leader\n")
				preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
				r.ReplyProposeTS(preply, propose.Reply)
				log.Printf("Don't propose. r.IsLeader %v, r.defaultBallot %v, r.Id %v, replicaIdFromBallot(r.defaultBallot) %v\n",
					r.IsLeader, r.defaultBallot, r.Id, replicaIdFromBallot(r.defaultBallot))
				break
			}
			cmds = append(cmds, propose.Command)
			proposals = append(proposals, propose)
			break

		}
	}
}

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(BATCH_INTERVAL)
		clockChan <- true
	}
}

/* Main event processing loop */

func (r *Replica) run() {
	if r.Id == 0 {
		r.IsLeader = true
		r.IsSlowdownReplica = true
	}

	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
	}

	clockChan = make(chan bool, 1)
	go r.clock()
	go r.batcher()

	// onOffProposeChan := r.ProposeChan
	totalMsgParseLat = 0
	averMsgParseLat = 0
	totalMsgCount = 0

	printStatsTimer := &time.Timer{}
	if PRINT_STATS {
		printStatsTimer = time.NewTimer(PRINT_STATS_INTERVAL)
	}

	// allFired := false
	// slowdownTimers := &slowdowntimers.SlowdownTimers{}
	if r.IsSlowdownReplica && INJECT_TRANSIENT_SLOWDOWN {
		// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
		r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	} else if r.IsSlowdownReplica && INJECT_LONGLIVED_SLOWDOWN {
		r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	}
	// if INJECT_TRANSIENT_SLOWDOWN && r.IsSlowdownReplica {
	// 	// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
	// 	slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// }

	for !r.Shutdown {
		// if r.IsSlowdownReplica && INJECT_TRANSIENT_SLOWDOWN {
		// 	slowdownTimers.CheckAndDoSlowdown()
		// }
		//  else if r.IsSlowdownReplica && INJECT_LONGLIVED_SLOWDOWN {
		// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
		// }

		select {
		case batchedCmds := <-r.batchedCmdsChan:
			r.handleProposeBatch(batchedCmds)
			break

		// case <-clockChan:
		// 	//activate the new proposals channel
		// 	onOffProposeChan = r.ProposeChan
		// 	break

		// case propose := <-onOffProposeChan:
		// 	//got a Propose from a client
		// 	dlog.Printf("Proposal with op %d\n", propose.Command.Op)
		// 	r.handlePropose(propose)
		// 	//deactivate the new proposals channel to prioritize the handling of protocol messages
		// 	if MAX_BATCH > 100 {
		// 		onOffProposeChan = nil
		// 	}
		// 	break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*paxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*paxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*paxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, %v\n", commit.LeaderId, commit)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*paxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received short Commit from replica %d, %v\n", commit.LeaderId, commit)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*paxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*paxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
		case <-printStatsTimer.C:
			r.printStats()
			printStatsTimer.Reset(PRINT_STATS_INTERVAL)
		default:
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
	return r.makeUniqueBallot((ballot >> 4) + 1)
}

func replicaIdFromBallot(ballot int32) int32 {
	return ballot & 15
}

func (r *Replica) updateCommittedUpTo() {
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		r.instanceSpace[r.committedUpTo+1].status == COMMITTED {
		r.committedUpTo++
	}
	dlog.Printf("Committed up to now %v\n", r.committedUpTo)
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
	dlog.Printf("Ballot is %v in prepare\n", ballot)
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &paxosproto.Prepare{r.Id, instance, ballot, ti}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.prepareRPC, args)

		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
		// 	r.replicaMu[q].Lock()
		// 	defer r.replicaMu[q].Unlock()

		// 	r.SendMsg(q, rpcCode, msg)
		// }(q, r.prepareRPC, args)
	}
}

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command) {
	var pa paxosproto.Accept
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
	args := &pa
	//args := &paxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	} else {
		if ALL_ACCEPT {
			n = r.N - 1
		} else {
			n = (r.N >> 1) + 1
		}
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.acceptRPC, args)

		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
		// 	r.replicaMu[q].Lock()
		// 	defer r.replicaMu[q].Unlock()

		// 	r.SendMsg(q, rpcCode, msg)
		// }(q, r.acceptRPC, args)
	}
}

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	var pc paxosproto.Commit
	// var pcs paxosproto.CommitShort
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	args := &pc
	// pcs.LeaderId = r.Id
	// pcs.Instance = instance
	// pcs.Ballot = ballot
	// pcs.Count = int32(len(command))
	// argsShort := &pcs

	//args := &paxosproto.Commit{r.Id, instance, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id
	sent := 0

	for sent < n {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.commitRPC, args)
	}

	// if !ALL_ACCEPT {
	// 	for sent < (r.N>>1)+1 {
	// 		q = (q + 1) % int32(r.N)
	// 		if q == r.Id {
	// 			break
	// 		}
	// 		if !r.Alive[q] {
	// 			continue
	// 		}
	// 		sent++
	// 		r.SendMsg(q, r.commitRPC, args)
	// 		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
	// 		// 	r.replicaMu[q].Lock()
	// 		// 	defer r.replicaMu[q].Unlock()

	// 		// 	r.SendMsg(q, rpcCode, msg)
	// 		// }(q, r.commitShortRPC, argsShort)
	// 	}
	// 	for sent > r.N-1 {
	// 		q = (q + 1) % int32(r.N)
	// 		if q == r.Id {
	// 			break
	// 		}
	// 		if !r.Alive[q] {
	// 			continue
	// 		}
	// 		sent++
	// 		r.SendMsg(q, r.commitRPC, args)
	// 		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
	// 		// 	r.replicaMu[q].Lock()
	// 		// 	defer r.replicaMu[q].Unlock()

	// 		// 	r.SendMsg(q, rpcCode, msg)
	// 		// }(q, r.commitRPC, args)
	// 	}
	// } else {
	// 	for sent < n {
	// 		q = (q + 1) % int32(r.N)
	// 		if q == r.Id {
	// 			break
	// 		}
	// 		if !r.Alive[q] {
	// 			continue
	// 		}
	// 		sent++

	// 		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
	// 		// 	r.replicaMu[q].Lock()
	// 		// 	defer r.replicaMu[q].Unlock()

	// 		// 	r.SendMsg(q, rpcCode, msg)
	// 		// }(q, r.commitShortRPC, argsShort)
	// 		r.SendMsg(q, r.commitShortRPC, argsShort)
	// 		// r.SendMsg(q, r.commitRPC, args)
	// 	}
	// 	if r.Thrifty && q != r.Id {
	// 		for sent < r.N-1 {
	// 			q = (q + 1) % int32(r.N)
	// 			if q == r.Id {
	// 				break
	// 			}
	// 			if !r.Alive[q] {
	// 				continue
	// 			}
	// 			sent++
	// 			r.SendMsg(q, r.commitRPC, args)
	// 			// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
	// 			// 	r.replicaMu[q].Lock()
	// 			// 	defer r.replicaMu[q].Unlock()

	// 			// 	r.SendMsg(q, rpcCode, msg)
	// 			// }(q, r.commitRPC, args)
	// 		}
	// 	}
	// }
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	//if !r.IsLeader {
	dlog.Printf("Propose: clientId OpId %v %v\n", propose.Command.ClientId, propose.Command.OpId)
	if !r.IsLeader || (r.defaultBallot != -1 && r.Id != replicaIdFromBallot(r.defaultBallot)) {
		dlog.Printf("Replying not leader\n")
		preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance
	r.crtInstance++

	batchSize := len(r.ProposeChan) + 1

	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	dlog.Printf("Batched %d\n", batchSize)
	r.stats.nBatches++
	r.stats.total += int32(batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0}}
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		dlog.Printf("Classic round for instance %d inst: %v\n", instNo, r.instanceSpace[instNo])
	} else {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0}}

		r.recordInstanceMetadata(r.instanceSpace[instNo])
		r.recordCommands(cmds)

		// log.Printf("Calling sync() in handlePropose()\n")
		r.sync()

		r.bcastAccept(instNo, r.defaultBallot, cmds)
		dlog.Printf("Fast round for instance %d inst: %v\n", instNo, r.instanceSpace[instNo])
	}
}

func (r *Replica) handleProposeBatch(batchedCmds BatchedCmds) {
	startProp := time.Now()
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance
	r.crtInstance++

	cmds := batchedCmds.cmds
	props := batchedCmds.proposals

	batchSliceNum := len(r.batchedCmdsChan) + 1

	for i := 1; i < batchSliceNum; i++ {
		newBatchCmds := <-r.batchedCmdsChan
		cmds = append(cmds, newBatchCmds.cmds...)
		props = append(props, newBatchCmds.proposals...)
	}

	batchSize := len(cmds)

	dlog.Printf("Batched %d\n", batchSize)
	r.stats.nBatches++
	r.stats.total += int32(batchSize)

	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{props, 0, 0, 0, 0}}
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		// log.Printf("Classic round for instance %d inst: %v\n", instNo, r.instanceSpace[instNo])
	} else {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{props, 0, 0, 0, 0}}

		r.recordInstanceMetadata(r.instanceSpace[instNo])
		r.recordCommands(cmds)

		// log.Printf("Calling sync() in handlePropose()\n")
		r.sync()

		r.bcastAccept(instNo, r.defaultBallot, cmds)
		// log.Printf("Fast round for instance %d inst: %v\n", instNo, r.instanceSpace[instNo])
	}
	propLat := time.Since(startProp)
	r.MsgParseLatChan <- &propLat
}

func (r *Replica) startViewChange() {
	// log.Printf("I'M STARTING VIEWCHANGE\n")

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance
	r.crtInstance++

	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			make([]state.Command, 0),
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{nil, 0, 0, 0, 0}}
		ballot := r.makeUniqueBallot(0)
		// r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		r.bcastPrepare(instNo, ballot, true)

		// cch added
		r.defaultBallot = ballot

		dlog.Printf("Classic round for instance %d\n", instNo)
	} else {
		r.instanceSpace[instNo] = &Instance{
			make([]state.Command, 0),
			r.makeBallotLargerThan(r.defaultBallot),
			PREPARING,
			&LeaderBookkeeping{nil, 0, 0, 0, 0}}
		ballot := r.makeBallotLargerThan(r.defaultBallot)
		r.bcastPrepare(instNo, ballot, true)

		r.defaultBallot = ballot

		dlog.Printf("Classic round for instance %d\n", instNo)
	}

	// we don't want to block on previous instances from the leader that are uncommitted
	// send accepts for all of them
	for holeInst := r.committedUpTo + 1; holeInst < r.crtInstance-1; holeInst++ {
		inst := r.instanceSpace[holeInst]
		if inst != nil {
			if inst.status == COMMITTED {
				continue
			}
		}
		// restart but I did inf prepare accept above
		// we don't have a way to reply to the commands that might be here
		// and this is legal, we can accept a higher proposal
		r.instanceSpace[holeInst] = &Instance{
			make([]state.Command, 0),
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{nil, 0, 0, 0, 0}}
		// ballot := r.makeBallotLargerThan(r.defaultBallot)
		// r.bcastPrepare(instNo, ballot, true)
		r.bcastAccept(holeInst, r.defaultBallot, nil)
		dlog.Printf("Fast round for instance %d inst: %v\n", holeInst, r.instanceSpace[holeInst])

		// r.defaultBallot = ballot
		// inst.ballot = r.defaultBallot
		// inst.status = PREPARED
		// inst.lb.maxRecvBallot = 0
		// inst.lb.prepareOKs = 0
		// inst.lb.acceptOKs = 0
		// inst.lb.nacks = 0
		// r.recordInstanceMetadata(r.instanceSpace[holeInst])
		// r.recordCommands(inst.cmds)
		// r.sync()

		// r.bcastAccept(instNo, r.defaultBallot, inst.cmds)
	}
}

func (r *Replica) handlePrepare(prepare *paxosproto.Prepare) {
	inst := r.instanceSpace[prepare.Instance]
	var preply *paxosproto.PrepareReply
	dlog.Printf("Got prepare %v\n", *prepare)

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		}
		// log.Printf("Calling sync() in handlePrepare()\n")
		r.sync()
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}

	dlog.Printf("Replying to prepare %v\n", *preply)
	r.replyPrepare(prepare.LeaderId, preply)

	// this is correct. ToInfinity is just lost on reception
	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *paxosproto.Accept) {
	// startHandle := time.Now()
	inst := r.instanceSpace[accept.Instance]
	var areply *paxosproto.AcceptReply
	dlog.Printf("Got accept %v\n", *accept)

	if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			r.instanceSpace[accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil}
			areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
		if inst.lb != nil && inst.lb.clientProposals != nil {
			// TODO this could be it, the "is this correct" is from the original authoer
			//   but apparently cch commented the loop out...?
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else {
		// reordered ACCEPT
		r.instanceSpace[accept.Instance].cmds = accept.Command
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
	}

	if areply.OK == TRUE {
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		// log.Printf("Calling sync() in handleAccept()\n")
		r.sync()
	}

	dlog.Printf("Replying to accept %v\n", *areply)
	r.replyAccept(accept.LeaderId, areply)
	// handleLat := time.Since(startHandle)
	// r.MsgParseLatChan <- &handleLat
}

func (r *Replica) handleCommit(commit *paxosproto.Commit) {
	startHandle := time.Now()
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d %v\n", commit.Instance, commit)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			nil}
		// } else {
	} else if inst.status != COMMITTED {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			// TODO this could be it, same as above...
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
	handleLat := time.Since(startHandle)
	r.MsgParseLatChan <- &handleLat
}

func (r *Replica) handleCommitShort(commit *paxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			COMMITTED,
			nil}
		// } else {
	} else if inst.status != COMMITTED {
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *paxosproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]

	// what here?
	if inst.status == ACCEPTED {
		if inst.ballot < preply.Ballot {
			log.Panicf("Incorrect ballots for preparing->accepted instance %v %v\n", preply.Ballot, inst.ballot)
		} else {
			// we actually accept this and should have updated state in handleAccept()
			return
		}
	}

	// if this was a leader-change prepare, it isn't just for this instance
	// but for all future instances as well
	// need to check if it is that case and update default ballot
	// this really should just be a different message type.....
	if inst.status == COMMITTED {
		// we could have received a commit
	}

	if inst.status != PREPARING {
		dlog.Printf("PrepareReply Instance %v not PREPARING %v\n", preply.Instance, *preply)
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		dlog.Printf("PrepareReply got good reply %v\n", *preply)
		inst.lb.prepareOKs++

		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.cmds = preply.Command
			inst.lb.maxRecvBallot = preply.Ballot
			if inst.lb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}

		if inst.lb.prepareOKs+1 > r.N>>1 {
			r.IsLeader = true
			dlog.Printf("Replica %v: I'M THE NEW LEADER NOW\n", r.Id)
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
			// log.Printf("Calling sync() in handlePrepareReply()\n")
			r.sync()
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		// TODO: there is probably another active leader
		dlog.Printf("PrepareReply got bad reply %v\n", *preply)

		inst.lb.nacks++
		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			if inst.lb.clientProposals != nil {
				// try the proposals in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
			dlog.Printf("Got a quorum of bad replies stepping down as leader\n")
			// TODO: if this is viewchange reply, should consider
			// back off and retry view change if necessary
			// step down from being a leader
			r.IsLeader = false
		}
	}
}

func (r *Replica) handleAcceptReply(areply *paxosproto.AcceptReply) {
	inst := r.instanceSpace[areply.Instance]
	// log.Printf("Got AcceptReply %v\n", *areply)

	// cch added !r.IsLeader ...
	if inst.status != PREPARED && inst.status != ACCEPTED { //|| !r.IsLeader || r.Id != replicaIdFromBallot(r.defaultBallot) {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {

			inst = r.instanceSpace[areply.Instance]
			inst.status = COMMITTED
			if inst.lb.clientProposals != nil && !r.Dreply {
				// give client the all clear
				for i := 0; i < len(inst.cmds); i++ {
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp}
					r.ReplyProposeTS(propreply, inst.lb.clientProposals[i].Reply)
				}
			}

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			// r.sync() //is this necessary?

			r.updateCommittedUpTo()

			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			// TODO
			// step down from being a leader
			fmt.Printf("Replica %v: I'M STEPPING DOWN FROM BEING A LEADER\n", r.Id)
			r.IsLeader = false
		}
	}
}

type BatchExecLatency struct {
	start  time.Time
	execed time.Time
}

var totalCmdExected int32
var cmdExecLat time.Duration

func (r *Replica) executeCommands() {
	totalCmdExected = 0
	cmdExecLat = 0
	execLat := make([]*BatchExecLatency, 1024*1024)

	i := int32(0)
	for !r.Shutdown {
		// if INJECT_TRANSIENT_SLOWDOWN && r.IsSlowdownReplica {
		// 	slowdownTimers.CheckAndDoSlowdown()
		// }
		// if r.IsSlowdownReplica && INJECT_TRANSIENT_SLOWDOWN {
		// 	slowdownTimers.CheckAndDoSlowdown()
		// }
		// else
		// if r.IsSlowdownReplica && INJECT_LONGLIVED_SLOWDOWN {
		// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
		// }

		executed := false

		for i <= r.committedUpTo {
			dlog.Printf("Executing %v\n", i)
			if r.instanceSpace[i].cmds != nil {
				if RECORD_EXEC_STATS {
					execLat[i] = &BatchExecLatency{time.Now(), time.Time{}}
				}
				inst := r.instanceSpace[i]
				// log.Printf("About to reply to client for instance %v\n", inst)
				// if i != 0 {
				// 	log.Printf("Start executing instance %d with %d cmds, average cmds per instance is %d.\n", i, len(inst.cmds), totalCmdExected/i)
				// }
				for j := 0; j < len(inst.cmds); j++ {
					beforeExected := time.Now()
					val := inst.cmds[j].Execute(r.State)
					afterExected := time.Now().Sub(beforeExected)
					totalCmdExected++
					cmdExecLat += afterExected
					// log.Printf("Executed %d-th command, which takes %v, average execTime is %v.\n", totalCmdExected, afterExected, time.Duration(int64(cmdExecLat)/int64(totalCmdExected)))
					//if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil {
					// log.Printf("About to reply to client %d for command %d\n", inst.cmds[j].ClientId, inst.cmds[j].OpId)
					if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil && j < len(inst.lb.clientProposals) {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							inst.lb.clientProposals[j].Timestamp}
						// dlog.Printf("Replying %v\n", *propreply)

						r.ReplyProposeTS(propreply, inst.lb.clientProposals[j].Reply)
					}
				}
				// log.Printf("Finished replied to client for instance %v\n", inst)
				if RECORD_EXEC_STATS {
					execLat[i].execed = time.Now()
					log.Printf("Execution duration of instance %d is %v.\n", i, execLat[i].execed.Sub(execLat[i].start))
				}
				i++
				executed = true
			} else {
				break
			}
		}

		if !executed {
			time.Sleep(1000)
		}
	}

}
