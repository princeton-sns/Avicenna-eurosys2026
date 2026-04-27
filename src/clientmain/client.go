package main

import (
	"avicennaproto"
	"bufio"
	"dlog"
	"fastrpc"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	filepath2 "path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"state"
	"stats"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// const REQUEST_TIMEOUT = 1500 * time.Millisecond
// const REQUEST_TIMEOUT = 100 * time.Millisecond
const ATLEAST_MESSAGE_INTERVAL = 10 * time.Millisecond // TODO Maybe do something more sophisticated. Is this a timeout?
const GET_VIEW_TIMEOUT = 100 * time.Millisecond
const GC_DEBUG_ENABLED = false
const PRINT_STATS = false
const N_STANDARD_DEVIATIONS = 10
const NPINGS = 50
const NPINGS_TO_DISCARD = 25

const CLIENT_TIMEOUT_INTERVAL = 2 * time.Second

const AVICENNA_PROPOSE_REALLEADER_ONLY = true
const AVICENNA_PROPOSE_LEADERS_ONLY = true
const AVICENNA_PROPOSE_NONSTANDBYS_ONLY = true
const ENABLE_AT_LEAST_TIMER = false

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var twoLeaders *bool = flag.Bool("twoLeaders", false, "Two leaders for slowdown tolerance. Defaults to false.")
var domr99rsm *bool = flag.Bool("domr99rsm", false, "MR99RSM protocol, send to all replicas. Defaults to false.")
var doavicenna *bool = flag.Bool("doavicenna", false, "Avicenna protocol. Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var cid *int = flag.Int("id", -1, "Client ID.")
var nclients *int = flag.Int("nclients", 1, "Number of clients this thread should simulate. Defaults to 1.")
var cpuProfile *string = flag.String("cpuprofile", "", "Name of file for CPU profile. If empty, no profile is created.")
var maxRuntime *int = flag.Int("runtime", -1, "Max duration to run experiment in second. If negative, stop after sending up to reqsNb requests")

// var debug *bool = flag.Bool("debug", false, "Enable debug output.")
var trim *float64 = flag.Float64("trim", 0.25, "Exclude some fraction of data at the beginning and at the end.")
var prefix *string = flag.String("prefix", "", "Path prefix for filenames.")
var hook *bool = flag.Bool("hook", true, "Add shutdown hook. Default: true.")
var verbose *bool = flag.Bool("verbose", false, "Print throughput to stdout.")
var numKeys *uint64 = flag.Uint64("numKeys", 100000, "Number of keys in simulated store.")
var proxyReplica *int = flag.Int("proxy", -1, "Replica Id to proxy requests to. If id < 0, use request Id mod N as default.")
var sendOnce *bool = flag.Bool("once", false, "Send request to only one leader.")
var tput_interval *float64 = flag.Float64("tput_interval_in_sec", 1, "Time interval to record and print throughput")
var request_timeout *time.Duration = flag.Duration("request_timeout", CLIENT_TIMEOUT_INTERVAL, "Timeout for client retransmits. Defaults to 350ms.")
var random_interval *int64 = flag.Int64("random_interval", 0, "Clients wait [0, random_interval] before sending another request. Defaults to 0ms.")
var percentMocked *float64 = flag.Float64("percent_mocked", 100, "The percent of requests to Mock. Defaults to 100.")

// GC debug
var garPercent = flag.Int("garC", 50, "Collect info about GC")

var N int

var clientId uint32

var successful []int
var rsp []bool

// var rarray []int

var latencies []int64
var readlatencies []int64
var writelatencies []int64

var timestamps []time.Time

var writerLocks []sync.Mutex

type DataPoint struct {
	elapse    time.Duration
	reqsCount int64
	t         time.Time
}

type Response struct {
	OpId       int32
	rcvingTime time.Time
	timestamp  int64
}

type MessageTime struct {
	rcvingTime time.Time
	msg        interface{}
}

type View struct {
	ViewId    int32
	PilotId   int32
	ReplicaId int32
	Active    bool
}

var throughputs []DataPoint

type Client struct {
	ClientId uint32
}

func writeWithDeadline(conn *net.Conn, writer *bufio.Writer, msgType uint8, msg *fastrpc.Serializable) error {
	err := (*conn).SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return err
	}
	defer (*conn).SetDeadline(time.Time{})

	err = writer.WriteByte(msgType)
	if err != nil {
		return err
	}

	(*msg).Marshal(writer)
	err = writer.Flush()
	return err
}

func atLeastTimerFunc(cmdId int32, timerChan chan int32, duration time.Duration) {
	time.Sleep(duration)
	timerChan <- cmdId
}

func YCSBZipf(r *rand.Rand, s float64, N float64) int64 {
	// 1. Generate a uniform random float between 0.0 and 1.0
	u := r.Float64()

	// 2. Apply the continuous Zipf approximation formula
	// x = (u * (N^(1-s) - 1) + 1)^(1 / (1-s))
	power := 1.0 - s
	x := math.Pow(u*(math.Pow(N, power)-1.0)+1.0, 1.0/power)

	// 3. Cast to integer (returns 1 to N)
	return int64(x)
}

func main() {

	flag.Parse()
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	log.Printf("Client retx interval %v\n", *request_timeout)
	if *noLeader {
		log.Printf("Proxy leader: %v\n", *proxyReplica)
	}
	if *doavicenna {
		log.Printf("Client starts Avicenna.\n")
	}

	runtime.GOMAXPROCS(*procs)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			dlog.Printf("Error creating CPU profile file %s: %v\n", *cpuProfile, err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
		go catchKill(interrupt)
	}

	if *hook {
		c := make(chan os.Signal, 1)
		// signal.Notify(c, os.Interrupt)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go shutdownHook(c)
	}

	if *cid < 0 {
		clientId = generateRandomClientId()
	} else {
		clientId = uint32(*cid)
	}

	log.Printf("Starting client %v!\n", clientId)

	r := rand.New(rand.NewSource(int64(clientId)))
	// zipf := rand.NewZipf(r, *s, *v, *numKeys)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	log.Printf("Connecting to master at %v\n", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	writerLocks = make([]sync.Mutex, N)

	//rarray := make([]int, *reqsNb)
	put := make([]bool, *reqsNb)

	// randObj := rand.New(rand.NewSource(time.Now().UnixNano()))

	karray := make([]int64, *reqsNb)
	if *noLeader { /*epaxos*/
		for i := 0; i < len(karray); i++ {

			if *conflicts >= 0 {
				r := rand.Intn(100)
				// karray[i] = (int64(i) << 32) | int64(clientId)
				if r < *conflicts {
					karray[i] = 0
				} else {
					// karray[i] = int64(43 + i)
					karray[i] = (int64(i) << 32) | int64(clientId)
				}
				r = rand.Intn(100)
				if r < *writes {
					put[i] = true
				} else {
					put[i] = false
				}
			} else {
				// karray[i] = int64(zipf.Uint64())
				karray[i] = YCSBZipf(r, *s, float64(*numKeys))
			}
		}
	} else {
		for i := 0; i < len(karray); i++ {
			karray[i] = rand.Int63n(int64(*numKeys))

			// r := rand.Intn(100)
			put[i] = true
			// if r < *writes {
			// 	put[i] = true
			// } else {
			// 	put[i] = false
			// }
		}
	}

	if *conflicts >= 0 {
		log.Println("Uniform distribution")
	} else {
		log.Println("Zipfian distribution: s = ", *s)
	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Panicf("Error connecting to replica %d: %v\n", i, err)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])

	}

	if *twoLeaders || *doavicenna {
		log.Println("Registering client id", clientId)
		/* Register Client Id */
		for i := 0; i < N; i++ {
			rciArgs := &genericsmrproto.RegisterClientIdArgs{ClientId: clientId}
			writers[i].WriteByte(genericsmrproto.REGISTER_CLIENT_ID)
			rciArgs.Marshal(writers[i])
			writers[i].Flush()
		}
	}

	time.Sleep(5 * time.Second)
	// todo this was commented out
	// registerClientIdSuccessful := waitRegisterClientIdReplies(readers, N)
	// log.Printf("Client Id Registration succeeds: %d out of %d\n", registerClientIdSuccessful, N)

	successful = make([]int, N)
	leader := -1

	// second leader
	leader2 := -1

	isRandomLeader := false // ePaxos with no proxy

	// views for two leaders
	var views []*View
	atLeastTimerCmdIdChan := make(chan int32, *reqsNb)

	if *noLeader == false {
		if *twoLeaders == false {
			reply := new(masterproto.GetLeaderReply)
			if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
				log.Fatalf("Error making the GetLeader RPC error: %v\n", err)
			}
			leader = reply.LeaderId
			log.Printf("The leader is replica %d\n", leader)
		} else { // two leaders // todo NEED TO DO SOMETHING LIKE THIS MR99RSM
			reply := new(masterproto.GetTwoLeadersReply)

			if err = master.Call("Master.GetTwoLeaders", new(masterproto.GetTwoLeadersArgs), reply); err != nil {
				log.Fatalf("Error making the GetTwoLeaders")
			}
			leader = reply.Leader1Id
			leader2 = reply.Leader2Id
			log.Printf("The leader 1 is replica %d. The leader 2 is replica %d\n", leader, leader2)

			// Init views. Assume initial view id is 0
			views = make([]*View, 2)
			views[0] = &View{ViewId: 0, PilotId: 0, ReplicaId: int32(leader), Active: true}
			views[1] = &View{ViewId: 0, PilotId: 1, ReplicaId: int32(leader2), Active: true}

		}
	} else if *proxyReplica >= 0 && *proxyReplica < N {
		leader = *proxyReplica
		log.Printf("The epaxos proxy is replica %d\n", leader)
	} else { // epaxos and no designated proxy specified
		isRandomLeader = true
		log.Printf("The epaxos proxy is random, based on command number!\n")
	}

	if *check {
		rsp = make([]bool, *reqsNb)
		for j := 0; j < *reqsNb; j++ {
			rsp[j] = false
		}
	}

	var done chan bool
	var readings chan *DataPoint
	tput_interval_in_sec := time.Duration(*tput_interval * 1e9)
	if *verbose {
		done = make(chan bool, 1)
		readings = make(chan *DataPoint, 600)
		go printer(readings, done)
	}

	// var surgMock chan bool
	var execTimeChan chan genericsmrproto.MockExecTime_
	var leaderReplyChan chan int32
	var pilot0ReplyChan chan Response
	var viewChangeChan chan *View
	var mockCommittedChan chan MessageTime
	var realCommittedChan chan MessageTime
	// var replicaReplyChans []chan int32

	// with pre-specified leader, we know which reader to check reply
	if !*twoLeaders && !*doavicenna {
		leaderReplyChan = make(chan int32, *reqsNb)
		if isRandomLeader {
			log.Printf("Start listening to every replicas.\n")
			// go waitRepliesRandomLeader(readers, N, leaderReplyChan)
			for i := 0; i < N; i++ {
				go waitRepliesEpaxos(readers, i, leaderReplyChan)
			}
		} else {
			log.Printf("Start listening to replica %d.\n", leader)
			// for i := 0; i < N; i++ {
			// 	go waitRepliesEpaxos(readers, i, leaderReplyChan)
			// }
			go waitReplies(readers, leader, *reqsNb, leaderReplyChan, *reqsNb)
		}
	} else if *twoLeaders {
		// with another pre-specified leader, we need to check other reply channel, and another reader
		pilot0ReplyChan = make(chan Response, *reqsNb)
		viewChangeChan = make(chan *View, 100)
		for i := 0; i < N; i++ {
			go waitRepliesPilot(readers, i, pilot0ReplyChan, viewChangeChan, *reqsNb*2)
		}
	} else { // avicenna
		execTimeChan = make(chan genericsmrproto.MockExecTime_, *reqsNb)
		pilot0ReplyChan = make(chan Response, N*(*reqsNb))
		viewChangeChan = make(chan *View, 100)
		mockCommittedChan = make(chan MessageTime, N*(*reqsNb))
		realCommittedChan = make(chan MessageTime, N*(*reqsNb))
		for i := 0; i < N; i++ {
			go waitRepliesReplica(readers, i, pilot0ReplyChan, viewChangeChan, mockCommittedChan, realCommittedChan, execTimeChan, *reqsNb*N) // was *N
		}
	}

	latencies = make([]int64, 0, *reqsNb)
	readlatencies = make([]int64, 0, *reqsNb)
	writelatencies = make([]int64, 0, *reqsNb)
	timestamps = make([]time.Time, 0, *reqsNb)

	throughputs = make([]DataPoint, 0, 600)

	var reqsCount int64 = 0

	before_total := time.Now()
	lastThroughputTime := before_total

	time.Sleep(5 * time.Second)
	log.Println("Finish 5-second warm-up!")

	var pilotErr, pilotErr1 error
	var lastGVSent0, lastGVSent1 time.Time
	lastPrint := time.Now()
	i := 0

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	var lastLatency int64 = -1
	type commitLatencyTrack struct {
		SentTime      time.Time
		Received      bool
		DoGhost       bool
		Inst          int32
		Cmds          []state.CommandAvi
		CommitLatency time.Duration
	}
	type sentTimeReceived struct {
		sentTime time.Time
		received bool
		doMock   bool
	}

	ghostCommitMap := make(map[int32]*commitLatencyTrack)
	realCommitMap := make(map[int32]*commitLatencyTrack)

	if !*twoLeaders && !*doavicenna && !isRandomLeader && *noLeader == false {
		leader = 0
		// log.Printf("I believe this is fvc starting leader at %v\n", leader)
	}
	doMock := 0
	overtime := false
	// doneTimer := time.NewTimer(time.Duration(*maxRuntime*time.Now().Second()))
	log.Println("================ Entering Main Loop ================")
	for i = 0; i < *reqsNb; i++ {
		id := int32(i)

		// Avicenna does not use this
		args := genericsmrproto.Propose{id, state.Command{ClientId: clientId, OpId: id, Op: state.PUT, K: 0, V: 0}, time.Now().UnixNano()}

		// For Avicenna only
		argsExecTime := genericsmrproto.ProposeWithExecTime{id, state.Command{ClientId: clientId, OpId: id, Op: state.PUT, K: 0, V: 0},
			genericsmrproto.EndToEndLatency_{Latency: -1, CommandId: -1}}

		if *doavicenna {
			if put[i] {
				argsExecTime.Command.Op = state.PUT
			} else {
				argsExecTime.Command.Op = state.GET
			}
			argsExecTime.Command.K = state.Key(karray[i])
			argsExecTime.Command.V = state.Value(i)
			if *doavicenna {
				// argsExecTime.Timestamp = 0
				if doMock > 0 { // only piggyback a timestamp if I mocked the last request.
					argsExecTime.EndToEndLatency.Latency = lastLatency
					argsExecTime.EndToEndLatency.CommandId = id - 1
					// log.Printf("About to report e2eLatency, e2eLatency: %d, exeLatCmdId: %d\n", lastLatency, id-1)
				}
			}
		}
		// var execTimeFromMessage int64
		/* Prepare proposal */

		if put[i] {
			args.Command.Op = state.PUT
		} else {
			args.Command.Op = state.GET
		}
		args.Command.K = state.Key(karray[i])
		args.Command.V = state.Value(i)
		//args.Timestamp = time.Now().UnixNano() // was commented out in copilot code

		before := time.Now()
		timestamps = append(timestamps, before)

		// decide whether or not to shadow this request.
		doMock = 0
		if *doavicenna {
			if rng.Float64() < float64(*percentMocked)/100 {
				doMock = 1
				ghostCommitMap[id] = &commitLatencyTrack{SentTime: before, Received: false, DoGhost: true, Inst: -1, Cmds: nil, CommitLatency: -1}
				realCommitMap[id] = &commitLatencyTrack{SentTime: before, Received: false, DoGhost: true, Inst: -1, Cmds: nil, CommitLatency: -1}
				// log.Printf("Command %d is shadow processed. Creating real and ghost maps, sending time %v\n", id, before)
			} else {
				doMock = 0
			}
		}

		argsExecTime.CommandId = int32(doMock)

		repliedCmdId := int32(-1)
		fromPilot := -1
		var rcvingTime time.Time
		var to *time.Timer
		// var atLeastTimer *time.Timer
		// to.Stop()
		var batch = int64(-1)
		succeeded := false
		dlog.Printf("Client %v on req %v\n", clientId, id)
		// mockSucceeded := false
		// term := time.NewTimer(time.Duration(*maxRuntime))
		// todo cch: feel like there should be an experiment length timer
		// if it doesn't receive a response clients just block
		if *twoLeaders {
			for {
				if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
					rcvingTime = time.Now()
					dlog.Printf("Client %v over time waiting for request, breaking\n", clientId)
					overtime = true
					break
				}
				// Check if there is newer view
				for i := 0; i < len(viewChangeChan); i++ {
					newView := <-viewChangeChan
					if newView.ViewId > views[newView.PilotId].ViewId {
						dlog.Printf("New view info: pilotId %v,  ViewId %v, ReplicaId %v\n", newView.PilotId, newView.ViewId, newView.ReplicaId)
						views[newView.PilotId].PilotId = newView.PilotId
						views[newView.PilotId].ReplicaId = newView.ReplicaId
						views[newView.PilotId].ViewId = newView.ViewId
						views[newView.PilotId].Active = true
					}
				}

				// get random server to ask about new view
				serverId := rand.Intn(N)
				if views[0].Active {
					leader = int(views[0].ReplicaId)
					pilotErr = nil
					if leader >= 0 {
						writers[leader].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader])
						pilotErr = writers[leader].Flush()
						if pilotErr != nil {
							views[0].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[0].Active {
					leader = -1
					if lastGVSent0 == (time.Time{}) || time.Since(lastGVSent0) >= GET_VIEW_TIMEOUT {
						for ; serverId == 0; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{0}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent0 = time.Now()
					}
				}

				if views[1].Active {
					leader2 = int(views[1].ReplicaId)
					/* Send to second leader for two-leader protocol */
					pilotErr1 = nil
					if *twoLeaders && !*sendOnce && leader2 >= 0 {
						writers[leader2].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader2])
						pilotErr1 = writers[leader2].Flush()
						if pilotErr1 != nil {
							views[1].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[1].Active {
					leader2 = -1
					if lastGVSent1 == (time.Time{}) || time.Since(lastGVSent1) >= GET_VIEW_TIMEOUT {
						for ; serverId == 1; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{1}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent1 = time.Now()
					}
				}
				if !succeeded {
					continue
				}

				// we successfully sent to at least one pilot
				succeeded = false
				to = time.NewTimer(*request_timeout) //REQUEST_TIMEOUT)
				toFired := false
				for true {
					select {
					case e := <-pilot0ReplyChan:
						repliedCmdId = e.OpId
						batch = e.timestamp
						rcvingTime = e.rcvingTime
						if repliedCmdId == id {
							to.Stop()
							succeeded = true
						}

					case <-to.C:
						// log.Printf("Client %v: TIMEOUT for request %v\n", clientId, id)
						repliedCmdId = -1
						rcvingTime = time.Now()
						succeeded = false
						toFired = true

					default:
					}

					if succeeded {
						if *check {
							rsp[id] = true
						}
						reqsCount++
						break
					} else if toFired {
						break
					}

					if repliedCmdId != -1 && repliedCmdId < id {
						// update latency if this response actually arrived ealier
						newLat := int64(rcvingTime.Sub(timestamps[repliedCmdId]) / time.Microsecond)
						if newLat < latencies[repliedCmdId] {
							latencies[repliedCmdId] = newLat
						}
					}
				} // end of foor loop waiting for result
				// successfully get the response. continue with the next request
				if succeeded {
					break
				} else if toFired {
					if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
						rcvingTime = time.Now()
						dlog.Printf("Client %v over time waiting for request, breaking\n", clientId)
						overtime = true
						break
					}
					continue
				}
			} // end of copilot
		} else if *doavicenna {

			// to = time.NewTimer(1 * time.Second)
			// to.Stop()
			succeeded = false

			numReplicaToSend := 0
			if AVICENNA_PROPOSE_REALLEADER_ONLY {
				if argsExecTime.EndToEndLatency.Latency > 0 {
					numReplicaToSend = N
				} else {
					numReplicaToSend = 1
				}
			} else if AVICENNA_PROPOSE_LEADERS_ONLY {
				if argsExecTime.EndToEndLatency.Latency > 0 {
					numReplicaToSend = N
				} else {
					numReplicaToSend = 2
				}
			} else if AVICENNA_PROPOSE_NONSTANDBYS_ONLY {
				if argsExecTime.EndToEndLatency.Latency > 0 {
					numReplicaToSend = N
				} else {
					numReplicaToSend = (N >> 1) + 2 // f+2 non-standbys
				}
			} else {
				numReplicaToSend = N
			}

			for nodeid := 0; nodeid < numReplicaToSend; nodeid++ {
				go func(nodeid int, argsExecTime genericsmrproto.ProposeWithExecTime) {
					writerLocks[nodeid].Lock()
					defer writerLocks[nodeid].Unlock()
					dlog.Printf("Sending %v to replica %v....\n", argsExecTime, nodeid)
					servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second)) // caution
					writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME)
					argsExecTime.Marshal(writers[nodeid])
					writers[nodeid].Flush()
				}(nodeid, argsExecTime)
				// log.Printf("Sending proposal to node %v, argsExecTime %v", nodeid, argsExecTime)
				// TODO This pattern is everywhere make function.
				// servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second)) // caution
				// writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME)
				// argsExecTime.Marshal(writers[nodeid])
				// writers[nodeid].Flush()
			}
			to = time.NewTimer(*request_timeout) //REQUEST_TIMEOUT) // start a timer so that we at least check if it's overtime
			dlog.Printf("Start timer to for 350s in line 660.\n")
			// if atLeastTimer != nil {
			// 	// 	dlog.Printf("TIME OUT after receiving mockCommitted before receiving reply, stopping atLeastTimer\n")
			// 	if !atLeastTimer.Stop() {
			// 		select {
			// 		case <-atLeastTimer.C:

			// 		default:
			// 		}
			// 	}
			// }
			// atLeastTimer = time.NewTimer(200 * time.Second) // just initialize
			dlog.Printf("Start timer atLeastTimer for 200s in line 662.\n")
			for !succeeded {
				// select {
				// case <-doneTimer.C:
				// 	overtime = true
				// default:
				// }
				if overtime {
					break
				}
				// // I don't think we need to retransmit for mr99rsm
				// for nodeid := 0; nodeid < N; nodeid++ {
				// 	dlog.Println("Writing to node %v", args)
				// 	writers[nodeid].WriteByte(genericsmrproto.PROPOSE)
				// 	args.Marshal(writers[nodeid])
				// 	writers[nodeid].Flush()
				// }
				if succeeded {
					log.Panicf("Succeeded?\n")
				}
				succeeded = false
				// log.Printf("Request Timeout %v to %v\n", REQUEST_TIMEOUT, to)
				toFired := false
				dlog.Printf("Entering waiting for request %v\n", argsExecTime)
				for {
					// select {
					// case <-doneTimer.C:
					// 	overtime = true
					// default:
					// }
					if overtime {
						break
					}

					// log.Printf("Entering select for id %v\n", id)
					select {
					case e := <-pilot0ReplyChan:
						dlog.Printf("Client got a reply of %v, waiting for %v\n", e.OpId, id)
						// log.Printf("Got a reply from a replica? execute time %v id got %v waiting for %v\n", e.timestamp, e.OpId, id)
						repliedCmdId = e.OpId
						batch = e.timestamp
						rcvingTime = e.rcvingTime
						// execTimeFromMessage = e.timestamp
						// log.Printf("Client got execution time %d from propose reply\n", execTimeFromMessage)
						if repliedCmdId == id {
							if to != nil {
								to.Stop()
							}
							// if atLeastTimer != nil {
							// 	if !atLeastTimer.Stop() {
							// 		select {
							// 		case <-atLeastTimer.C:

							// 		default:
							// 		}
							// 	}
							// }
							succeeded = true
						}

					// case e := <-surgMock:
					// doMockForce = e

					// we need to tell the Replicas we heard from the MockLeader
					case mockCommittedMessageTime := <-mockCommittedChan:
						mockCommittedMsg := mockCommittedMessageTime.msg.(*genericsmrproto.MockCommitted)
						mockCommitted := *mockCommittedMsg
						ghostCommitId := mockCommitted.OpId

						// log.Printf("Got GhostCommitted, OpId %d, realCommitted received %d\n", ghostCommitId, realCommitMap[ghostCommitId].Received)
						if sendTimeReceived, exist := ghostCommitMap[ghostCommitId]; exist && !sendTimeReceived.Received {
							// if sendTimeReceived, exist := ghostCommitMap[ghostCommitId]; exist && !sendTimeReceived.Received {
							// log.Printf("First time receive ghostCommitted for command %d, receive time %v, sent time %v, latency %v\n", ghostCommitId, mockCommittedMessageTime.rcvingTime, ghostCommitMap[ghostCommitId].SentTime, mockCommittedMessageTime.rcvingTime.Sub(ghostCommitMap[ghostCommitId].SentTime))
							// if mockReply.OpId == id && !mockSucceeded {
							ghostCommitMap[ghostCommitId].Received = true
							ghostCommitMap[ghostCommitId].Inst = mockCommitted.Instance
							ghostCommitMap[ghostCommitId].Cmds = mockCommitted.Commands
							// mockSucceeded = true
							// send the MockLatency to the replicas
							rawLat := mockCommittedMessageTime.rcvingTime.Sub(ghostCommitMap[ghostCommitId].SentTime)
							if ghostCommitId != mockCommitted.OpId {
								log.Printf("Weird, ghostCommitId %v, mockCommitted.OpId %v\n", ghostCommitId, mockCommitted.OpId)
							}
							ghostCommitMap[ghostCommitId].CommitLatency = rawLat
							if int64(rawLat/time.Microsecond) < 0 {
								log.Printf("Weird, receive a ghost commit latency less than 0. id %d, Sent time %v, receive time %v, latency %v, int64 latency %d\n",
									ghostCommitId, ghostCommitMap[ghostCommitId].SentTime, mockCommittedMessageTime.rcvingTime, rawLat, int64(rawLat/time.Microsecond))
							}
							// lat := int64(rawLat / time.Microsecond)
							// mc := genericsmrproto.CommittedFromClient{mockCommitted.Instance, clientId, mockCommitted.OpId, lat, mockCommitted.Commands}
							if realCommitMap[ghostCommitId].Received {
								realLat := int64(realCommitMap[ghostCommitId].CommitLatency / time.Microsecond)
								ghostLat := int64(rawLat / time.Microsecond)
								CommitLatFeedback := genericsmrproto.CommitLatencyFeedback{CommandId: state.CommandId{ClientId: clientId, OpId: ghostCommitId},
									RealInstance: realCommitMap[ghostCommitId].Inst, GhostInstance: ghostCommitMap[ghostCommitId].Inst,
									RealCommitLatency: realLat, GhostCommitLatency: ghostLat,
									RealInstCmds: realCommitMap[ghostCommitId].Cmds, GhostInstCmds: ghostCommitMap[ghostCommitId].Cmds}
								// log.Printf("Sending commit latency feedback: OpId %d, real commit latency %d, ghost commit latency %d\n", ghostCommitId, realLat, ghostLat)
								// log.Printf("CommitLatFeedback to send %v\n", CommitLatFeedback)
								if CommitLatFeedback.RealCommitLatency < 0 || CommitLatFeedback.GhostCommitLatency < 0 {
									log.Printf("Weird, we have a commit latency less than 0, %v, sent time in real map %v, sent time in ghost map %v.\n", CommitLatFeedback, realCommitMap[ghostCommitId].SentTime, ghostCommitMap[ghostCommitId].SentTime)
									log.Printf("Ghost commit latency %v. real commit atency%v.\n", ghostCommitMap[ghostCommitId].CommitLatency, realCommitMap[ghostCommitId].CommitLatency)
								}

								for nodeid := 0; nodeid < N; nodeid++ {
									go func(nodeid int, CommitLatFeedback genericsmrproto.CommitLatencyFeedback) {
										writerLocks[nodeid].Lock()
										defer writerLocks[nodeid].Unlock()

										servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second))
										writers[nodeid].WriteByte(genericsmrproto.COMMIT_LATENCY_FEEDBACK)
										CommitLatFeedback.Marshal(writers[nodeid])
										writers[nodeid].Flush()
									}(nodeid, CommitLatFeedback)
								}
							} else { // ghostCommitted received before realCommited
								// timerDuration := 1 * time.Microsecond
								// log.Printf("When receiving ghostCommit for cmd %d, realCommit has not received.\n", ghostCommitId)
								if ENABLE_AT_LEAST_TIMER {
									timerDuration := 20 * time.Millisecond
									go atLeastTimerFunc(ghostCommitId, atLeastTimerCmdIdChan, timerDuration)
								}
								// timerDuration := time.Duration(0.7 * float64(rawLat))
								// if atLeastTimer != nil {
								// 	if !atLeastTimer.Stop() {
								// 		select {
								// 		case <-atLeastTimer.C:

								// 		default:
								// 		}
								// 	}
								// }
								// atLeastTimer = time.NewTimer(time.Duration(0.5 * float64(rawLat)))
							}
							dlog.Printf("mockCommitted %v arrives before realCommitted, starting atLeastTimer for 10ms\n", mockCommitted.OpId)
						}

					case realCommittedMessageTime := <-realCommittedChan:
						realCommittedMsg := realCommittedMessageTime.msg.(*genericsmrproto.RealCommitted)
						realCommitted := *realCommittedMsg
						realCommitId := realCommitted.OpId
						// log.Printf("Got RealCommitted, OpId %d, ghostCommitted received %d\n", realCommitId, ghostCommitMap[realCommitId].Received)
						if sendTimeReceived, exist := realCommitMap[realCommitId]; exist && !sendTimeReceived.Received {
							// log.Printf("First time receive realCommitted for command %d, receive time %v, sent time %v, latency %v\n",
							// realCommitId, realCommittedMessageTime.rcvingTime, realCommitMap[realCommitId].SentTime, realCommittedMessageTime.rcvingTime.Sub(realCommitMap[realCommitId].SentTime))
							realCommitMap[realCommitId].Received = true
							realCommitMap[realCommitId].Inst = realCommitted.Instance
							realCommitMap[realCommitId].Cmds = realCommitted.Commands
							// if atLeastTimer != nil {
							// 	if !atLeastTimer.Stop() {
							// 		select {
							// 		case <-atLeastTimer.C:

							// 		default:
							// 		}
							// 	}
							// }
							// to.Stop() // don't send at least messages if we received the real commit

							// if sendTimeReceived.doMock {
							// if !sendTimeReceived.doMock {
							// 	log.Printf("It's weird, this command %d is not set to ghost process\n", realCommitted.OpId)
							// }
							rawLat := realCommittedMessageTime.rcvingTime.Sub(realCommitMap[realCommitId].SentTime)
							lat := int64(rawLat / time.Microsecond)
							if realCommitId != realCommitted.OpId {
								log.Printf("Weird, realCommitId %d, realCommitted.OpId %d\n", realCommitId, realCommitted.OpId)
							}
							realCommitMap[realCommitId].CommitLatency = rawLat
							if lat < 0 {
								log.Printf("Weird, receive a real commit latency less than 0. Sent time %v, receive time %v, latency %v, int64 latency %d.\n",
									realCommitMap[realCommitId].SentTime, realCommittedMessageTime.rcvingTime, rawLat, lat)
							}

							if ghostCommitMap[realCommitId].Received {
								ghostLat := int64(ghostCommitMap[realCommitId].CommitLatency / time.Microsecond)
								CommitLatFeedback := genericsmrproto.CommitLatencyFeedback{CommandId: state.CommandId{ClientId: clientId, OpId: realCommitId},
									RealInstance: realCommitMap[realCommitId].Inst, GhostInstance: ghostCommitMap[realCommitId].Inst,
									RealCommitLatency: lat, GhostCommitLatency: ghostLat,
									RealInstCmds: realCommitMap[realCommitId].Cmds, GhostInstCmds: ghostCommitMap[realCommitId].Cmds}
								// log.Printf("Sending commit latency feedback: OpId %d, real commit latency %d, ghost commit latency %d\n", realCommitId, lat, ghostLat)
								// log.Printf("CommitLatFeedback to send %v\n", CommitLatFeedback)
								if CommitLatFeedback.RealCommitLatency < 0 || CommitLatFeedback.GhostCommitLatency < 0 {
									log.Printf("Weird, we have a commit latency less than 0, %v, sent time in real map %v, sent time in ghost map %v.\n", CommitLatFeedback, realCommitMap[realCommitId].SentTime, ghostCommitMap[realCommitId].SentTime)
									log.Printf("Ghost commit latency %v. real commit atency%v.\n", ghostCommitMap[realCommitId].CommitLatency, realCommitMap[realCommitId].CommitLatency)
								}

								for nodeid := 0; nodeid < N; nodeid++ {
									go func(nodeid int, CommitLatFeedback genericsmrproto.CommitLatencyFeedback) {
										writerLocks[nodeid].Lock()
										defer writerLocks[nodeid].Unlock()

										servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second))
										writers[nodeid].WriteByte(genericsmrproto.COMMIT_LATENCY_FEEDBACK)
										CommitLatFeedback.Marshal(writers[nodeid])
										writers[nodeid].Flush()
									}(nodeid, CommitLatFeedback)
								}
							} else {
								// log.Printf("When receiving realCommit for cmd %d, ghostCommit has not received.\n", realCommitId)
								if ENABLE_AT_LEAST_TIMER {
									timerDuration := 20 * time.Millisecond
									go atLeastTimerFunc(realCommitId, atLeastTimerCmdIdChan, timerDuration)
								}
								// timerDuration := time.Duration(0.7 * float64(rawLat))
								// if atLeastTimer != nil {
								// 	if !atLeastTimer.Stop() {
								// 		select {
								// 		case <-atLeastTimer.C:

								// 		default:
								// 		}
								// 	}
								// }
								// atLeastTimer = time.NewTimer(time.Duration(0.5 * float64(rawLat)))
							}
						}

					case <-to.C:
						// log.Printf("Client %v: TIMEOUT for request %v\n", clientId, id)
						log.Printf("Retransmitting with ghosting..., cmdId: %d\n", id)
						if succeeded {
							log.Panicf("Timed out but also succeeded?\n")
						}
						doMock = 1
						argsExecTime.CommandId = int32(doMock)
						for nodeid := 0; nodeid < N; nodeid++ {
							go func(nodeid int, argsExecTime genericsmrproto.ProposeWithExecTime) {
								writerLocks[nodeid].Lock()
								defer writerLocks[nodeid].Unlock()

								servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second)) // caution
								writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME)
								argsExecTime.Marshal(writers[nodeid])
								writers[nodeid].Flush() // caution, this is blocking
							}(nodeid, argsExecTime)
							// log.Printf("Writing to node after timing out once %v argsExecTime %v", nodeid, argsExecTime)
							// TODO This pattern is everywhere make function.
							// servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second))
							// writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME)
							// argsExecTime.Marshal(writers[nodeid])
							// writers[nodeid].Flush()
						}
						ghostCommitMap[id] = &commitLatencyTrack{SentTime: time.Now(), Received: false, DoGhost: true, Inst: -1, Cmds: nil, CommitLatency: -1}
						realCommitMap[id] = &commitLatencyTrack{SentTime: time.Now(), Received: false, DoGhost: true, Inst: -1, Cmds: nil, CommitLatency: -1}
						// if atLeastTimer != nil {
						// 	// 	dlog.Printf("TIME OUT after receiving mockCommitted before receiving reply, stopping atLeastTimer\n")
						// 	if !atLeastTimer.Stop() {
						// 		select {
						// 		case <-atLeastTimer.C:

						// 		default:
						// 		}
						// 	}
						// }
						// atLeastTimer = time.NewTimer(200 * time.Second)
						to.Reset(*request_timeout)

					// case <-atLeastTimer.C:
					case atLeastTimeoutCmd := <-atLeastTimerCmdIdChan:
						if ghostSendTimeReceived, exist := ghostCommitMap[atLeastTimeoutCmd]; exist && ghostSendTimeReceived.Received {
							if realSendTimeReceived, exist := realCommitMap[atLeastTimeoutCmd]; exist && !realSendTimeReceived.Received {
								log.Printf("Waiting for realCommitted atLeastTimer TIME OUT for %v\n", atLeastTimeoutCmd)
								realCommitAtLeastLat := int64(time.Since(before) / time.Microsecond)
								CommitLatFeedback := genericsmrproto.CommitLatencyFeedback{CommandId: state.CommandId{ClientId: clientId, OpId: atLeastTimeoutCmd},
									RealInstance: realCommitMap[atLeastTimeoutCmd].Inst, GhostInstance: ghostCommitMap[atLeastTimeoutCmd].Inst,
									RealCommitLatency: realCommitAtLeastLat, GhostCommitLatency: int64(ghostCommitMap[atLeastTimeoutCmd].CommitLatency / time.Microsecond),
									RealInstCmds: realCommitMap[atLeastTimeoutCmd].Cmds, GhostInstCmds: ghostCommitMap[atLeastTimeoutCmd].Cmds}

								for nodeid := 0; nodeid < N; nodeid++ {
									go func(nodeid int, CommitLatFeedback genericsmrproto.CommitLatencyFeedback) {
										writerLocks[nodeid].Lock()
										defer writerLocks[nodeid].Unlock()

										servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second))
										writers[nodeid].WriteByte(genericsmrproto.REAL_COMMIT_AT_LEAST)
										CommitLatFeedback.Marshal(writers[nodeid])
										writers[nodeid].Flush()
									}(nodeid, CommitLatFeedback)
								}
							} else {
								// log.Printf("AtLeastTimer times out, but we receive both real and ghost commit for command %d.\n", id)
							}
						}

						if realSendTimeReceived, exist := realCommitMap[atLeastTimeoutCmd]; exist && realSendTimeReceived.Received {
							if ghostSendTimeReceived, exist := ghostCommitMap[atLeastTimeoutCmd]; exist && !ghostSendTimeReceived.Received {
								log.Printf("Waiting for ghostCommitted atLeastTimer TIME OUT for %v\n", atLeastTimeoutCmd)
								ghostCommitAtLeast := int64(time.Since(before) / time.Microsecond)
								CommitLatFeedback := genericsmrproto.CommitLatencyFeedback{CommandId: state.CommandId{ClientId: clientId, OpId: atLeastTimeoutCmd},
									RealInstance: realCommitMap[atLeastTimeoutCmd].Inst, GhostInstance: ghostCommitMap[atLeastTimeoutCmd].Inst,
									RealCommitLatency: int64(realCommitMap[atLeastTimeoutCmd].CommitLatency / time.Microsecond), GhostCommitLatency: ghostCommitAtLeast,
									RealInstCmds: realCommitMap[atLeastTimeoutCmd].Cmds, GhostInstCmds: ghostCommitMap[atLeastTimeoutCmd].Cmds}
								for nodeid := 0; nodeid < N; nodeid++ {
									go func(nodeid int, CommitLatFeedback genericsmrproto.CommitLatencyFeedback) {
										writerLocks[nodeid].Lock()
										defer writerLocks[nodeid].Unlock()

										servers[nodeid].SetDeadline(time.Now().Add(90 * time.Second))
										writers[nodeid].WriteByte(genericsmrproto.GHOST_COMMIT_AT_LEAST)
										CommitLatFeedback.Marshal(writers[nodeid])
										writers[nodeid].Flush()
									}(nodeid, CommitLatFeedback)
								}
							} else {

							}
						}

					default:
					}

					if succeeded {
						if *check {
							rsp[id] = true
						}
						reqsCount++
						break
					} else if toFired {
						// this was incorrect for mr99rsm I think...
						// log.Printf("Checking if over max runtime (%v) when timing out: maxRuntim %v since %v maxRuntime Duration%v\n",
						// *maxRuntime, *maxRuntime, time.Since(before_total), time.Duration(*maxRuntime)*time.Second)
						dlog.Printf("Timeout fired id %v after sending REAL_COMMIT_AT_LEAST\n", id)
						if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
							rcvingTime = time.Now()
							dlog.Printf("Client %v over time waiting for request %v after timeout fired, breaking\n", clientId, id)
							overtime = true
						}
						break
					}
					// not hit because this loop is broken above, just a sanity check
					if toFired {
						panic("Processing response when timeout fired")
					}

					// if time.Since(lastPrint) > 2*time.Second {
					// 	lastPrint = time.Now()
					dlog.Printf("Client %v: request %v: sent at %v; reply from pilot: %v; batch %v;\n", clientId, id, before, fromPilot, batch)
					// }

					// break out of waiting loop if not succeeded and over time
					// we include a rcvingTime so that this request is included in
					// the latencies and trimmed properly.
					// log.Printf("Checking break succ %v toFired %v\n", succeeded, toFired)
					if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
						rcvingTime = time.Now()
						dlog.Printf("Client %v over time waiting for request %v, breaking\n", clientId, id)
						overtime = true
						break
					}

					// TODO what does this mean when this happens?
					if repliedCmdId >= 0 && repliedCmdId < id {
						// update latency if this response actually arrived ealier
						newLat := int64(rcvingTime.Sub(timestamps[repliedCmdId]) / time.Microsecond)
						if newLat < latencies[repliedCmdId] {
							// log.Panicf("Will this ever be triggered?\n")
							latencies[repliedCmdId] = newLat
						}
					}
				}
			} // successfully get the response. continue with the next request
		} else {
			if isRandomLeader { /*epaxos with random leader*/
				// leader = rand.Intn(N - 1)
				leader = i % N
			} else if *noLeader == false { /*MultiPaxos*/
				// leader = 0 why each request 0...
			}
			if leader >= 0 {
				writers[leader].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(writers[leader])
				writers[leader].Flush()
				// log.Printf("Command %d is write? %v", i, (args.Command.Op == state.PUT))
			}
			dlog.Printf("Client finished sent command %v\n", args)
			// TODO not sure if this works here
			if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
				rcvingTime = time.Now()
				dlog.Printf("Client %v over time waiting for request, breaking\n", clientId)
				overtime = true
				break
			}
			// timer and retransmit stuff for FVC
			to = time.NewTimer(*request_timeout) //REQUEST_TIMEOUT)
			err := false
			for true {
				if !succeeded && *maxRuntime >= 0 && time.Since(before_total) > time.Duration(*maxRuntime)*time.Second {
					rcvingTime = time.Now()
					dlog.Printf("Client %v over time waiting for request, breaking\n", clientId)
					overtime = true
					break
				}
				select {
				case e := <-leaderReplyChan:
					if e == id {
						rcvingTime = time.Now()
					}
					// drain concurrent replies if not replicaCmdId
					defaulted := false
					// log.Printf("0 e is %v\n", e)
					for e != id && !defaulted {
						select {
						case e = <-leaderReplyChan:
						default:
							defaulted = true
						}
						// log.Printf("1 e is %v\n", e)
					}
					// log.Printf("2 e is %v\n", e)
					if e == id {
						defaulted := false
						for !defaulted {
							select {
							case <-leaderReplyChan:
							default:
								defaulted = true
							}
						}
					}
					// log.Printf("3 e is %v\n", e)
					repliedCmdId = e
					if e == id {
						dlog.Printf("Received command reply %v\n", id)
						to.Stop()
					}
					// log.Printf("e is %v\n", e)
					if e == -1 {
						// log.Printf("Requesting a leader update\n")
						reply := new(masterproto.GetLeaderReply)
						master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
						if leader != reply.LeaderId { // changing leaders
							leader = reply.LeaderId
							// log.Printf("Got new leader for request %v. New leader is replica %v\n", id, leader)
							go waitReplies(readers, leader, *reqsNb-int(id), leaderReplyChan, *reqsNb-int(id))
							writers[leader].WriteByte(genericsmrproto.PROPOSE)
							args.Marshal(writers[leader])
							writers[leader].Flush()
							to.Reset(*request_timeout) //REQUEST_TIMEOUT)
						} else {
							// if no new leader, but received a not leader reply, still need to retransmit
							// log.Printf("No new leader retransmitting\n")
							writers[leader].WriteByte(genericsmrproto.PROPOSE)
							args.Marshal(writers[leader])
							writers[leader].Flush()
							to.Reset(*request_timeout) //REQUEST_TIMEOUT)
						}
						err = false
						succeeded = false // was ok?
						rcvingTime = time.Time{}
						continue
					}
					break
				case <-to.C:
					repliedCmdId = id
					rcvingTime = time.Now()
					err = true
					break
				default:
				}
				if (err || repliedCmdId == int32(-1)) && rcvingTime != (time.Time{}) {
					log.Printf("Timed out on request %v\n", id)
					reply := new(masterproto.GetLeaderReply)
					master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
					if leader != reply.LeaderId { // changing leaders
						leader = reply.LeaderId
						// log.Printf("Got new leader for request %v. New leader is replica %v\n", id, leader)
						// go waitReplies(readers, leader, *reqsNb-int(id), leaderReplyChan, *reqsNb-int(id))
						writers[leader].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader])
						writers[leader].Flush()
						to.Reset(*request_timeout) //REQUEST_TIMEOUT)
						err = false
						succeeded = false // was ok?
						rcvingTime = time.Time{}
						continue
					} else {
						// log.Printf("No new leader for request %v; waiting longer\n", id)
						args.Marshal(writers[leader])
						writers[leader].Flush()
						to.Reset(*request_timeout) //REQUEST_TIMEOUT)
						err = false
						succeeded = false
						rcvingTime = time.Time{}
						repliedCmdId = int32(-1)
						continue
					}
				}

				if repliedCmdId == id && !err && rcvingTime != (time.Time{}) {
					if *check {
						rsp[id] = true
					}
					reqsCount++
					succeeded = true
					break
				}
			}
		}

		// Request latency
		lat := int64(rcvingTime.Sub(before) / time.Microsecond)
		lastLatency = lat
		// lastExecTime = execTimeFromMessage

		dlog.Printf("lat %v id %v succeeded %v\n", lat, id, succeeded)

		if !succeeded {
			// log.Printf("Unsuccessful request %v outside of loop\n", id)
		}
		// log.Printf("Setting lastLatency to %v for %v %v\n", lastLatency, clientId, id)
		latencies = append(latencies, lat)
		if put[i] {
			writelatencies = append(writelatencies, lat)
		} else {
			readlatencies = append(readlatencies, lat)
		}

		if PRINT_STATS && lat >= 0 { //330000 { //10000 {
			if GC_DEBUG_ENABLED {
				var garC debug.GCStats
				debug.ReadGCStats(&garC)
				dlog.Printf("NumGC: %v; PauseTotal: %v; Pause: %v; LastGC: %v\n", garC.NumGC, garC.PauseTotal, garC.Pause, garC.LastGC)
			}
			// log.Printf("Client %v: request %v: sent at %v; reply from pilot: %v; batch %v; latency: %v\n", clientId, id, before, fromPilot, batch, lat)
		} else if PRINT_STATS && time.Since(lastPrint) > 2*time.Second {
			lastPrint = time.Now()
			// log.Printf("Client %v: request %v: sent at %v; reply from pilot: %v; batch %v; latency: %v\n", clientId, id, before, fromPilot, batch, lat)
		}

		currentTime := time.Now()
		// Throughput every interval
		if currentTime.Sub(lastThroughputTime) >= tput_interval_in_sec {
			p := DataPoint{elapse: currentTime.Sub(before_total), reqsCount: reqsCount, t: currentTime}
			throughputs = append(throughputs, p)

			if *verbose && readings != nil {
				readings <- &p
			}
			lastThroughputTime = currentTime
		}

		if *maxRuntime >= 0 && currentTime.Sub(before_total) > time.Duration(*maxRuntime)*time.Second {
			log.Printf("Client %v over time, breaking\n", clientId)
			break
		}
	}
	if *verbose && readings != nil {
		close(readings)
	}
	//fmt.Println(latencies)

	//after_total := time.Now()

	// s := 5 * time.Second
	s := ((clientId % 28) + 1) * 500 * uint32(time.Millisecond)
	log.Printf("Sleeping before writing files for %vms\n", s/uint32(time.Millisecond))
	time.Sleep(time.Duration(rng.Int63n(int64(s))))
	//totalTimeInSec := float64(time.Since(before_total) / time.Second)
	//log.Printf("Runtime: %v seconds\n", totalTimeInSec)
	totalRuntime := time.Since(before_total)
	fmt.Println("=========================")
	log.Printf("Runtime: %v \n", totalRuntime)
	log.Printf("Total requests: %d\n", reqsCount)
	log.Printf("Overall average throughput: %v (reqs/sec)\n", uint64(float64(reqsCount)*float64(time.Second)/float64(totalRuntime)))

	if *check {
		for j := int64(0); j < reqsCount; j++ {
			if !rsp[j] {
				fmt.Println("Didn't receive", j)
			}
		}
	}

	// Output latencies and throughput
	writeDataToFiles()
	// GC
	//debug.SetGCPercent(*garPercent)
	//debug.PrintStack()

	if GC_DEBUG_ENABLED {
		var garC debug.GCStats
		debug.ReadGCStats(&garC)
		log.Printf("\nLastGC:\t%s", garC.LastGC)         // time of last collection
		log.Printf("\nNumGC:\t%d", garC.NumGC)           // number of garbage collections
		log.Printf("\nPauseTotal:\t%s", garC.PauseTotal) // total pause for all collections
		log.Printf("\nPause:\t%s", garC.Pause)           // pause history, most recent first
	}

	///* Output latencies */
	//writeLatenciesToFile(latencies, "")
	///* Output throughputs */
	//processAndPrintThroughputs(throughputs)

	time.Sleep(1 * time.Second)

	// Clean up
	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	if *verbose && done != nil {
		<-done
	}
	master.Close()
}

func pingAndSendRttTable(writers []*bufio.Writer, pilot0ReplyChan chan Response) {
	// log.Printf("Starting to ping\n")
	replyChs := make([]chan Response, N)
	nReplies := make([]int, N)
	rtts := make([][]float64, N)
	nPings := NPINGS
	nToDiscard := NPINGS_TO_DISCARD
	sendTime := make([]time.Time, N)
	nTotalReplies := 0
	for i := 0; i < N; i++ {
		replyChs[i] = make(chan Response, 3)
		nReplies[i] = 0
		rtts[i] = make([]float64, 0)
		// rtts[i] = int64(^uint64(0) >> 1)
	}

	prop := genericsmrproto.ProposeWithExecTime{CommandId: 0,
		Command:         state.Command{ClientId: clientId, OpId: -1, Op: state.NONE, K: 0, V: 0},
		EndToEndLatency: genericsmrproto.EndToEndLatency_{Latency: -1, CommandId: -1}}
	for nodeid := 0; nodeid < N; nodeid++ {
		sendTime[nodeid] = time.Now()
		writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME)
		prop.Marshal(writers[nodeid])
		writers[nodeid].Flush()
	}
	for nTotalReplies < N*nPings {
		reply := <-pilot0ReplyChan
		nodeid := int(math.Abs(float64(reply.OpId)))
		lat := float64(reply.rcvingTime.Sub(sendTime[nodeid]).Microseconds())
		// log.Printf("lat is %v\n", lat)
		if nReplies[nodeid]+1 > nToDiscard {
			dlog.Printf("discarding %vth reply lat: %v\n", nReplies[nodeid]+1, lat)
			rtts[nodeid] = append(rtts[nodeid], lat)
		}
		// if lat < rtts[nodeid] {
		// 	rtts[nodeid] = lat
		// }

		nReplies[nodeid]++
		nTotalReplies++
		if nReplies[nodeid] < nPings {
			sendTime[nodeid] = time.Now()
			if err := writers[nodeid].WriteByte(genericsmrproto.PROPOSE_WITH_EXEC_TIME); err != nil {
				panic(fmt.Sprintf("Error writing ping %s", err))
			}
			prop.Marshal(writers[nodeid])
			writers[nodeid].Flush()
		}
	}
	// calculate
	mins := make([]int64, N)
	for i := range mins {
		// rtts[i] = rtts[i][50:]
		rep, _ := stats.Mean(rtts[i])
		stdDev, _ := stats.StandardDeviation(rtts[i])
		mins[i] = int64(rep + N_STANDARD_DEVIATIONS*stdDev)
		log.Printf("Client RTTs: rep %v  stdde %v all %v\n", rep, stdDev, mins)
	}
	// send
	for nodeid := 0; nodeid < N; nodeid++ {
		writers[nodeid].WriteByte(genericsmrproto.CLIENT_RTT_TABLE)
		crt := genericsmrproto.ClientRttTable{ClientId: clientId, Rtts: mins} // rtts is rtts to all
		crt.Marshal(writers[nodeid])
		writers[nodeid].Flush()

	}
	dlog.Printf("Client %v RTTs to nodes: %v\n", clientId, rtts)
}

func startLogicalClient() {

}

func waitReplies(readers []*bufio.Reader, leader int, n int, done chan int32, expected int) {
	var msgType byte
	var err error
	reply := new(genericsmrproto.ProposeReplyTS)

	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			// log.Printf("Got a reply: %v\n", reply)
			if reply.OK != 0 {
				successful[leader]++
				//done <- &Response{OpId: reply.CommandId, rcvingTime: time.Now()}
				done <- reply.CommandId
				if expected == successful[leader] {
					return
				}
			} else {
				done <- -1 // cch added
			}
			break
		default:
			break
		}
	}
}

func waitRepliesEpaxos(readers []*bufio.Reader, replica int, done chan int32) {
	var msgType byte
	var err error
	reply := new(genericsmrproto.ProposeReplyTS)

	for true {
		if msgType, err = readers[replica].ReadByte(); err != nil {
			log.Panicf("Error reading msg from replica %d.\n", replica)
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[replica]); err != nil {
				continue
			}
			if reply.OK != 0 {
				done <- reply.CommandId
			}
			break
		default:
			break
		}
	}
}

// waitRepliesRandomLeader is used by EPaxos with no specified proxy and waits for replies
// from any of the n bufio.Reader in readers and puts the unmarshaled message into done.
// TODO n is unnecessary, len(readers) should be used.
func waitRepliesRandomLeader(readers []*bufio.Reader, n int, done chan int32) {
	var msgType byte
	var err error
	reply := new(genericsmrproto.ProposeReplyTS)

	for true {
		for i := 0; i < n; i++ {
			if msgType, err = readers[i].ReadByte(); err != nil {
				continue
			}

			switch msgType {
			case genericsmrproto.PROPOSE_REPLY:
				if err = reply.Unmarshal(readers[i]); err != nil {
					continue
				}
				if reply.OK != 0 {
					successful[i]++
					//done <- &Response{OpId: reply.CommandId, rcvingTime: time.Now()}
					done <- reply.CommandId
				}
				break
			default:
				break
			}
		}
	}
}

func waitRepliesPilot(readers []*bufio.Reader, leader int, done chan Response, viewChangeChan chan *View, expected int) {
	dlog.Println("Starting waitRepliesPilot thread for node ", leader)
	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTS)
	getViewReply := new(genericsmrproto.GetViewReply)
	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}
		// dlog.Println("Replies thread %d got response msgType %d", msgType)

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if reply.OK != 0 {
				// dlog.Println("OK ", leader)
				successful[leader]++
				done <- Response{reply.CommandId, time.Now(), reply.Timestamp}
				if expected == successful[leader] {
					// dlog.Println("returning ", leader)
					return
				} else {
					// dlog.Println("not returning ", leader)
				}
			}
			break

		case genericsmrproto.GET_VIEW_REPLY:
			if err = getViewReply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if getViewReply.OK != 0 { /*View is active*/
				viewChangeChan <- &View{getViewReply.ViewId, getViewReply.PilotId, getViewReply.ReplicaId, true}
			}
			break

		default:
			break
		}
	}
}

// waitRepliesReplica is used by mr99rsm to wait for a reply from a specified replica.
// this has too many arguments
func waitRepliesReplica(readers []*bufio.Reader, leader int, done chan Response, viewChangeChan chan *View,
	mockCommittedChan chan MessageTime, realCommittedChan chan MessageTime, execTimeChan chan genericsmrproto.MockExecTime_, expected int) {
	dlog.Println("Starting waitRepliesReplica thread for node ", leader)
	dlog.Println("CLIENTPINGREPLY %v CLIENTMOCKREPLY %v\n", avicennaproto.CLIENTPINGREPLY, avicennaproto.CLIENTMOCKREPLY)
	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTSMock)
	getViewReply := new(genericsmrproto.GetViewReply)
	// realCommitted := new(genericsmrproto.RealCommitted)
	// mockCommitted := new(genericsmrproto.MockCommitted)
	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}
		// dlog.Println("Replies thread %d got response msgType %d", msgType)

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			// log.Printf("Got reply OK is %v\n", reply.OK)
			// TODO figure out what to do with this
			if reply.OK == avicennaproto.CLIENTMOCKREPLY {
				// no Mock replies anymore
				// mockReplyChan <- Response{reply.CommandId, time.Now(), reply.Timestamp}
				// log.Printf("Client got a MockReply!\n")
			} else if reply.OK != 0 {
				successful[leader]++
				done <- Response{reply.CommandId, time.Now(), reply.Timestamp}

				// surgMock <- reply.MockInstruct
				if expected == successful[leader] {
					// dlog.Println("returning successful %v expected %v", successful, expected)
					return
				} else {
					// dlog.Println("not returning ", leader)
				}
			} else if reply.OK == avicennaproto.CLIENTPINGREPLY {
				done <- Response{reply.CommandId, time.Now(), reply.Timestamp}
			}
			break
		case genericsmrproto.REAL_COMMITTED:
			rc := new(genericsmrproto.RealCommitted)
			rc.Unmarshal(readers[leader])
			realCommittedChan <- MessageTime{time.Now(), rc}
			break
		case genericsmrproto.MOCK_COMMITTED:
			gc := new(genericsmrproto.MockCommitted)
			gc.Unmarshal(readers[leader])
			// log.Printf("Got MOCK_COMMITTED %v from replica %v\n", mockCommitted, leader)
			mockCommittedChan <- MessageTime{time.Now(), gc}
			break
		case genericsmrproto.GET_VIEW_REPLY:
			if err = getViewReply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if getViewReply.OK != 0 { /*View is active*/
				viewChangeChan <- &View{getViewReply.ViewId, getViewReply.PilotId, getViewReply.ReplicaId, true}
			}
			break

		default:
			break
		}
	}
}

func waitPingReplies(readers []*bufio.Reader, replica int, done chan Response) {
	dlog.Println("Starting waitPingReplies thread for node ", replica)
	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTS)
	for {
		if msgType, err = readers[replica].ReadByte(); err != nil {
			break
		}
		dlog.Println("Replies thread %d got response msgType %d", msgType)

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[replica]); err != nil {
				break
			}
			if reply.OK != 0 {
				dlog.Println("OK ", replica)
				successful[replica]++
				select {
				case done <- Response{reply.CommandId, time.Now(), reply.Timestamp}:
				default:
					dlog.Printf("PingReply thread for replica %v done listening\n", replica)
					return
				}
			}
			break
		default:
			break
		}

	}
}

func waitRegisterClientIdReplies(readers []*bufio.Reader, n int) int {

	if n > len(readers) {
		return -1
	}

	success := 0
	reply := new(genericsmrproto.RegisterClientIdReply)
	for i := 0; i < n; i++ {
		i := 0
		//for success < n {
		if err := reply.Unmarshal(readers[i]); err != nil {
			fmt.Println("Error when reading RegisterClientIdReply from replica", i, ":", err)
			i = (i + 1) % n
			continue
		}
		if reply.OK != 0 {
			success++
		}

		i = (i + 1) % n
	}

	return success

}

func generateRandomClientId() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Uint32()
}

func generateRandomOpId() int32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Int31()
}

func printer(dataChan chan *DataPoint, done chan bool) {
	for {
		reading, more := <-dataChan
		if !more {
			if done != nil {
				done <- true
			}
			return
		}
		log.Printf("%.1f\t%d\t%.0f\n", float64(reading.elapse)/float64(time.Second), reading.reqsCount, float64(reading.reqsCount)*float64(time.Second)/float64(reading.elapse))
	}

}

/* Trim and sort the latencies */
func processLatencies(latencies []int64) []int64 {

	if len(latencies) <= 0 {
		return latencies
	}
	trimLength := int(float64(len(latencies)) * *trim)
	latencies = latencies[trimLength : len(latencies)-trimLength]
	sort.Sort(int64Slice(latencies)) // this should just use sort.Slice with a lambda

	return latencies
}

func getLatencyPercentiles(latencies []int64, shouldTrim bool) []int64 {
	if shouldTrim {
		latencies = processLatencies(latencies)
	}

	percentiles := make([]int64, 0, 100)
	l := len(latencies)
	if l == 0 {
		return percentiles
	}

	for i := 1; i < 100; i++ {
		idx := int(float64(l) * float64(i) / 100.0)
		percentiles = append(percentiles, latencies[idx])
	}
	// add 99.9 percentile
	percentiles = append(percentiles, latencies[int(float64(l)*0.999)])
	return percentiles
}

func processAndPrintThroughputs(throughputs []DataPoint) (error, string) {
	var overallTput string = "NaN"
	var instTput string = "NaN"

	filename := fmt.Sprintf("client-%d.throughput.txt", clientId)
	filepath := filepath2.Join(*prefix, filename)
	f, err := os.Create(filepath)

	if err != nil {
		return err, overallTput
	}

	defer f.Close()

	for i, p := range throughputs {
		overallTput = "NaN"
		instTput = "NaN"
		if p.elapse > time.Duration(0) {
			overallTput = strconv.FormatInt(int64(float64(p.reqsCount)*float64(time.Second)/float64(p.elapse)), 10)

		}

		if i == 0 {
			instTput = strconv.FormatInt(p.reqsCount, 10)
		} else if p.elapse > throughputs[i-1].elapse {
			instTput = strconv.FormatInt(int64(
				float64(p.reqsCount-throughputs[i-1].reqsCount)*float64(time.Second)/
					float64(p.elapse-throughputs[i-1].elapse)), 10)
		}
		line := fmt.Sprintf("%.1f\t%d\t%v\t%v\t%.1f\n", float64(p.elapse)/float64(time.Second), p.reqsCount, overallTput, instTput, float64(p.t.UnixNano())*float64(time.Nanosecond)/float64(time.Second))
		_, err = f.WriteString(line)
		log.Printf(line)
	}

	// Trimming
	trimmedOverallTput := "NaN"
	trimLength := int(float64(len(throughputs)) * *trim)
	throughputs = throughputs[trimLength : len(throughputs)-trimLength]
	newlen := len(throughputs)
	if newlen == 1 {
		trimmedOverallTput = strconv.FormatInt(int64(
			float64(throughputs[0].reqsCount)*float64(time.Second)/float64(throughputs[0].elapse)), 10)
	} else if newlen > 1 && throughputs[newlen-1].elapse > throughputs[0].elapse {
		trimmedOverallTput = strconv.FormatInt(int64(
			float64(throughputs[newlen-1].reqsCount-throughputs[0].reqsCount)*float64(time.Second)/
				float64(throughputs[newlen-1].elapse-throughputs[0].elapse)), 10)
	}

	log.Printf("%s\n", overallTput)
	log.Printf("%s\n", trimmedOverallTput)

	_, err = f.WriteString(fmt.Sprintf("%s\n", overallTput))
	_, err = f.WriteString(fmt.Sprintf("%s\n", trimmedOverallTput))

	f.Sync()

	return err, trimmedOverallTput

}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	//fmt.Println(processLatencies(latencies))
	writeLatenciesToFile(latencies, "")
	// dlog.Printf("Caught signal and stopped CPU profile before exit.\n")
	log.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}

/* Helper functions to write to file */
func checkError(e error) {
	if e != nil {
		panic(e)
	}
}

func writeLatenciesToFile(latencies []int64, latencyType string) {

	// trimmedLatencies: trimmed and sorted
	trimmedLatencies := processLatencies(latencies)
	filename := fmt.Sprintf("client-%d.%slatency.all.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, trimmedLatencies)

	filename = fmt.Sprintf("client-%d.%slatency.percentiles.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, getLatencyPercentiles(trimmedLatencies, false))
}

// return the percentiles
func writeLatenciesToFile2(latencies []int64, latencyType string) []int64 {

	// original latencies
	filename := fmt.Sprintf("client-%d.%slatency.orig.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	// writeSliceToFile(filepath, latencies)

	// trimmedLatencies: trimmed and sorted
	trimmedLatencies := processLatencies(latencies)
	filename = fmt.Sprintf("client-%d.%slatency.all.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	writeSliceToFile(filepath, trimmedLatencies)

	percentiles := getLatencyPercentiles(trimmedLatencies, false)
	filename = fmt.Sprintf("client-%d.%slatency.percentiles.txt", clientId, latencyType)
	filepath = filepath2.Join(*prefix, filename)
	// writeSliceToFile(filepath, percentiles)

	return percentiles
}

func writeThroughputLatency(throughput string, latencies []int64, latencyType string) error {

	if len(latencies) < 100 {
		return nil
	}

	filename := fmt.Sprintf("client-%d.tput%slat.txt", clientId, latencyType)
	filepath := filepath2.Join(*prefix, filename)
	f, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer f.Close()

	text := fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", throughput, latencies[0], latencies[24],
		latencies[49], latencies[74], latencies[89], latencies[94], latencies[98], latencies[99])
	_, err = f.WriteString(text)

	if err != nil {
		return err
	}

	return f.Sync()
}

func writeSliceToFile(filename string, arr []int64) error {
	f, err := os.Create(filename)

	if err != nil {
		return err
	}

	defer f.Close()
	//w := bufio.NewWriter(f)

	for _, val := range arr {

		//_, err := w.WriteString(string(val) + "\n")
		text := fmt.Sprintf("%v\n", val)
		_, err := f.WriteString(text)
		//_, err := io.WriteString(f,  text)

		if err != nil {
			return err
		}
	}
	//w.Flush()
	return f.Sync()

}

func writeTimestampsToFile(arr []time.Time, latencies []int64) error {

	filename := fmt.Sprintf("client-%d.timestamps.orig.txt", clientId)
	filepath := filepath2.Join(*prefix, filename)

	f, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer f.Close()

	var n int
	if len(arr) < len(latencies) {
		n = len(arr)
	} else {
		n = len(latencies)
	}
	for i := 0; i < n; i++ {

		val := arr[i]
		text := fmt.Sprintf("%02d:%02d:%02d.%v\t%v\n", val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), latencies[i])
		_, err := f.WriteString(text)

		if err != nil {
			return err
		}
	}

	return f.Sync()
}

func writeUnixTimestampsToFile(arr []time.Time, latencies []int64) error {

	filename := fmt.Sprintf("client-%d.unixoffsets.orig.txt", clientId)
	filepath := filepath2.Join(*prefix, filename)

	f, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer f.Close()

	var n int
	if len(arr) < len(latencies) {
		n = len(arr)
	} else {
		n = len(latencies)
	}

	// for offsets
	// get min time
	// minTime := arr[0]
	// for _, t := range arr {
	// 	if t.Before(minTime) {
	// 		minTime = t
	// 	}
	// }
	// minUnixTime := minTime.UnixMicro()

	for i := 0; i < n; i++ {
		val := arr[i]
		// text := fmt.Sprintf("%v\t%v\n", val.UnixMicro()-minUnixTime, latencies[i])
		text := fmt.Sprintf("%v\t%v\n", val.UnixMicro(), latencies[i])
		_, err := f.WriteString(text)

		if err != nil {
			return err
		}
	}
	return f.Sync()
}

func shutdownHook(c chan os.Signal) {
	sig := <-c
	log.Printf("I've got killed by signal %s! Cleaning up...", sig)

	///* Output latencies */
	//writeLatenciesToFile(latencies, "")
	//
	///* Output throughputs */
	//processAndPrintThroughputs(throughputs)
	writeDataToFiles()
	os.Exit(1)
}

var mu sync.Mutex

func writeDataToFiles() {
	// hook might try to run this at the same time?
	mu.Lock()
	defer mu.Unlock()

	/* Output timestamp */
	writeTimestampsToFile(timestamps, latencies)

	// MR99RSM VERSION
	/* Output unix timestamps */
	writeUnixTimestampsToFile(timestamps, latencies)

	/* Output throughputs */
	_, throughput := processAndPrintThroughputs(throughputs)

	/* Output latencies */
	percentiles := writeLatenciesToFile2(latencies, "")
	writeThroughputLatency(throughput, percentiles, "")

	/* Output read/write latencies */
	// writeLatenciesToFile2(readlatencies, "read")
	// writeLatenciesToFile2(writelatencies, "write")

}

/* Helper interface for sorting int64 */
type int64Slice []int64

func (arr int64Slice) Len() int {
	return len(arr)
}

func (arr int64Slice) Less(i, j int) bool {
	return arr[i] < arr[j]
}

func (arr int64Slice) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}
