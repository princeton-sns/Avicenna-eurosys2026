package genericsmr

import (
	"bufio"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"net"
	"os"
	"rdtsc"
	"runtime"
	"slowdowntimers"
	"state"
	"strings"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 200000

const INJECT_TRANSIENT_SLOWDOWN = false
const INJECT_LONGLIVED_SLOWDOWN = true
const INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT = false
const INJECT_NIC_SLOWDOWN = false

type SerializableWithRecvTime struct {
	Obj      fastrpc.Serializable
	RecvTime time.Time
}

type RPCPairWithRecvTime struct {
	Obj              fastrpc.Serializable
	ChanWithRecvTime chan SerializableWithRecvTime // with recvtime is one change
}

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*genericsmrproto.Propose
	Reply *bufio.Writer
}

type ProposeWithExecTime struct {
	*genericsmrproto.ProposeWithExecTime
	Reply *bufio.Writer
}

type Beacon struct {
	Rid       int32
	Timestamp uint64
}

type Client struct {
	*genericsmrproto.RegisterClientIdArgs
	Reply *bufio.Writer
}

type GetView struct {
	*genericsmrproto.GetView
	Reply *bufio.Writer
}

type ClientRttTable struct {
	*genericsmrproto.ClientRttTable
	Reply *bufio.Writer
}

var slowdownTimers *slowdowntimers.SlowdownTimers

type Replica struct {
	N            int        // total number of replicas
	Id           int32      // the ID of the current replica
	PeerAddrList []string   // array with the IP:port address of every replica
	Peers        []net.Conn // cache of connections to all other replicas
	PeerReaders  []*bufio.Reader
	PeerWriters  []*bufio.Writer
	Alive        []bool // connection status
	Listener     net.Listener

	State *state.State

	ProposeChan             chan *Propose             // channel for client proposals
	ProposeWithExecTimeChan chan *ProposeWithExecTime // channel for client proposals for mr99rsm
	BeaconChan              chan *Beacon              // channel for beacons from peer replicas

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable             map[uint8]*RPCPair
	rpcTableWithRecvTime map[uint8]*RPCPairWithRecvTime
	rpcCode              uint8
	recvCallbacks        map[uint8]RecvCallbackArg

	Ewma []float64

	OnClientConnect chan bool

	RegisterClientIdChan chan *Client // channel for registering client id

	GetViewChan                     chan *GetView
	ClientRttChan                   chan *ClientRttTable
	RealCommitAtLeastFromClientChan chan *genericsmrproto.CommitLatencyFeedback
	MockCommitAtLeastFromClientChan chan *genericsmrproto.CommitLatencyFeedback
	CommitLatencyFeedBackChan       chan *genericsmrproto.CommitLatencyFeedback
	IsSlowdownReplica               bool
	// TimesToSlowdown             []time.Time
	TimeToSlowdown   time.Time
	SlowdownDuration time.Duration
	PingTimers       *slowdowntimers.SlowdownTimers
	ClientWriteLock  []sync.Mutex
	MsgParseLatChan  chan *time.Duration
}

type RecvCallback func(interface{}, fastrpc.Serializable)
type RecvCallbackArg struct {
	Cb  RecvCallback
	Arg interface{}
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool) *Replica {
	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		make([]net.Conn, len(peerAddrList)),
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *ProposeWithExecTime, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		dreply,
		false,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		make(map[uint8]*RPCPairWithRecvTime),
		genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1,
		make(map[uint8]RecvCallbackArg),
		make([]float64, len(peerAddrList)),
		make(chan bool, 1200),
		make(chan *Client, CHAN_BUFFER_SIZE),
		make(chan *GetView, CHAN_BUFFER_SIZE),
		make(chan *ClientRttTable, CHAN_BUFFER_SIZE),
		make(chan *genericsmrproto.CommitLatencyFeedback, CHAN_BUFFER_SIZE),
		make(chan *genericsmrproto.CommitLatencyFeedback, CHAN_BUFFER_SIZE),
		make(chan *genericsmrproto.CommitLatencyFeedback, CHAN_BUFFER_SIZE),
		false,
		// make([]time.Time, 0),
		time.Time{},
		0, // time.Duration
		&slowdowntimers.SlowdownTimers{},
		make([]sync.Mutex, 2000),
		make(chan *time.Duration, CHAN_BUFFER_SIZE),
	}

	slowdownTimers = &slowdowntimers.SlowdownTimers{}
	var err error

	if r.StableStore, err = os.Create(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
	}

	// Setup times to slowdown
	cur := time.Now()
	// r.TimeToSlowdown = cur.Add(65 * time.Second)
	// r.TimeToSlowdown = cur.Add(47 * time.Second)
	if INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT {
		r.TimeToSlowdown = cur.Add(33 * time.Second)
	} else {
		r.TimeToSlowdown = cur.Add(48 * time.Second)
	}

	return r
}

// func (r *Replica) StartSlowdownInjectionTimers() {
// 	log.Printf("Starting timers\n")
// 	for k, t := range r.TimesToSlowdown {
// 		severity := 10 * time.Millisecond
// 		d := t.Sub(time.Now())
// 		if d > 0 {
// 			slowdowntimers.InjectSlowdownIn(d, severity, k, r.Id)
// 		} else {
// 			log.Printf("Missed a slowdown time! %v now %v\n", t, time.Now())
// 		}
// 	}
// }

/* Client API */

func (r *Replica) Ping(args *genericsmrproto.PingArgs, reply *genericsmrproto.PingReply) error {
	// log.Printf("Handling Ping from master\n")

	return nil
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	return nil
}

func (r *Replica) BeTheLeader2(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	return nil
}

/* ============= */

func (r *Replica) ConnectToPeers() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
				if tcp, ok := conn.(*net.TCPConn); ok {
					tcp.SetNoDelay(true) // ← important for latency
					tcp.SetKeepAlive(true)
					tcp.SetKeepAlivePeriod(30 * time.Second)
					tcp.SetWriteBuffer(1 << 20)
				}
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		log.Printf("Writing id %v as %v\n", r.Id, bs)
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) ConnectToPeersNoListeners() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	var err0 error
	port := strings.Split(r.PeerAddrList[r.Id], ":")[1]
	r.Listener, err0 = net.Listen("tcp", fmt.Sprintf(":%s", port)) //r.PeerAddrList[r.Id])
	if err0 != nil {
		panic(fmt.Sprintf("Listen error id %v peerAddrList %v error %v\n", r.Id, r.PeerAddrList[r.Id], err0))
	}

	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Printf("Accept error replica %v: %v", i, err)
			continue
		}
		log.Printf("conn %v err %v\n", conn, err)
		if _, err := io.ReadFull(conn, bs); err != nil {
			log.Printf("Connection establish error replica %v: %v", i, err)
			// fmt.Println("Connection establish error:", err)
			continue
		} else {
			log.Printf("Connected to replica %v bs %v\n", i, bs)
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		log.Printf("Got id %v bs %v\n", id, bs)
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
	}

	done <- true
}

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}

		log.Printf("Got a client connection\n")
		go r.clientListener(conn)

		r.OnClientConnect <- true
	}
}

// cch: All messages from other replicas go here?
// cch: there is one go replicaListener() for every replica
func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply

	// slowdownTimers := &slowdowntimers.SlowdownTimers{}
	i := 0
	for err == nil && !r.Shutdown {
		i++

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		// if r.IsSlowdownReplica {
		// 	if INJECT_TRANSIENT_SLOWDOWN {
		// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
		// 		slowdownTimers.CheckAndDoSlowdown()
		// 	} else if INJECT_LONGLIVED_SLOWDOWN && INJECT_NIC_SLOWDOWN {
		// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
		// 		slowdownTimers.CheckAndDoLongLivedSlowdown()
		// 	}
		// }
		// beforeParsing := time.Now()

		switch uint8(msgType) {

		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon
			break

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			//TODO: UPDATE STUFF
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(rdtsc.Cputicks()-gbeaconReply.Timestamp)
			log.Println(r.Ewma)
			break

		default:
			// beforeParsing := time.Now()
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}

				// cch: check if there is a callback defined and call it
				// one extra check
				if cb_arg, exists := r.recvCallbacks[msgType]; exists {
					cb_arg.Cb(cb_arg.Arg, obj)
				}

				rpair.Chan <- obj
			} else if rpair, present := r.rpcTableWithRecvTime[msgType]; present {
				swrt := SerializableWithRecvTime{rpair.Obj.New(), time.Now()}
				if err = swrt.Obj.Unmarshal(reader); err != nil {
					break
				}
				// unmarshalDuration := time.Now().Sub(swrt.RecvTime)
				// cch: check if there is a callback defined and call it
				if cb_arg, exists := r.recvCallbacks[msgType]; exists {
					cb_arg.Cb(cb_arg.Arg, swrt.Obj)
				}
				rpair.ChanWithRecvTime <- swrt
				// log.Printf("[LISTENER] Finished pushing message from %d, received at %v, unmarshal time %v\n", rid, swrt.RecvTime, unmarshalDuration)
			} else {
				log.Println("Error: received unknown message type, msgType: ", msgType)
			}
			// parseLat := time.Since(beforeParsing)
			// r.MsgParseLatChan <- &parseLat
		}
		// parseLat := time.Since(beforeParsing)
		// r.MsgParseLatChan <- &parseLat
	}
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error

	// slowdownTimers := &slowdowntimers.SlowdownTimers{}
	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}
		// if r.IsSlowdownReplica {
		// 	// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
		// 	if INJECT_TRANSIENT_SLOWDOWN {
		// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
		// 		slowdownTimers.CheckAndDoSlowdown()
		// 	} else if (INJECT_LONGLIVED_SLOWDOWN && INJECT_NIC_SLOWDOWN) || INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT {
		// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
		// 		slowdownTimers.CheckAndDoLongLivedSlowdown()
		// 	}
		// }
		// cch I think the slowdown should be injected here...
		// log.Printf("Time waiting for a byte: %v\n", time.Since(now))

		switch uint8(msgType) {

		case genericsmrproto.PROPOSE:
			// var afterParsing time.Duration
			// beforeParsing := time.Now()
			prop := new(genericsmrproto.Propose)
			if err = prop.Unmarshal(reader); err != nil {
				// afterParsing = time.Since(beforeParsing)
				break
			}
			//if (time.Now().UnixNano() - prop.Timestamp) >= int64(5000000) /*5ms*/ {
			//		fmt.Printf("Replica %v: clientListener: request %v-%v takes %v (us)\n", r.Id, prop.Command.ClientId, prop.CommandId, (time.Now().UnixNano()-prop.Timestamp)/int64(1000))

			//}

			// log.Printf("genericsmr got propose %v\n", prop)
			r.ProposeChan <- &Propose{prop, writer}
			// afterParsing = time.Since(beforeParsing)
			// r.MsgParseLatChan <- &afterParsing
			break
		case genericsmrproto.PROPOSE_WITH_EXEC_TIME:
			// var afterParsing time.Duration
			// beforeParsing := time.Now()
			prop := new(genericsmrproto.ProposeWithExecTime)
			if err = prop.Unmarshal(reader); err != nil {
				// afterParsing = time.Since(beforeParsing)
				break
			}
			r.ProposeWithExecTimeChan <- &ProposeWithExecTime{prop, writer}
			// afterParsing = time.Since(beforeParsing)
			// r.MsgParseLatChan <- &afterParsing
			break

		case genericsmrproto.READ:
			read := new(genericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case genericsmrproto.PROPOSE_AND_READ:
			pr := new(genericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break

		case genericsmrproto.REGISTER_CLIENT_ID:

			rci := new(genericsmrproto.RegisterClientIdArgs)
			if err = rci.Unmarshal(reader); err != nil {
				fmt.Println("Error reading from client", err)
				break
			}
			dlog.Println("Receiving registration from client", rci.ClientId)
			r.RegisterClientIdChan <- &Client{rci, writer}
			break

		case genericsmrproto.GET_VIEW:
			gv := new(genericsmrproto.GetView)
			if err = gv.Unmarshal(reader); err != nil {
				break
			}
			r.GetViewChan <- &GetView{gv, writer}
			break

		case genericsmrproto.CLIENT_RTT_TABLE:
			crt := new(genericsmrproto.ClientRttTable)
			if err = crt.Unmarshal(reader); err != nil {
				break
			}
			r.ClientRttChan <- &ClientRttTable{crt, writer}
			break
		// case genericsmrproto.LATENCY:
		// 	l := new(genericsmrproto.Latency)
		// 	if err = l.Unmarshal(reader); err != nil {
		// 		break
		// 	}
		// 	r.LatencyChan <- l
		// 	break
		case genericsmrproto.REAL_COMMIT_AT_LEAST:
			commitLatFeedback := new(genericsmrproto.CommitLatencyFeedback)
			if err = commitLatFeedback.Unmarshal(reader); err != nil {
				break
			}
			r.RealCommitAtLeastFromClientChan <- commitLatFeedback
			break
		case genericsmrproto.GHOST_COMMIT_AT_LEAST:
			commitLatFeedback := new(genericsmrproto.CommitLatencyFeedback)
			if err = commitLatFeedback.Unmarshal(reader); err != nil {
				break
			}
			r.MockCommitAtLeastFromClientChan <- commitLatFeedback
			break
		case genericsmrproto.COMMIT_LATENCY_FEEDBACK:
			commitLatFeedback := new(genericsmrproto.CommitLatencyFeedback)
			if err = commitLatFeedback.Unmarshal(reader); err != nil {
				break
			}
			r.CommitLatencyFeedBackChan <- commitLatFeedback
			break
		default:
			break
		}

	}
	if err != nil && err != io.EOF {
		log.Println("Error when reading from client connection:", err)
	}
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	return code
}

func (r *Replica) RegisterRPCCallback(code uint8, cb RecvCallback, arg interface{}) {
	r.recvCallbacks[code] = RecvCallbackArg{cb, arg}
}

func (r *Replica) RegisterRPCWithRecvTime(msgObj fastrpc.Serializable,
	notify chan SerializableWithRecvTime) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTableWithRecvTime[code] = &RPCPairWithRecvTime{msgObj, notify}
	return code
}

func (r *Replica) SendRotateMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	if dlog.DLOG {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			dlog.Printf("sending a msg to %v instance(from %s:%v) \n", peerId, details.Name(), no)
		} else {
			dlog.Printf("Sending a msg to %v")
		}
	}
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	beforeMarshal := time.Now()
	msg.Marshal(w)
	log.Printf("[SEND] Taking %v to marshal Rotate message.\n", time.Now().Sub(beforeMarshal))
	w.Flush()
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	// beforeParse := time.Now()
	if dlog.DLOG {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			dlog.Printf("sending a msg to %v instance(from %s:%v) \n", peerId, details.Name(), no)
		} else {
			dlog.Printf("Sending a msg to %v")
		}
	}
	// beforeParsing := time.Now()
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
	// parseLat := time.Since(beforeParse)
	// r.MsgParseLatChan <- &parseLat
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) ReplyPropose(reply *genericsmrproto.ProposeReply, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	// beforeParsing := time.Now()
	reply.Marshal(w)
	w.Flush()
	// parseLat := time.Since(beforeParsing)
	// r.MsgParseLatChan <- &parseLat
}

var Mu sync.Mutex

func (r *Replica) ReplyProposeTS(reply *genericsmrproto.ProposeReplyTS, w *bufio.Writer) error {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	// beforeParsing := time.Now()
	Mu.Lock()
	defer Mu.Unlock()
	// if r.IsSlowdownReplica && (INJECT_LONGLIVED_SLOWDOWN || INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT) {
	// 	slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
	// }
	if err := w.WriteByte(genericsmrproto.PROPOSE_REPLY); err != nil {
		return err
	}
	reply.Marshal(w)
	if err := w.Flush(); err != nil {
		return err
	}
	// parseLat := time.Since(beforeParsing)
	// r.MsgParseLatChan <- &parseLat
	return nil
}

func (r *Replica) ReplyProposeTSMock(clientId uint32, reply *genericsmrproto.ProposeReplyTSMock, w *bufio.Writer) error {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	// beforeParsing := time.Now()
	r.ClientWriteLock[clientId].Lock()
	defer r.ClientWriteLock[clientId].Unlock()
	// if r.IsSlowdownReplica && (INJECT_LONGLIVED_SLOWDOWN || INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT) {
	// 	slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
	// }
	if err := w.WriteByte(genericsmrproto.PROPOSE_REPLY); err != nil {
		return err
	}
	reply.Marshal(w)
	if err := w.Flush(); err != nil {
		return err
	}
	// parseLat := time.Since(beforeParsing)
	// r.MsgParseLatChan <- &parseLat
	return nil
}

func (r *Replica) SendBeacon(peerId int32) {
	w := r.PeerWriters[peerId]
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
	beacon := &genericsmrproto.Beacon{rdtsc.Cputicks()}
	beacon.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyBeacon(beacon *Beacon) {
	w := r.PeerWriters[beacon.Rid]
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := &genericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyRegisterClientId(reply *genericsmrproto.RegisterClientIdReply, w *bufio.Writer) {
	w.WriteByte(genericsmrproto.REGISTER_CLIENT_ID_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyGetView(reply *genericsmrproto.GetViewReply, w *bufio.Writer) {
	w.WriteByte(genericsmrproto.GET_VIEW_REPLY)
	reply.Marshal(w)
	w.Flush()
}

// updates the preferred order in which to communicate with peers according to a preferred quorum
func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.PreferredPeerOrder = aux
}
