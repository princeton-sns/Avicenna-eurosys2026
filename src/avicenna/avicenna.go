package avicenna

import (
	"avicennaproto"
	"bufio"
	"container/heap"
	"dlog"
	"errors"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"slowdowntimers"
	"sort"
	"state"
	"stats"
	"sync"
	"time"
)

const NOF_MICROSECONDS_PER_SECOND = 1000000

const MAX_CLIENTS = 2048

const GC_DEBUG_ENABLED = true

/***** Slowdown Constants *****/
const INJECT_TRANSIENT_SLOWDOWN = false
const INJECT_LONGLIVED_SLOWDOWN = true
const INJECT_LONGLIVED_SLOWDOWN_FOR_CLIENT = false
const COMMIT_REPLY_ONLY_IF_MOCK_REQUESTED_FROM_CLIENT = true
const INJECT_SSD_SLOWDOWN = true

const SORT_RTT = false

/******************************/

/***** LAN vs WAN parameters *****/

// const BATCH_INTERVAL = 10 * time.Millisecond // WAN
// const BATCH_INTERVAL = 500 * time.Microsecond // LAN
const BATCH_INTERVAL = 250 * time.Microsecond // LAN
// const BATCH_INTERVAL = 100 * time.Microsecond // LAN

const DRAIN_INTERVAL = 100 * time.Microsecond
const ARBITRARY_STANDBY_NO_ROLE = true // set this to Ture in LAN to preven leader or ghost leader from being standbys.

/*********************************/

// var slowdown_array = []int{10, 20, 40, 80, 160, 320, 640}
var receivedAPropose bool = false

const SLOWDOWNS_TO_TOLERATE = 1

const SANITY_CHECK = true

const CHAN_BUFFER_SIZE = 200000

const BYPASS_LATEST_SEEN = false

// const FALSE = uint8(0)
// const TRUE = uint8(1)
// const SENT_NULL = uint8(2) // we use r.sentForPhase for this
// const MOCKREPLY = uint8(3)
// const ATLEAST = uint8(4)
// const MOCKLATENCY = uint8(5) // never change 5 and 4!!!

// const (
//
//	DO_NOT_MOCK_THIS_DID_NOT_MOCK_LAST uint8 = iota
//	MOCK_THIS_DID_NOT_MOCK_LAST
//	DO_NOT_MOCK_THIS_DID_MOCK_LAST
//	MOCK_THIS_MOCK_LAST
//
// )
const (
	FALSE uint8 = iota
	TRUE
	SENT_NULL
	MOCKREPLY
	REALCOMMITATLEAST
	MOCKCOMMITLATENCY
	REALCOMMITLATENCY
)

const MAX_BATCH = 10000

const progTimerDuration = 1 * time.Minute
const PRINT_STATS_INTERVAL = 5 * time.Second
const PRINT_STATS = true

/*** Rotation parameters ***/

// detection consts
const MOCK = true // only used for panics
const DISABLE_SHORT_PATH = false
const SLOWDOWN_CHAN_SIZE = 16
const ROTATION_DELAY = 10 * time.Second //10 * time.Second
const REQUIRED_PERCENT_IMPROVEMENT = 10 //.5 //.03 // cli?
// const REQUIRED_TOTAL_IMPROVEMENT = BATCH_INTERVAL // 0.5 //0.03 // cli?
const REQUIRED_TOTAL_IMPROVEMENT = 10 * time.Millisecond // use BATCH_INTERVAL
const REQUIRED_TOTAL_AND_PERCENT = true
const OBJ_LATENCY_COM = 100 // 100: maximum latency; 95: 95th percentile latency; 0: average latency; 195: request num under 95th latency
// const CLEANUP_INTERVAL = 1 * time.Second

// const CLEANUP_INTERVAL = 200 * time.Millisecond

// const CLEANUP_INTERVAL = 1 * time.Second

const CLEANUP_INTERVAL = 200 * time.Millisecond

const WINDOW_SIZE = 1 * time.Second

// const WINDOW_SIZE = 200 * time.Millisecond

// const ENABLE_SHORT_PATH = true

/***************************/

const MAXUINT32 = ^uint32(0)
const MINUINT32 = 0
const MAXINT32 = int32(MAXUINT32 >> 1)
const MININT32 = -MAXINT32 - 1

const MAXUINT64 = ^uint64(0)
const MINUINT64 = 0
const MAXINT64 = int64(MAXUINT64 >> 1)
const MININT64 = -MAXINT64 - 1

var dups uint64

// args: iface metadata, lat float64, real bool, atLeast bool) bool
type ObjectiveFunc func(iface interface{}, clientAndLatency avicennaproto.ClientLatency, real bool, atLeast bool, commit bool, execTime int64) bool

type ObjectiveFuncCommit func(iface interface{}, clientCommitLat avicennaproto.ClientCommitLatency, realAtLeast bool, ghostAtLeast bool) bool

type ObjectiveFuncGhostExec func(iface interface{}, ghostExecLat avicennaproto.GhostExecLatency) bool

type ObjectiveFuncRealE2E func(iface interface{}, realE2ELat avicennaproto.ClientLatency) bool

type ClientLatencyHeap struct {
	backingArray []avicennaproto.ClientLatencyWithTimestamp
	latestCmd    map[uint32]int32
}

func (h *ClientLatencyHeap) Len() int { return len(h.backingArray) }

// want MaxHeap Less is More
func (h *ClientLatencyHeap) Less(i, j int) bool {
	return (h.backingArray)[i].ClientAndLatency.Latency >= (h.backingArray)[j].ClientAndLatency.Latency
}

func (h *ClientLatencyHeap) Swap(i, j int) {
	h.backingArray[i], h.backingArray[j] = h.backingArray[j], h.backingArray[i]
}

func (h *ClientLatencyHeap) Push(x any) {
	entry := x.(avicennaproto.ClientLatencyWithTimestamp)
	// log.Printf("[OBJ] Pushing %v\n", entry)
	h.backingArray = append(h.backingArray, entry)
	client := entry.ClientAndLatency.CmdId.ClientId
	h.latestCmd[client] = entry.ClientAndLatency.CmdId.OpId
}

func (h *ClientLatencyHeap) Pop() any {
	old := h.backingArray
	n := len(old)
	item := old[n-1]
	h.backingArray = old[0 : n-1]
	return item
}

func (h *ClientLatencyHeap) removeOldEntries(cutoffTime time.Time) {
	// Remove entries older than cutoffTime
	validEntries := make([]avicennaproto.ClientLatencyWithTimestamp, 0)
	for _, entry := range h.backingArray {
		if entry.Timestamp.After(cutoffTime) {
			validEntries = append(validEntries, entry)
		}
	}
	h.backingArray = validEntries
	heap.Init(h)
}

// should we have an ongoing distribution for every replica
// or just two heaps and reset them when we rotate?
// we don't want to reuse old latencies?
// but how many configurations are we rotating through? we may only have two replicas proposing
// until failure experiments.
// well we can reset when we have multiple replicas anyway, I guess that it the main question:
//
//	Do we reset the heaps to empty and rebuild them?
type TwoHeaps struct {
	real ClientLatencyHeap
	mock ClientLatencyHeap
	// we don't want to act on latency if we haven't received
	// both real (including AtLeast) and mock latencies for the same request
	pendingReal map[state.CommandId]avicennaproto.ClientLatency //bool
	pendingMock map[state.CommandId]avicennaproto.ClientLatency //bool //map[uint32]avicennaproto.ClientLatency
	receivedOne map[uint32]bool

	realEndToEnd ClientLatencyHeap
	mockEndToEnd ClientLatencyHeap
	// we don't want to act on latency if we haven't received
	// both real (including AtLeast) and mock latencies for the same request
	pendingRealEndToEnd map[state.CommandId]avicennaproto.ClientLatencyWithExecTime //bool
	pendingMockEndToEnd map[state.CommandId]avicennaproto.ClientLatencyWithExecTime //bool //map[uint32]avicennaproto.ClientLatency
	receivedOneEndToEnd map[uint32]bool

	maxLen int

	lastCleanup     time.Time
	cleanupInterval time.Duration
	windowSize      time.Duration
}

func (th *TwoHeaps) checkOrder() {
	// log.Printf("[OBJ CHECK] Checking heap partial orders...\n")
	flag := true
	for i := 1; i < len(th.real.backingArray); i++ {
		parent := (i - 1) / 2
		if th.real.backingArray[parent].ClientAndLatency.Latency < th.real.backingArray[i].ClientAndLatency.Latency {
			flag = false
			break
		}
	}
	// log.Printf("[OBJ CHECK]\t Real heap: %v\n", flag)

	flag = true
	for i := 1; i < len(th.mock.backingArray); i++ {
		parent := (i - 1) / 2
		if th.mock.backingArray[parent].ClientAndLatency.Latency < th.mock.backingArray[i].ClientAndLatency.Latency {
			flag = false
			break
		}
	}
	dlog.Printf("[OBJ CHECK]\t Mock heap: %v\n", flag)

	flag = true
	for i := 1; i < len(th.realEndToEnd.backingArray); i++ {
		parent := (i - 1) / 2
		if th.realEndToEnd.backingArray[parent].ClientAndLatency.Latency < th.realEndToEnd.backingArray[i].ClientAndLatency.Latency {
			flag = false
			break
		}
	}
	// log.Printf("[OBJ CHECK]\t RealEndToEnd heap: %v\n", flag)

	flag = true
	for i := 1; i < len(th.mockEndToEnd.backingArray); i++ {
		parent := (i - 1) / 2
		if th.mockEndToEnd.backingArray[parent].ClientAndLatency.Latency < th.mockEndToEnd.backingArray[i].ClientAndLatency.Latency {
			flag = false
			break
		}
	}
	// log.Printf("[OBJ CHECK]\t MockEndToEnd heap: %v\n", flag)
}

func (th *TwoHeaps) checkTimeWindow() {
	// log.Printf("[OBJ CHECK] Checking heap entry time window...\n")
	curTime := time.Now()
	cutoffTime := curTime.Add(-1 * th.windowSize)
	for _, entry := range th.real.backingArray {
		if entry.Timestamp.Before(cutoffTime) {
			// log.Printf("[OBJ CHECK]\t Real heap: %v\n", entry)
		}
	}

	for _, entry := range th.mock.backingArray {
		if entry.Timestamp.Before(cutoffTime) {
			// log.Printf("[OBJ CHECK]\t Mock heap: %v\n", entry)
		}
	}

	for _, entry := range th.realEndToEnd.backingArray {
		if entry.Timestamp.Before(cutoffTime) {
			// log.Printf("[OBJ CHECK]\t RealEndToEnd heap: %v\n", entry)
		}
	}

	for _, entry := range th.mockEndToEnd.backingArray {
		if entry.Timestamp.Before(cutoffTime) {
			// log.Printf("[OBJ CHECK]\t MockEndToEnd heap: %v\n", entry)
		}
	}
}

func (th *TwoHeaps) checkLatencyPair() {
	// log.Printf("[OBJ CHECK] Checking heap latency pairs...\n")
	flag := true

	if len(th.real.backingArray) != len(th.mock.backingArray) {
		flag = false
	} else {
		realMap := make(map[state.CommandId]struct{}, len(th.real.backingArray))
		mockMap := make(map[state.CommandId]struct{}, len(th.mock.backingArray))
		for _, e := range th.real.backingArray {
			realMap[e.ClientAndLatency.CmdId] = struct{}{}
		}
		for _, e := range th.mock.backingArray {
			mockMap[e.ClientAndLatency.CmdId] = struct{}{}
			if _, ok := realMap[e.ClientAndLatency.CmdId]; !ok {
				flag = false
				break
			}
		}
		for _, e := range th.real.backingArray {
			if _, ok := mockMap[e.ClientAndLatency.CmdId]; !ok {
				flag = false
				break
			}
		}
	}
	dlog.Printf("[OBJ CHECK]\t Real mock heaps: %v\n", flag)

	flag = true
	if len(th.realEndToEnd.backingArray) != len(th.mockEndToEnd.backingArray) {
		flag = false
	} else {
		realMap := make(map[state.CommandId]struct{}, len(th.real.backingArray))
		mockMap := make(map[state.CommandId]struct{}, len(th.mock.backingArray))
		for _, e := range th.realEndToEnd.backingArray {
			realMap[e.ClientAndLatency.CmdId] = struct{}{}
		}
		for _, e := range th.mockEndToEnd.backingArray {
			mockMap[e.ClientAndLatency.CmdId] = struct{}{}
			if _, ok := realMap[e.ClientAndLatency.CmdId]; !ok {
				flag = false
				break
			}
		}
		for _, e := range th.realEndToEnd.backingArray {
			if _, ok := mockMap[e.ClientAndLatency.CmdId]; !ok {
				flag = false
				break
			}
		}
	}
	// log.Printf("[OBJ CHECK]\t Real mock endtoend heaps: %v\n", flag)
}

func (th *TwoHeaps) checkInvariant() {
	// log.Printf("==========Start checking heap invariants==========\n")
	th.checkOrder()
	th.checkLatencyPair()
	th.checkTimeWindow()
	// log.Printf("===========End checking heap invariants===========\n")
}

func (th *TwoHeaps) addLatencyEntry(clientAndLatency avicennaproto.ClientLatency, real bool, timestamp time.Time) {
	if timestamp.Sub(th.lastCleanup) > th.cleanupInterval {
		cutoffTime := timestamp.Add(-1 * th.windowSize)
		th.real.removeOldEntries(cutoffTime)
		th.mock.removeOldEntries(cutoffTime)
		th.realEndToEnd.removeOldEntries(cutoffTime)
		th.mockEndToEnd.removeOldEntries(cutoffTime)
		th.lastCleanup = timestamp
	}

	entry := avicennaproto.ClientLatencyWithTimestamp{
		ClientAndLatency: clientAndLatency,
		Timestamp:        timestamp,
	}

	if real {
		heap.Push(&th.real, entry)
		th.real.latestCmd[clientAndLatency.CmdId.ClientId] = clientAndLatency.CmdId.OpId
	} else {
		heap.Push(&th.mock, entry)
		th.mock.latestCmd[clientAndLatency.CmdId.ClientId] = clientAndLatency.CmdId.OpId
	}
}

// returns index into the backingArray and true if newer false if older, -1 if client not in idx map
func (h *ClientLatencyHeap) checkIfNewer(clientAndLatency avicennaproto.ClientLatency) (replaceIdx int, push bool) {
	client := clientAndLatency.CmdId.ClientId
	idx := h.latestCmd[client]

	if clientAndLatency.CmdId.OpId >= idx {
		return -1, true
	}

	return -1, false
}

func (th *TwoHeaps) clear() {
	// log.Printf("Clearing my TwoHeaps\n")
	th.real = ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)}
	th.mock = ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)}
	th.pendingReal = make(map[state.CommandId]avicennaproto.ClientLatency)
	th.pendingMock = make(map[state.CommandId]avicennaproto.ClientLatency)
	th.receivedOne = make(map[uint32]bool)
	th.realEndToEnd = ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)}
	th.mockEndToEnd = ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)}
	th.pendingRealEndToEnd = make(map[state.CommandId]avicennaproto.ClientLatencyWithExecTime)
	th.pendingMockEndToEnd = make(map[state.CommandId]avicennaproto.ClientLatencyWithExecTime)
	th.receivedOneEndToEnd = make(map[uint32]bool)
}

// returns true if it was in the heap
func (th *TwoHeaps) checkPendingAndAddEndToEnd(clientAndLatency avicennaproto.ClientLatency, real bool, execTime int64) bool {
	// Invariant check, remember to comment when testing performance
	// th.checkInvariant()
	// End of invariant check
	if real {
		// check the mock pending map
		dlog.Printf("[OBJ] About to check pendingMock: \n\tMockEndToEnd %v \n\tRealEndToEnd %v\n", th.pendingMockEndToEnd, th.pendingRealEndToEnd)
		if mockClientAndLatency, ok := th.pendingMockEndToEnd[clientAndLatency.CmdId]; ok {
			// if there was a mock latency message pending, and if this is the newer cmdId
			// then this must also be the newer version of the real latency
			_, mockNewer := th.mockEndToEnd.checkIfNewer(mockClientAndLatency.ClientAndLatency)
			_, realNewer := th.realEndToEnd.checkIfNewer(clientAndLatency)
			timeStamp := time.Now()

			if realNewer {
				if SANITY_CHECK && !mockNewer {
					log.Panicf("EndToEnd Got a newer real request, but pending mock is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
				}
				// log.Printf("[OBJ] Got a RealEndToEnd %v, execTime %v, pendingMockEndToEnd is available %v\n", clientAndLatency, execTime, mockClientAndLatency)
				// add both to the maps
				if mockClientAndLatency.ExecTime > 0 && mockClientAndLatency.ClientAndLatency.Latency > 0 {
					heap.Push(&th.realEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
					heap.Push(&th.mockEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: mockClientAndLatency.ClientAndLatency, Timestamp: timeStamp})
					delete(th.pendingMockEndToEnd, clientAndLatency.CmdId)
				} else {
					th.pendingRealEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: clientAndLatency, ExecTime: execTime}
				}
				// mockClientAndLatency.ClientAndLatency.Latency += execTime
			}

			if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
				cutoffTime := timeStamp.Add(-1 * th.windowSize)
				th.real.removeOldEntries(cutoffTime)
				th.mock.removeOldEntries(cutoffTime)
				th.realEndToEnd.removeOldEntries(cutoffTime)
				th.mockEndToEnd.removeOldEntries(cutoffTime)
				th.lastCleanup = timeStamp
			}

			// delete(th.pendingMockEndToEnd, clientAndLatency.CmdId)
			// Invariant check, remember to comment when testing performance
			// th.checkInvariant()
			// End of invariant check
			return true
		} else {
			// repeated requests are not possible
			// not in the map so we should add it to its pending map
			// if we are adding this to pending we could have a previous one also pending but just haven't received it,
			// only take the most recent one?
			// TODO only solve this if it becomes a problem.
			th.pendingRealEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: clientAndLatency, ExecTime: execTime}
			// log.Printf("[OBJ] Got a RealEndToEnd %v, execTime %v, added to pendingRealEndToEnd: %v\n", clientAndLatency, execTime, th.pendingRealEndToEnd[clientAndLatency.CmdId])
			return false
		}
	} else { // this was a mock
		if execTime >= 0 { // report ghost leader execution time
			// log.Printf("[OBJ] Receive ghost exec latency %v, execTime %d\n", clientAndLatency, execTime)
			if SANITY_CHECK && clientAndLatency.Latency > 0 {
				log.Panicf("Weird, receive a mock e2e latency with both commit and exec latency larger than 0")
			}
			if mockClientAndLatency, ok := th.pendingMockEndToEnd[clientAndLatency.CmdId]; ok {
				if mockClientAndLatency.ExecTime < 0 {
					if SANITY_CHECK && mockClientAndLatency.ClientAndLatency.Latency < 0 {
						log.Panicf("Wired, a mock e2e latency in pending has both commit and exec latency less than 0\n")
					}
					mockClientAndLatency.ExecTime = execTime
					mockClientAndLatency.ClientAndLatency.Latency += execTime
				}
				if realClientAndLatency, found := th.pendingRealEndToEnd[clientAndLatency.CmdId]; found {
					_, mockNewer := th.mockEndToEnd.checkIfNewer(mockClientAndLatency.ClientAndLatency)
					_, realNewer := th.realEndToEnd.checkIfNewer(realClientAndLatency.ClientAndLatency)
					timeStamp := time.Now()

					if mockNewer {
						if SANITY_CHECK && !realNewer {
							log.Panicf("EndToEnd Got a newer mock request, but pending real is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
						}
						// log.Printf("[OBJ] Received execTime, about to push real endtoend, %v\n", realClientAndLatency)
						heap.Push(&th.realEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: realClientAndLatency.ClientAndLatency, Timestamp: timeStamp})
						// log.Printf("[OBJ] About to push mock endtoend, %v\n", mockClientAndLatency)
						heap.Push(&th.mockEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: mockClientAndLatency.ClientAndLatency, Timestamp: timeStamp})
						delete(th.pendingRealEndToEnd, clientAndLatency.CmdId)
						delete(th.pendingMockEndToEnd, clientAndLatency.CmdId)

						if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
							cutoffTime := timeStamp.Add(-1 * th.windowSize)
							th.real.removeOldEntries(cutoffTime)
							th.mock.removeOldEntries(cutoffTime)
							th.realEndToEnd.removeOldEntries(cutoffTime)
							th.mockEndToEnd.removeOldEntries(cutoffTime)
							th.lastCleanup = timeStamp
						}
						return true
					}
				} else {
					th.pendingMockEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: mockClientAndLatency.ClientAndLatency, ExecTime: execTime}
				}
				return false
			} else {
				// execution latency arrives earlier than commit latency
				th.pendingMockEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: clientAndLatency, ExecTime: execTime}
				return false
			}
		} else { // added from mock commit latency
			// log.Printf("[OBJ] Received ghost commit latency %v, execTime %d\n", clientAndLatency, execTime)
			if SANITY_CHECK && clientAndLatency.Latency < 0 {
				log.Panicf("Weird, receive a mock e2e latency with both commit and exec latency less than 0\n")
			}
			if mockClientAndLatency, ok := th.pendingMockEndToEnd[clientAndLatency.CmdId]; ok {
				if mockClientAndLatency.ExecTime > 0 && mockClientAndLatency.ClientAndLatency.Latency < 0 && clientAndLatency.Latency > 0 {
					clientAndLatency.Latency += mockClientAndLatency.ExecTime
					if realClientAndLatency, found := th.pendingRealEndToEnd[clientAndLatency.CmdId]; found {
						_, mockNewer := th.mockEndToEnd.checkIfNewer(mockClientAndLatency.ClientAndLatency)
						_, realNewer := th.realEndToEnd.checkIfNewer(realClientAndLatency.ClientAndLatency)
						timeStamp := time.Now()

						if mockNewer {
							if SANITY_CHECK && !realNewer {
								log.Panicf("EndToEnd Got a newer mock request, but pending real is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
							}

							// log.Printf("[OBJ] Received execTime, about to push real endtoend, %v\n", realClientAndLatency)
							heap.Push(&th.realEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: realClientAndLatency.ClientAndLatency, Timestamp: timeStamp})
							// log.Printf("[OBJ] About to push mock endtoend, %v\n", clientAndLatency)
							heap.Push(&th.mockEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
							delete(th.pendingRealEndToEnd, clientAndLatency.CmdId)
							delete(th.pendingMockEndToEnd, clientAndLatency.CmdId)

							if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
								cutoffTime := timeStamp.Add(-1 * th.windowSize)
								th.real.removeOldEntries(cutoffTime)
								th.mock.removeOldEntries(cutoffTime)
								th.realEndToEnd.removeOldEntries(cutoffTime)
								th.mockEndToEnd.removeOldEntries(cutoffTime)
								th.lastCleanup = timeStamp
							}
							return true
						}
					} else {
						th.pendingMockEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: clientAndLatency, ExecTime: mockClientAndLatency.ExecTime}
					}
				}
				return false
			}

			th.pendingMockEndToEnd[clientAndLatency.CmdId] = avicennaproto.ClientLatencyWithExecTime{ClientAndLatency: clientAndLatency, ExecTime: -1}
			return false
		}
	}
}

func (th *TwoHeaps) checkAndAddCommit(clientRealCommitLat avicennaproto.ClientLatency, clientGhostCommitLat avicennaproto.ClientLatency, realAtLeast bool, ghostAtLeast bool) bool {
	_, realNewer := th.real.checkIfNewer(clientRealCommitLat)
	_, ghostNewer := th.mock.checkIfNewer(clientGhostCommitLat)
	timeStamp := time.Now()

	if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
		cutoffTime := timeStamp.Add(-1 * th.windowSize)
		th.real.removeOldEntries(cutoffTime)
		th.mock.removeOldEntries(cutoffTime)
		th.realEndToEnd.removeOldEntries(cutoffTime)
		th.mockEndToEnd.removeOldEntries(cutoffTime)
		th.lastCleanup = timeStamp
	}

	if realNewer && ghostNewer {
		// log.Printf("[OBJ] Push ghost commit latency to pending %v\n", clientGhostCommitLat)
		if clientGhostCommitLat.Latency < 0 {
			log.Printf("Weird, receive a ghost commit latency less than 0 %v\n", clientGhostCommitLat)
		} else if !ghostAtLeast {
			th.checkPendingAndAddEndToEnd(clientGhostCommitLat, false, -1)
		} else {

		}
		heap.Push(&th.real, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientRealCommitLat, Timestamp: timeStamp})
		heap.Push(&th.mock, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientGhostCommitLat, Timestamp: timeStamp})
		return true
	}
	return false
}

// returns true if it was in the heap
func (th *TwoHeaps) checkPendingAndAdd(clientAndLatency avicennaproto.ClientLatency, real bool) bool {
	log.Printf("Weird! We stop using checkPendingAndAdd function!\n")
	// we did have it
	// Invariant check, remember to comment when testing performance
	// th.checkInvariant()
	// End of invariant check
	if real {
		// check the mock pending map
		// log.Printf("[OBJ] About to check pendingMock: \n\tMock %v \n\tReal %v\n", th.pendingMock, th.pendingReal)
		if mockClientAndLatency, ok := th.pendingMock[clientAndLatency.CmdId]; ok {
			// if there was a mock latency message pending, and if this is the newer cmdId
			// then this must also be the newer version of the real latency
			_, mockNewer := th.mock.checkIfNewer(mockClientAndLatency)
			_, realNewer := th.real.checkIfNewer(clientAndLatency)
			timeStamp := time.Now()
			if realNewer {
				if SANITY_CHECK && !mockNewer {
					log.Panicf("Got a newer real request, but pending mock is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
				}
				// add both to the maps
				heap.Push(&th.real, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
				heap.Push(&th.mock, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: mockClientAndLatency, Timestamp: timeStamp})
			}

			if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
				cutoffTime := timeStamp.Add(-1 * th.windowSize)
				th.real.removeOldEntries(cutoffTime)
				th.mock.removeOldEntries(cutoffTime)
				th.realEndToEnd.removeOldEntries(cutoffTime)
				th.mockEndToEnd.removeOldEntries(cutoffTime)
				th.lastCleanup = timeStamp
			}

			delete(th.pendingMock, clientAndLatency.CmdId)
			// Invariant check, remember to comment when testing performance
			// th.checkInvariant()
			// End of invariant check
			return true
		} else {
			// repeated requests are not possible
			// not in the map so we should add it to its pending map
			// if we are adding this to pending we could have a previous one also pending but just haven't received it,
			// only take the most recent one?
			// TODO only solve this if it becomes a problem.
			th.pendingReal[clientAndLatency.CmdId] = clientAndLatency
			return false
		}
	} else { // this was a mock
		// check the mock pending map
		// log.Printf("About to check pendingReal: \n\tMock %v \n\tReal %v\n", th.pendingMock, th.pendingReal)

		// if _, ok := th.pendingMockEndToEnd[clientAndLatency.CmdId]; !ok {
		// 	th.pendingMockEndToEnd[clientAndLatency.CmdId] = clientAndLatency
		// }

		th.checkPendingAndAddEndToEnd(clientAndLatency, false, 0)

		if realClientAndLatency, ok := th.pendingReal[clientAndLatency.CmdId]; ok {
			// if there was a mock latency message pending, and if this is the newer cmdId
			// then this must also be the newer version of the real latency
			_, mockNewer := th.mock.checkIfNewer(clientAndLatency)
			_, realNewer := th.real.checkIfNewer(realClientAndLatency)
			timeStamp := time.Now()
			if mockNewer {
				if SANITY_CHECK && !realNewer {
					log.Panicf("Got a newer mock request, but real is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
				}
				// add both to the maps
				heap.Push(&th.real, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: realClientAndLatency, Timestamp: timeStamp})
				heap.Push(&th.mock, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
			}

			if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
				cutoffTime := timeStamp.Add(-1 * th.windowSize)
				th.real.removeOldEntries(cutoffTime)
				th.mock.removeOldEntries(cutoffTime)
				th.realEndToEnd.removeOldEntries(cutoffTime)
				th.mockEndToEnd.removeOldEntries(cutoffTime)
				th.lastCleanup = timeStamp
			}

			delete(th.pendingReal, clientAndLatency.CmdId)
			// Invariant check, remember to comment when testing performance
			// th.checkInvariant()
			// End of invariant check
			return true
		} else {
			// delete(th.pendingMock, state.CommandId{clientAndLatency.CmdId.ClientId, clientAndLatency.CmdId.OpId - 1})
			th.pendingMock[clientAndLatency.CmdId] = clientAndLatency
			return false
		}
	}
}

func getMin(h *ClientLatencyHeap) (int64, error) {
	if len(h.backingArray) <= 0 {
		return 0, errors.New("Empty array")
	}
	min := h.backingArray[0].ClientAndLatency.Latency
	for _, val := range h.backingArray {
		if val.ClientAndLatency.Latency < min {
			min = val.ClientAndLatency.Latency
		}
	}
	return min, nil
}

func checkMin(a *ClientLatencyHeap, b *ClientLatencyHeap) bool {
	// TODO efficiency
	minA, errA := getMin(a)
	if errA != nil {
		return false
	}
	minB, errB := getMin(b)
	if errB != nil {
		return false
	}
	if minA < minB-int64(REQUIRED_TOTAL_IMPROVEMENT) {
		return true
	}
	return false
}

func getPercentile(h *ClientLatencyHeap, percentile float64) int64 {
	if len(h.backingArray) == 0 {
		return 0
	}
	sorted := make([]int64, len(h.backingArray))
	for i, entry := range h.backingArray {
		sorted[i] = entry.ClientAndLatency.Latency
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	idx := int(float64(len(sorted)-1) * percentile)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	} else if idx < 0 {
		idx = 0
	}

	return sorted[idx]
}

func calculateReqPerSecUnderLat(h *ClientLatencyHeap, latP int64) float64 {
	if len(h.backingArray) == 0 || latP <= 0 {
		return 0.0
	}

	totalReqPerSec := 0.0
	countUnderLatP := 0

	for _, entry := range h.backingArray {
		if entry.ClientAndLatency.Latency <= latP && entry.ClientAndLatency.Latency > 0 {
			reqPerSec := float64(NOF_MICROSECONDS_PER_SECOND) / float64(entry.ClientAndLatency.Latency)
			totalReqPerSec += reqPerSec
			countUnderLatP++
		}
	}

	if countUnderLatP == 0 {
		return 0.0
	}

	return totalReqPerSec
}

func calculateTailAverLatency(h *ClientLatencyHeap, latP int64) float64 {
	if len(h.backingArray) == 0 || latP <= 0 {
		return 0.0
	}

	totalLatNum := int64(0)
	countLat := int64(0)

	for _, entry := range h.backingArray {
		if entry.ClientAndLatency.Latency >= latP {
			totalLatNum += entry.ClientAndLatency.Latency
			countLat++
		}
	}

	if countLat == 0 {
		return 0.0
	}

	return float64(totalLatNum / countLat)
}

func checkIfBetterTailAver(mock *ClientLatencyHeap, real *ClientLatencyHeap, maxLen int, _percentile int) bool {
	percentile := float64(_percentile) / 100
	if maxLen <= real.Len() && len(mock.backingArray) > 0 && len(real.backingArray) > 0 {
		mockLatP := getPercentile(mock, percentile)
		realLatP := getPercentile(real, percentile)

		if mockLatP <= 0 || realLatP <= 0 {
			return false
		}

		mockTailLatAver := calculateTailAverLatency(mock, mockLatP)
		realRailLatAver := calculateTailAverLatency(real, realLatP)

		if realRailLatAver > mockTailLatAver {
			diff := realRailLatAver - mockTailLatAver
			percentImprovement := float64(diff) / float64(mockTailLatAver)
			if REQUIRED_TOTAL_AND_PERCENT {
				requiredImpro := float64(REQUIRED_TOTAL_IMPROVEMENT/time.Microsecond) + mockTailLatAver*REQUIRED_PERCENT_IMPROVEMENT
				if diff < requiredImpro {
					return false
				}
			} else {
				if percentImprovement < REQUIRED_PERCENT_IMPROVEMENT {
					return false
				}
			}
			log.Printf("[OBJ] real %d, ghost %d, diff is %d, percentImprove %f\n", realRailLatAver, mockTailLatAver, diff, percentImprovement)
			return true
		} else {
			return false
		}
	}
	return false
}

func checkIfBetterThrputP(mock *ClientLatencyHeap, real *ClientLatencyHeap, maxLen int, _percentile int) bool {
	percentile := float64(_percentile) / 100
	if maxLen <= real.Len() && len(mock.backingArray) > 0 && len(real.backingArray) > 0 {
		mockLatP := getPercentile(mock, percentile)
		realLatP := getPercentile(real, percentile)

		if mockLatP <= 0 || realLatP <= 0 {
			return false
		}

		mockReqPerSec := calculateReqPerSecUnderLat(mock, mockLatP)
		realReqPerSec := calculateReqPerSecUnderLat(real, realLatP)
		// log.Printf("[OBJ] Real request per second: %v, mock request per second: %v\n", realReqPerSec, mockReqPerSec)

		if mockReqPerSec > realReqPerSec {
			diff := mockReqPerSec - realReqPerSec
			percentImperovement := float64(diff) / float64(realReqPerSec)

			if REQUIRED_TOTAL_AND_PERCENT {
				requiredAbsImpro := float64(NOF_MICROSECONDS_PER_SECOND) / float64(REQUIRED_TOTAL_IMPROVEMENT.Microseconds())
				requiredImpro := requiredAbsImpro + REQUIRED_PERCENT_IMPROVEMENT*float64(realReqPerSec)
				if diff < requiredImpro {
					return false
				}
			} else {
				if percentImperovement < REQUIRED_PERCENT_IMPROVEMENT {
					return false
				}
			}
			// log.Printf("[OBJ] Want to rotate: real %v vs mock %v, diff is %v, percent: %v\n", realReqPerSec, mockReqPerSec, diff, percentImperovement)
			return true
		} else {
			return false
		}
	}
	return false
}

// compare the 95th percentile latency between real and mock
func checkIfLessP(a *ClientLatencyHeap, b *ClientLatencyHeap, maxLen int, _percentile int) bool {
	percentile := float64(_percentile) / 100
	if maxLen <= b.Len() && len(a.backingArray) > 0 && len(b.backingArray) > 0 {
		lenA := len(a.backingArray)
		lenB := len(b.backingArray)
		latenciesA := make([]int64, lenA)
		for i, cl := range a.backingArray {
			latenciesA[i] = cl.ClientAndLatency.Latency
		}

		latenciesB := make([]int64, lenB)
		for i, cl := range b.backingArray {
			latenciesB[i] = cl.ClientAndLatency.Latency
		}

		sort.Slice(latenciesA, func(i, j int) bool {
			return latenciesA[i] < latenciesA[j]
		})
		sort.Slice(latenciesB, func(i, j int) bool {
			return latenciesB[i] < latenciesB[j]
		})

		idxA := int(math.Ceil(percentile*float64(lenA))) - 1
		if idxA < 0 {
			idxA = 0
		}
		if idxA >= lenA {
			idxA = lenA - 1
		}

		idxB := int(math.Ceil(percentile*float64(lenB))) - 1
		if idxB < 0 {
			idxB = 0
		}
		if idxB >= lenB {
			idxB = lenB - 1
		}

		perLatA := latenciesA[idxA]
		perLatB := latenciesB[idxB]

		if perLatA < perLatB {
			diff := perLatB - perLatA
			percentImprovement := float64(diff) / float64(perLatB)

			if REQUIRED_TOTAL_AND_PERCENT {
				required := (REQUIRED_PERCENT_IMPROVEMENT*float64(perLatB)*float64(time.Microsecond) + float64(REQUIRED_TOTAL_IMPROVEMENT)) / float64(time.Microsecond)
				// log.Printf("[OBJ] Diff is %v, requiring %v, real p95 latency is %v\n", diff, required, perLatB)
				if float64(diff) < required {
					return false
				}
			} else {
				if percentImprovement < REQUIRED_PERCENT_IMPROVEMENT || time.Duration(diff)*time.Microsecond <= REQUIRED_TOTAL_IMPROVEMENT {
					// log.Printf("[OBJ] real %d, ghost %d, diff is %d, percentImprove %f\n", perLatB, perLatA, diff, percentImprovement)
					return false
				}
			}
			// log.Printf("[OBJ] Want to rotate: %v vs %v , diff is %v %% improvement %v\n",
			// 	perLatA, perLatB, diff, percentImprovement)
			log.Printf("[OBJ] real %d, ghost %d, diff is %d, percentImprove %f\n", perLatB, perLatA, diff, percentImprovement)
			return true
		} else {
			return false
		}
	}
	return false
}

// compare the average latency between real and mock
func checkIfLessAver(a *ClientLatencyHeap, b *ClientLatencyHeap, maxLen int) bool {
	if maxLen <= b.Len() && len(a.backingArray) > 0 && len(b.backingArray) > 0 {
		sum := int64(0)
		for _, cl := range a.backingArray {
			sum += cl.ClientAndLatency.Latency
		}
		averA := float64(sum) / float64(len(a.backingArray))

		sum = 0
		for _, cl := range b.backingArray {
			sum += cl.ClientAndLatency.Latency
		}
		averB := float64(sum) / float64(len(b.backingArray))

		// log.Printf("[OBJ] Mock average latency: %v, real average latency: %v\n", averA, averB)
		if averA < averB {
			diff := averB - averA
			percentImprovement := float64(diff) / float64(averB)

			if REQUIRED_TOTAL_AND_PERCENT {
				required := (REQUIRED_PERCENT_IMPROVEMENT*float64(averB)*float64(time.Microsecond) + float64(REQUIRED_TOTAL_IMPROVEMENT)) / float64(time.Microsecond)
				// log.Printf("Diff is %v, requiring %v, real average latency is %v\n", diff, required, averB)
				if float64(diff) < required {
					return false
				}
			} else {
				if percentImprovement < REQUIRED_PERCENT_IMPROVEMENT || time.Duration(diff)*time.Microsecond <= REQUIRED_TOTAL_IMPROVEMENT {
					return false
				}
			}
			// log.Printf("Want to rotate: %v vs %v(average), diff is %v %% improvement %v\n",
			// 	averA, averB, diff, percentImprovement)
			return true
		} else {
			return false
		}
	}
	return false
}

// compare the maximum latency between real and mock
func checkIfLess(a *ClientLatencyHeap, b *ClientLatencyHeap, maxLen int) bool {
	if maxLen <= b.Len() && len(a.backingArray) > 0 && len(b.backingArray) > 0 {
		if a.backingArray[0].ClientAndLatency.Latency < b.backingArray[0].ClientAndLatency.Latency {
			diff := b.backingArray[0].ClientAndLatency.Latency - a.backingArray[0].ClientAndLatency.Latency
			percentImprovement := float64(diff) / float64(b.backingArray[0].ClientAndLatency.Latency)

			if REQUIRED_TOTAL_AND_PERCENT {
				// log.Printf("latency %v percent %v product %v aded %v\n", float64(b.backingArray[0].Latency), REQUIRED_PERCENT_IMPROVEMENT, REQUIRED_PERCENT_IMPROVEMENT*float64(b.backingArray[0].Latency),
				// 	REQUIRED_PERCENT_IMPROVEMENT*float64(b.backingArray[0].Latency)+float64(REQUIRED_TOTAL_IMPROVEMENT))

				required := (REQUIRED_PERCENT_IMPROVEMENT*float64(b.backingArray[0].ClientAndLatency.Latency)*float64(time.Microsecond) + float64(REQUIRED_TOTAL_IMPROVEMENT)) / float64(time.Microsecond)
				// log.Printf("[OBJ] Diff is %v requiring %v real latency is %v\n", diff, required, b.backingArray[0].ClientAndLatency.Latency)
				if float64(diff) < required {
					// TODO efficiency
					return checkMin(a, b)
					// return false
				}

				// log.Printf("Diff is %v requiring %v real latency is %v\n", diff, required, b.backingArray[0].Latency)
			} else {
				// only rotate if above some percent imrpovement
				if percentImprovement < REQUIRED_PERCENT_IMPROVEMENT || time.Duration(diff)*time.Microsecond <= REQUIRED_TOTAL_IMPROVEMENT {
					return checkMin(a, b)
					// return false
				}
			}
			log.Printf("Want to rotate: %v vs %v diff is %v %% improvement %v\n",
				a.backingArray[0], b.backingArray[0], b.backingArray[0].ClientAndLatency.Latency-a.backingArray[0].ClientAndLatency.Latency,
				float64((b.backingArray[0].ClientAndLatency.Latency-a.backingArray[0].ClientAndLatency.Latency))/float64(b.backingArray[0].ClientAndLatency.Latency))
			// log.Printf("Offending requests in mock: %v\n", a.backingArray[a.idxMap[b.backingArray[0].CmdId.ClientId]])

			// th.clear()
			return true
		} else if a.backingArray[0].ClientAndLatency.Latency-b.backingArray[0].ClientAndLatency.Latency < int64(5*time.Millisecond) {
			// if they are nearly the same get the better min latency
			return checkMin(a, b)
		}
	}
	return false
}

func (th *TwoHeaps) checkIfRotateAndClearEndToEnd() bool {
	ret := false
	timeStamp := time.Now()

	if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
		cutoffTime := timeStamp.Add(-1 * th.windowSize)
		th.realEndToEnd.removeOldEntries(cutoffTime)
		th.mockEndToEnd.removeOldEntries(cutoffTime)
		th.lastCleanup = timeStamp
	}
	// log.Printf("[OBJ] Checking EndToEnd heaps, timestamp: %v, realEndToEnd: %v, mockEndToEnd: %v\n", timeStamp, th.realEndToEnd.backingArray, th.mockEndToEnd.backingArray)
	// th.checkTimeWindow()

	if OBJ_LATENCY_COM == 100 {
		// log.Printf("[OBJ] Checking maximum end to end latency.\n")
		ret = checkIfLess(&th.mockEndToEnd, &th.realEndToEnd, th.maxLen)
	} else if OBJ_LATENCY_COM < 100 && OBJ_LATENCY_COM > 0 {
		// log.Printf("[OBJ] Checking %vth percentile end to end latency.\n", OBJ_LATENCY_COM)
		ret = checkIfLessP(&th.mockEndToEnd, &th.realEndToEnd, th.maxLen, OBJ_LATENCY_COM)
	} else if OBJ_LATENCY_COM == 0 {
		// log.Printf("[OBJ] Checking average end to end latency.\n")
		ret = checkIfLessAver(&th.mockEndToEnd, &th.realEndToEnd, th.maxLen)
	} else if OBJ_LATENCY_COM < 200 && OBJ_LATENCY_COM > 100 {
		// log.Printf("[OBJ] Checking request number per second under %vth percentile end to end latency\n", OBJ_LATENCY_COM-100)
		ret = checkIfBetterThrputP(&th.mockEndToEnd, &th.realEndToEnd, th.maxLen, OBJ_LATENCY_COM-100)
	} else if OBJ_LATENCY_COM < 300 && OBJ_LATENCY_COM > 200 {
		ret = checkIfBetterTailAver(&th.mockEndToEnd, &th.realEndToEnd, th.maxLen, OBJ_LATENCY_COM-200)
	} else {
		return false
	}
	if ret {
		th.clear()
	}
	return ret
}

func (th *TwoHeaps) checkIfRotateAndClear() bool {
	ret := false
	timeStamp := time.Now()

	if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
		cutoffTime := timeStamp.Add(-1 * th.windowSize)
		th.real.removeOldEntries(cutoffTime)
		th.mock.removeOldEntries(cutoffTime)
		th.lastCleanup = timeStamp
	}
	// log.Printf("[OBJ] Checking heaps: timestamp: %v, real: %v, mock: %v\n",
	// 	timeStamp, th.real.backingArray, th.mock.backingArray)
	// th.checkTimeWindow()

	if OBJ_LATENCY_COM == 100 {
		// log.Printf("[OBJ] Checking maximum latency.\n")
		ret = checkIfLess(&th.mock, &th.real, th.maxLen)
	} else if OBJ_LATENCY_COM < 100 && OBJ_LATENCY_COM > 0 {
		// log.Printf("[OBJ] Checking %vth percentile latency.\n", OBJ_LATENCY_COM)
		ret = checkIfLessP(&th.mock, &th.real, th.maxLen, OBJ_LATENCY_COM)
	} else if OBJ_LATENCY_COM == 0 {
		// log.Printf("[OBJ] Checking average latency.\n")
		ret = checkIfLessAver(&th.mock, &th.real, th.maxLen)
	} else if OBJ_LATENCY_COM < 200 && OBJ_LATENCY_COM > 100 {
		// log.Printf("[OBJ] Checking request number per second under %vth percentile latency.\n", OBJ_LATENCY_COM-100)
		ret = checkIfBetterThrputP(&th.mock, &th.real, th.maxLen, OBJ_LATENCY_COM-100)
	} else if OBJ_LATENCY_COM < 300 && OBJ_LATENCY_COM > 200 {
		// log.Printf("[OBJ] Checking request number per second under %vth percentile latency.\n", OBJ_LATENCY_COM-100)
		ret = checkIfBetterTailAver(&th.mock, &th.real, th.maxLen, OBJ_LATENCY_COM-200)
	} else {
		return false
	}
	if ret {
		th.clear()
	}
	return ret
}

func processCommitObjectiveFunctionEndToEnd(iface interface{}, clientAndLatency avicennaproto.ClientLatency, real bool, atLeast bool, execTime_ int64) bool {
	th := iface.(*TwoHeaps)

	log.Printf("Weird, we stop using processCommitObjectiveFunctionEndToEnd function\n")
	// execTime := execTime_ / time.Hour.Microseconds()
	execTime := execTime_ / 1000
	// log.Printf("execTime was %v\n", execTime)

	// delete first latency from every client each time it rotates (it will be high)
	if !th.receivedOneEndToEnd[clientAndLatency.CmdId.ClientId] {
		th.receivedOneEndToEnd[clientAndLatency.CmdId.ClientId] = true
		return false
	}

	if SANITY_CHECK && th.mockEndToEnd.Len() != th.realEndToEnd.Len() {
		log.Panicf("objectiveFunctionEndToEnd(): th.mockEndToEnd.Len != th.realEndToEnd.Len %v != %v\n", th.mockEndToEnd.Len(), th.realEndToEnd.Len())
	}

	// log.Printf("[OBJ] New endtoend latency: %v, real: %v, atLeast: %v, execTime: %v\n", clientAndLatency, real, atLeast, execTime_)

	if !atLeast {
		addedToHeap := false
		if real {
			// need make sure there is a corresponding pending Latency message
			// and add to both maps if there is
			addedToHeap = th.checkPendingAndAddEndToEnd(clientAndLatency, real, execTime)
		} else {
			dlog.Printf("Got a mock EndToEnd latency from a client! %v\n", clientAndLatency)
			// log.Printf("This is weird, we don't expect MockEndToEnd latency to be added directly!!!\n")
			addedToHeap = th.checkPendingAndAddEndToEnd(clientAndLatency, real, execTime)
		}
		// TODO TODO TODO we should not say mock is better if received real for opid 5 but mock only at 4
		// 			right now this just replaces it and checks
		if th.maxLen > th.realEndToEnd.Len() {
			// log.Printf("not enough client requests to determine if should rotate\n")
		}
		// log.Printf("[OBJ] Not atLeast, addedToHeap: %v, realEndToEndHeapArray: %v, mockEndToEndHeapArray: %v\n",
		// 	addedToHeap, th.realEndToEnd.backingArray, th.mockEndToEnd.backingArray)
		// th.checkTimeWindow()
		if addedToHeap {
			if th.checkIfRotateAndClearEndToEnd() {
				return true
			}
		}
	} else {
		// we did have it
		// check the mock pending map
		log.Printf("It's weird, we have an EndToEnd with atLeast!!!\n")
		dlog.Printf("checkPendingForAtLeast() About to check pendingMock: \n\tMock %v \n\tReal %v\n", th.pendingMock, th.pendingReal)
		if pendingMockClientAndLatency, ok := th.pendingMockEndToEnd[clientAndLatency.CmdId]; ok {
			// if there was a mock latency message pending, and if this is the newer cmdId
			// then this must also be the newer version of the real latency
			_, mockNewer := th.mockEndToEnd.checkIfNewer(pendingMockClientAndLatency.ClientAndLatency)
			_, realNewer := th.realEndToEnd.checkIfNewer(clientAndLatency)
			timeStamp := time.Now()

			if realNewer {
				if SANITY_CHECK && !mockNewer {
					log.Panicf("checkPendingForAtLeast()Got a newer real request, but pending mock is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
				}

				// add both to the maps
				heap.Push(&th.realEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
				pendingMockClientAndLatency.ClientAndLatency.Latency += execTime
				heap.Push(&th.mockEndToEnd, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: pendingMockClientAndLatency.ClientAndLatency, Timestamp: timeStamp})
			}

			if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
				cutoffTime := timeStamp.Add(-1 * th.windowSize)
				th.real.removeOldEntries(cutoffTime)
				th.mock.removeOldEntries(cutoffTime)
				th.realEndToEnd.removeOldEntries(cutoffTime)
				th.mockEndToEnd.removeOldEntries(cutoffTime)
				th.lastCleanup = timeStamp
			}

			delete(th.pendingMockEndToEnd, clientAndLatency.CmdId)
			// log.Printf("[OBJ] Timestamp: %v, Is atLeast, realEndToEndHeapArray: %v, mockEndToEndHeapArray: %v\n",
			// 	timeStamp, th.realEndToEnd.backingArray, th.mockEndToEnd.backingArray)

			// Invariant check, remember to comment when testing performance
			// th.checkInvariant()
			// End of invariant check

			if th.checkIfRotateAndClearEndToEnd() {
				return true
			}
		}
	}
	return false
}

func processCommitObjectiveFunction(iface interface{}, clientAndLatency avicennaproto.ClientLatency, real bool, atLeast bool) bool {
	th := iface.(*TwoHeaps)

	// Invariant check, remember to comment when testing performance
	// th.checkInvariant()
	// End of invariant check

	// delete first latency from every client each time it rotates (it will be high)
	if !th.receivedOne[clientAndLatency.CmdId.ClientId] {
		th.receivedOne[clientAndLatency.CmdId.ClientId] = true
		return false
	}

	if SANITY_CHECK && th.mock.Len() != th.real.Len() {
		log.Panicf("objectiveFunction(): th.mock.Len != th.real.Len %v != %v\n", th.mock.Len(), th.real.Len())
	}

	// log.Printf("[OBJ] New commit latency: %v, real: %v, atLeast: %v\n", clientAndLatency, real, atLeast)

	if !atLeast {
		addedToHeap := false
		if real {
			// need make sure there is a corresponding pending Latency message
			// and add to both maps if there is
			// log.Printf("[OBJ] New real commit latency: {ClientId %d, CommandId %d, latency %d}\n", clientAndLatency.CmdId.ClientId, clientAndLatency.CmdId.OpId, clientAndLatency.Latency)
			addedToHeap = th.checkPendingAndAdd(clientAndLatency, real)
		} else {
			dlog.Printf("Got a mock latency from a client! %v\n", clientAndLatency)
			// log.Printf("[OBJ] New mock commit latency: {ClientId %d, CommandId %d, latency %d}\n", clientAndLatency.CmdId.ClientId, clientAndLatency.CmdId.OpId, clientAndLatency.Latency)
			addedToHeap = th.checkPendingAndAdd(clientAndLatency, real)
		}
		// TODO TODO TODO we should not say mock is better if received real for opid 5 but mock only at 4
		// 			right now this just replaces it and checks
		if th.maxLen > th.real.Len() {
			dlog.Printf("not enough client requests to determine if should rotate\n")
		}
		// log.Printf("[OBJ] Not atLeast, addedToHeap: %v, realHeapArray: %v, mockHeapArray: %v\n", addedToHeap, th.real.backingArray, th.mock.backingArray)
		if addedToHeap {
			if th.checkIfRotateAndClear() {
				return true
			}
		}
	} else {
		// we did have it
		// check the mock pending map
		dlog.Printf("checkPendingForAtLeast() About to check pendingMock: \n\tMock %v \n\tReal %v\n", th.pendingMock, th.pendingReal)
		if pendingMockClientAndLatency, ok := th.pendingMock[clientAndLatency.CmdId]; ok {
			// if there was a mock latency message pending, and if this is the newer cmdId
			// then this must also be the newer version of the real latency
			_, mockNewer := th.mock.checkIfNewer(pendingMockClientAndLatency)
			_, realNewer := th.real.checkIfNewer(clientAndLatency)
			timeStamp := time.Now()
			if realNewer {
				if SANITY_CHECK && !mockNewer {
					log.Panicf("checkPendingForAtLeast()Got a newer real request, but pending mock is not newer!? clientAndLatency %v\n\n\tth %v\n", clientAndLatency, *th)
				}

				// add both to the maps
				heap.Push(&th.real, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: clientAndLatency, Timestamp: timeStamp})
				heap.Push(&th.mock, avicennaproto.ClientLatencyWithTimestamp{ClientAndLatency: pendingMockClientAndLatency, Timestamp: timeStamp})
			}

			if timeStamp.Sub(th.lastCleanup) > th.cleanupInterval {
				cutoffTime := timeStamp.Add(-1 * th.windowSize)
				th.real.removeOldEntries(cutoffTime)
				th.mock.removeOldEntries(cutoffTime)
				th.realEndToEnd.removeOldEntries(cutoffTime)
				th.mockEndToEnd.removeOldEntries(cutoffTime)
				th.lastCleanup = timeStamp
			}

			delete(th.pendingMock, clientAndLatency.CmdId)
			// log.Printf("[OBJ] Timestamp: %v, Is atLeast, addedToHeap: %v, realHeapArray: %v, mockHeapArray: %v\n",
			// 	timeStamp, realNewer, th.real.backingArray, th.mock.backingArray)

			// Invariant check, remember to comment when testing performance
			// th.checkInvariant()
			// End of invariant check

			if th.checkIfRotateAndClear() {
				return true
			}
		}
	}

	return false
}

func objectiveFunction(iface interface{}, clientAndLatency avicennaproto.ClientLatency, real bool, atLeast bool, commit bool, execTime int64) bool {
	dlog.Printf("objectiveFunction processing %v len(twoheaps) %v %v\n", clientAndLatency, len(iface.(*TwoHeaps).real.backingArray), len(iface.(*TwoHeaps).mock.backingArray))
	if !commit {
		if SANITY_CHECK && commit || atLeast {
			log.Panicf("Impossible parameters to objectiveFunction one of %v %v is true\n", commit, atLeast)
		}
		return processCommitObjectiveFunctionEndToEnd(iface, clientAndLatency, real, atLeast, execTime)
	} else {
		return processCommitObjectiveFunction(iface, clientAndLatency, real, atLeast)
	}
}

func objectiveFunctionCommit(iface interface{}, clientCommitLatency avicennaproto.ClientCommitLatency, realAtLeast bool, ghostAtLeast bool) bool {
	th := iface.(*TwoHeaps)

	// Invariant check, remember to comment when testing performance
	// th.checkInvariant()
	// End of invariant check

	// delete first latency from every client each time it rotates (it will be high)
	if !th.receivedOne[clientCommitLatency.CmdId.ClientId] {
		th.receivedOne[clientCommitLatency.CmdId.ClientId] = true
		return false
	}

	if SANITY_CHECK && th.mock.Len() != th.real.Len() {
		log.Panicf("objectiveFunction(): th.mock.Len != th.real.Len %v != %v\n", th.mock.Len(), th.real.Len())
	}

	// log.Printf("[OBJ] New commit latency: %v, real: %v, atLeast: %v\n", clientAndLatency, real, atLeast)

	addedToHeap := false

	cmdId := clientCommitLatency.CmdId

	addedToHeap = th.checkAndAddCommit(avicennaproto.ClientLatency{CmdId: cmdId, Latency: clientCommitLatency.RealCommitLatency},
		avicennaproto.ClientLatency{CmdId: cmdId, Latency: clientCommitLatency.GhostCommitLatency}, realAtLeast, ghostAtLeast)
	// TODO TODO TODO we should not say mock is better if received real for opid 5 but mock only at 4
	// 			right now this just replaces it and checks
	if th.maxLen > th.real.Len() {
		dlog.Printf("not enough client requests to determine if should rotate\n")
	}
	// log.Printf("[OBJ] Not atLeast, addedToHeap: %v, realHeapArray: %v, mockHeapArray: %v\n", addedToHeap, th.real.backingArray, th.mock.backingArray)
	if addedToHeap {
		if th.checkIfRotateAndClear() {
			return true
		}
	}
	return false
}

func objectiveFunctionGhostExec(iface interface{}, ghostExecLatency avicennaproto.GhostExecLatency) bool {
	th := iface.(*TwoHeaps)

	addedToHeap := false

	// log.Printf("[OBJ] Push ghost execTime to pending %v\n", avicennaproto.ClientLatency{CmdId: ghostExecLatency.CmdId, Latency: -1})
	addedToHeap = th.checkPendingAndAddEndToEnd(avicennaproto.ClientLatency{CmdId: ghostExecLatency.CmdId, Latency: -1}, false, ghostExecLatency.ExecTime)

	if addedToHeap {
		if th.checkIfRotateAndClearEndToEnd() {
			return true
		}
	}
	return false
}

func objectiveFunctionRealE2E(iface interface{}, realE2ELatency avicennaproto.ClientLatency) bool {
	th := iface.(*TwoHeaps)

	addedToHeap := false

	addedToHeap = th.checkPendingAndAddEndToEnd(realE2ELatency, true, -1)

	if addedToHeap {
		if th.checkIfRotateAndClearEndToEnd() {
			return true
		}
	}
	return false
}

// oof, do we really need this
// should have inherited...
type CommandDoMock struct {
	Command state.Command
	DoMock  bool
}

type BatchedCmds struct {
	Cmds   []state.CommandAvi
	DoMock bool
}

type ReplicaStats struct {
	maxRotateSize     int32
	maxMockRotateSize int32
	total             int32
	nBatches          int32
	nMsgsSent         int32

	nAcceptTx          int32
	nAcceptRx          int32
	nAcceptReplyTx     int32
	nAcceptReplyRx     int32
	nMockAcceptTx      int32
	nMockAcceptRx      int32
	nMockAcceptReplyTx int32
	nMockAcceptReplyRx int32

	nCommitsTx              int32
	nCommitsToClientsTx     int32
	nMockCommitsTx          int32
	nMockCommitsToClientsTx int32
	nCommitsRx              int32
	nMockCommitsRx          int32
	nProposalsReceived      int32

	nCommittedFromClient     int32
	nMockCommittedFromClient int32
	nRealCommitAtLeast       int32
}

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	// RPC members
	acceptChan         chan genericsmr.SerializableWithRecvTime
	acceptExecTimeChan chan genericsmr.SerializableWithRecvTime
	commitChan         chan genericsmr.SerializableWithRecvTime
	acceptReplyChan    chan genericsmr.SerializableWithRecvTime
	rotateChan         chan genericsmr.SerializableWithRecvTime
	pingChan           chan genericsmr.SerializableWithRecvTime
	pingReplyChan      chan genericsmr.SerializableWithRecvTime
	rttTableChan       chan genericsmr.SerializableWithRecvTime
	slowdownChan       chan int32
	acceptRPC          uint8
	acceptExecTimeRPC  uint8
	commitRPC          uint8
	acceptReplyRPC     uint8
	rotateRPC          uint8
	pingRPC            uint8
	pingReplyRPC       uint8
	rttTableRPC        uint8

	// Protocol members
	instanceSpace     []*Instance // the space of all instances (used and not yet used)
	instanceSpaceMock []*Instance // the mock instance space used for slowdown detection

	// phase/rotation metadata
	phase              int32 // the current phase
	sentForPhase       bool  // did I send Rotate for this phase?
	nRotateMessages    int8  // the number of Rotate messages this replica received in phase
	RotateMessages     map[int32]*[]avicennaproto.InstanceCommands
	RotateMessagesMock map[int32]*[]avicennaproto.InstanceCommands // for the MockInstances
	crtInstance        int32                                       // highest active instance number that this replica knows about
	crtInstanceMock    int32                                       // highest active instance number that this replica knows about // TODO might need this
	timer              *time.Timer                                 // unused
	endTime            time.Time

	// defined by user
	objectiveFuncCommit    ObjectiveFuncCommit
	objectiveFuncGhostExec ObjectiveFuncGhostExec
	objectiveFuncRealE2E   ObjectiveFuncRealE2E
	objectiveFuncMD        interface{}

	Shutdown          bool
	counter           int // todo unused
	flush             bool
	committedUpTo     int32
	mockCommittedUpTo int32 // TODO commits are done by both followers and coordinator

	clientWriters     map[uint32]*bufio.Writer
	execMap           []bool                            // keep track executed commands; could also use map; chose array for performance reason
	execMapMock       []bool                            // keep track executed commands; could also use map; chose array for performance reason
	cmdMap            map[state.CommandId]CommandDoMock // both real and mock use the cmdMap
	latestCmdSeen     map[uint32]state.CommandAvi       //int32                  // latest command seen for each client
	latestCmdSeenMock map[uint32]state.CommandAvi       //int32                  // latest command seen for each client Mock

	configurations   []Configuration
	configArmageddon []Configuration
	rttTable         map[int32]map[int32]int64
	clientRttTable   map[uint32]map[int32]int64

	// cmdTimers map[state.CommandId]time.Timer
	cmdMetadata map[state.CommandId]*CommandMetadata // TODO merge with cmdMap
	warmupDone  bool

	stats ReplicaStats

	slowdownTimers *slowdowntimers.SlowdownTimers

	surgMock bool

	mockExecTime chan genericsmrproto.MockExecTime_

	clientCommitLatChan  chan avicennaproto.ClientCommitLatency
	clientRealE2ELatChan chan avicennaproto.ClientLatency
	ghostExecLatChan     chan []genericsmrproto.MockExecTime_
	stopEva              chan struct{}

	broadcastCommitToClientChan chan InstWithNum
	batchedCmdsChan             chan BatchedCmds
	replicaMu                   []sync.Mutex
	globalMaxCommittedReal      int32
	globalMaxCommittedGhost     int32
	// what do we need to doo for mocking?
	// have a map of client requests to the statuses of each replica mocking
	// compare the mocked request to the actual requests above and start timers
	// the timers put values in a channel which is checked to trigger rotation
	// the metadata for each client request should have the status for each replica
	// each replica should also mock commit messages (the leader can be slow
	// just before committing)
	// cmdMetadata map[state.CommandId]bool // is bool fine?
}

type CommandMetadata struct {
	timer      *time.Timer
	status     InstanceStatus // either NULL ACCEPTED or COMMITTED // todo use different status type
	startTime  time.Time
	accept     time.Time
	mockAccept time.Time
	commit     time.Time
	mockCommit time.Time

	mockAcceptTime time.Duration
	acceptTime     time.Duration

	mockCommitTime time.Duration
	commitTime     time.Duration

	// acceptTimerStart    time.Time
	// acceptTimerDuration time.Duration
	// acceptTimerStop     time.Time
	// acceptTimerFired    time.Time
	// commitTimerStart    time.Time
	// commitTimerDuration time.Duration
	// commitTimerStop     time.Time
	// commitTimerFired    time.Time
}

type InstanceStatus int

const (
	NULL InstanceStatus = iota
	RECEIVEDFROMOTHER
	RECEIVED
	ACCEPTED // I received this from the actual coordinator (which could be me)
	MOCKACCEPTED
	COMMITTED
	MOCKCOMMITTED
	OVERCOMMITTED
	GHOSTOVERCOMMITTED
)

// can maybe get rid of MOCK* statuses

type Configuration struct {
	coordinator       int32
	delegate          int32
	nVotesForDelegate int32
	acceptors         map[int32]bool
	standByReplicas   map[int32]bool
	mockCoordinator   int32
	// mockCoordinators  map[int32]bool
}

// type CommandDoMock struct {
// 	*state.Command
// 	doMock bool
// }

type Instance struct {
	cmds       []state.CommandAvi
	status     InstanceStatus
	accepts    int
	doGhost    bool
	phase      int32
	commitTime time.Time
}

type InstWithNum struct {
	Instance Instance
	instNo   int32
}

func createInstance(cmds []state.CommandAvi, status InstanceStatus, accepts int, doGhost bool, phase int32) *Instance {
	if dlog.DLOG { // probably unnecessary
		_, _, no, ok := runtime.Caller(1)
		if ok {
			dlog.Printf("createInstance called from %v\n", no)
		}

		dlog.Printf("Creating instance (from line %v) cmds %v status %v accepts %v\n", no, cmds, status, accepts)
	}
	return &Instance{cmds, status, accepts, doGhost, phase, time.Time{}} // this is safe in Go, see pointer analysis
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, slowdownDuration time.Duration) *Replica {
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),
		make(chan genericsmr.SerializableWithRecvTime, 3*genericsmr.CHAN_BUFFER_SIZE),
		make(chan genericsmr.SerializableWithRecvTime, 3*genericsmr.CHAN_BUFFER_SIZE), // 3?
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),   // 3?
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),   // 3?
		make(chan genericsmr.SerializableWithRecvTime, genericsmr.CHAN_BUFFER_SIZE),   // 3?
		make(chan int32, SLOWDOWN_CHAN_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0,
		make([]*Instance, 15*1024*1024), // instanceSpace
		make([]*Instance, 15*1024*1024), // instanceSpaceMock
		0,                               // uncommittedInstance, phase, crtCoordinator
		false,                           // sentForPhase
		0,                               // nReceivedMessages
		make(map[int32]*[]avicennaproto.InstanceCommands, 0),
		make(map[int32]*[]avicennaproto.InstanceCommands, 0),
		0, 0, nil, // nil is *time.Timer
		time.Time{},
		objectiveFunctionCommit,
		objectiveFunctionGhostExec,
		objectiveFunctionRealE2E,
		nil,
		false,
		0,
		true,
		-1, -1,
		make(map[uint32]*bufio.Writer),
		make([]bool, MAX_CLIENTS<<21),
		make([]bool, MAX_CLIENTS<<21),
		// make(map[state.CommandId]state.Command),
		make(map[state.CommandId]CommandDoMock),
		make(map[uint32]state.CommandAvi),
		make(map[uint32]state.CommandAvi),
		make([]Configuration, len(peerAddrList)),
		make([]Configuration, len(peerAddrList)),
		make(map[int32]map[int32]int64),
		make(map[uint32]map[int32]int64),
		make(map[state.CommandId]*CommandMetadata),
		false,
		ReplicaStats{},
		&slowdowntimers.SlowdownTimers{},
		false,
		make(chan genericsmrproto.MockExecTime_, genericsmr.CHAN_BUFFER_SIZE),
		make(chan avicennaproto.ClientCommitLatency, genericsmr.CHAN_BUFFER_SIZE),
		make(chan avicennaproto.ClientLatency, genericsmr.CHAN_BUFFER_SIZE),
		make(chan []genericsmrproto.MockExecTime_, genericsmr.CHAN_BUFFER_SIZE),
		make(chan struct{}),
		make(chan InstWithNum, genericsmr.CHAN_BUFFER_SIZE),
		make(chan BatchedCmds, genericsmr.CHAN_BUFFER_SIZE),
		make([]sync.Mutex, 7),
		0, 0,
	}

	r.SlowdownDuration = slowdownDuration

	// make(map[state.CommandId]state.Command)}
	for id := int32(0); id < int32(r.N); id++ {
		r.rttTable[id] = make(map[int32]int64)
	}
	r.objectiveFuncMD = &TwoHeaps{
		ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)},
		ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)},
		make(map[state.CommandId]avicennaproto.ClientLatency),
		make(map[state.CommandId]avicennaproto.ClientLatency),
		make(map[uint32]bool),
		ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)},
		ClientLatencyHeap{backingArray: make([]avicennaproto.ClientLatencyWithTimestamp, 0), latestCmd: make(map[uint32]int32)},
		make(map[state.CommandId]avicennaproto.ClientLatencyWithExecTime),
		make(map[state.CommandId]avicennaproto.ClientLatencyWithExecTime),
		make(map[uint32]bool),
		5,
		time.Now(),
		CLEANUP_INTERVAL,
		WINDOW_SIZE,
	}

	r.Durable = durable

	r.commitRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.Commit), r.commitChan)
	r.acceptRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.Accept), r.acceptChan)
	r.acceptExecTimeRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.AcceptExecTime), r.acceptExecTimeChan)
	r.RegisterRPCCallback(r.acceptRPC, receiveCoordinatorAcceptCallback, r)
	r.acceptReplyRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.AcceptReply), r.acceptReplyChan)
	r.rotateRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.Rotate), r.rotateChan)
	r.pingRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.Ping), r.pingChan)
	r.pingReplyRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.PingReply), r.pingReplyChan)
	r.rttTableRPC = r.RegisterRPCWithRecvTime(new(avicennaproto.RttTable), r.rttTableChan)

	go r.run()
	return r
}

func (r *Replica) evaluateRealAndGhost() {
	for {
		// if r.sentForPhase {
		// 	time.Sleep(5 * time.Second)
		// }
		select {
		case <-r.stopEva:
			r.objectiveFuncMD.(*TwoHeaps).clear()
		default:

		}

		select {
		case clientCommitLat := <-r.CommitLatencyFeedBackChan:
			if r.warmupDone {
				// if r.warmupDone && !r.isCoordinator(r.Id) {
				if r.objectiveFuncCommit(r.objectiveFuncMD, avicennaproto.ClientCommitLatency{CmdId: clientCommitLat.CommandId,
					RealCommitLatency: clientCommitLat.RealCommitLatency, GhostCommitLatency: clientCommitLat.GhostCommitLatency}, false, false) {
					if DISABLE_SHORT_PATH == false {
						log.Printf("Decide to rotate from commit latency.\n")
						r.slowdownChan <- r.phase
						r.objectiveFuncMD.(*TwoHeaps).clear()
					}
				}
				// r.clientCommitLatChan <- avicennaproto.ClientCommitLatency{CmdId: commitLat.CommandId,
				// 	RealCommitLatency: commitLat.RealCommitLatency, GhostCommitLatency: commitLat.GhostCommitLatency}
			}

		case realCommitAtLeast := <-r.RealCommitAtLeastFromClientChan:
			if r.warmupDone {
				if r.objectiveFuncCommit(r.objectiveFuncMD, avicennaproto.ClientCommitLatency{CmdId: realCommitAtLeast.CommandId,
					RealCommitLatency: realCommitAtLeast.RealCommitLatency, GhostCommitLatency: realCommitAtLeast.GhostCommitLatency}, true, false) {
					if DISABLE_SHORT_PATH == false {
						log.Printf("Decide to rotate from RealCommitAtLeast latency.\n")
						r.slowdownChan <- r.phase
						r.objectiveFuncMD.(*TwoHeaps).clear()
					}
				}
			}
		case ghostCommitAtLeast := <-r.MockCommitAtLeastFromClientChan:
			if r.warmupDone {
				if r.objectiveFuncCommit(r.objectiveFuncMD, avicennaproto.ClientCommitLatency{CmdId: ghostCommitAtLeast.CommandId,
					RealCommitLatency: ghostCommitAtLeast.RealCommitLatency, GhostCommitLatency: ghostCommitAtLeast.GhostCommitLatency}, false, true) {
					if DISABLE_SHORT_PATH == false {
						log.Printf("Decide to rotate from GhostCommitAtLeast latency.\n")
						r.slowdownChan <- r.phase
						r.objectiveFuncMD.(*TwoHeaps).clear()
					}
				}
			}

		case clientRealE2ELatency := <-r.clientRealE2ELatChan:
			if r.warmupDone && !r.isCoordinator(r.Id) {
				if r.objectiveFuncRealE2E(r.objectiveFuncMD, clientRealE2ELatency) {
					log.Printf("Decide to rotate from e2e latency")
					r.slowdownChan <- r.phase
					r.objectiveFuncMD.(*TwoHeaps).clear()
				}
			}

		case ghostExecLatency := <-r.ghostExecLatChan:
			if r.warmupDone && !r.isCoordinator(r.Id) {
				for _, entry := range ghostExecLatency {
					if entry.ExecTime >= 0 {
						if r.objectiveFuncGhostExec(r.objectiveFuncMD, avicennaproto.GhostExecLatency{CmdId: entry.CommandId, ExecTime: entry.ExecTime}) {
							log.Printf("Decide to rotate from e2e latency")
							r.slowdownChan <- r.phase
							r.objectiveFuncMD.(*TwoHeaps).clear()
						}
					} else {
						log.Printf("Weird, receive a ghost execution latency less than 0 %v\n", entry)
					}

				}
			}
		default:
			time.Sleep(BATCH_INTERVAL)
		}
	}
}

// index is offset by one because we don't want to use the first config again
func (r *Replica) isCoordinatorForPhase(rid int32, phase int32) bool {
	// log.Printf("isCoordinatorForPhase rid %v phase %v\n", rid, phase)
	return rid == r.configurationForPhase(phase).coordinator
	// return rid == r.configurations[int(phase)%len(r.configurations)].coordinator
	// old
	// if phase > 0 {
	// 	return rid == r.configurations[(phase-1)%(SLOWDOWNS_TO_TOLERATE+1)].coordinator
	// }
	// return rid == r.configurations[len(r.configurations)-1].coordinator
}

// todo why does this and the above func not use the configurationForPhase() func?
// index is offset by one because we don't want to use the first config again
func (r *Replica) isMockCoordinatorForPhase(rid int32, phase int32) bool {
	return rid == r.configurationForPhase(phase).mockCoordinator

	// if phase > 0 {
	// 	return rid == r.configurations[(phase-1)%(SLOWDOWNS_TO_TOLERATE+1)].mockCoordinator
	// }
	// return rid == r.configurations[len(r.configurations)-1].mockCoordinator
}

// index is offset by one because we don't want to use the first config again
func (r *Replica) configurationForPhase(phase int32) *Configuration {
	if int(phase+1)%(r.N+1) == 0 {
		return &r.configArmageddon[int(phase+1)/(r.N+1)]
	}
	return &r.configurations[int(phase+1)%(r.N+1)-1]
	// dlog.Printf("isCoordinatorForPhase %v idx %v\n", phase, phase%(SLOWDOWNS_TO_TOLERATE+1))
	// if phase > 0 {
	// 	return &r.configurations[(phase-1)%(SLOWDOWNS_TO_TOLERATE+1)]
	// }

	// return &r.configurations[len(r.configurations)-1]
}

func (r *Replica) isCurMockCoordinator() bool {
	dlog.Printf("curMockCoordinator returning %v\n", r.curConfiguration().mockCoordinator)
	return r.Id == r.curConfiguration().mockCoordinator
	// if _, ok := r.curConfiguration().mockCoordinators[r.Id]; ok {
	// 	return true
	// }
	// return false
}

func (r *Replica) curCoordinator() int32 {
	dlog.Printf("curCoordinator returning %v\n", r.curConfiguration().coordinator)
	return r.curConfiguration().coordinator
}

func (r *Replica) isCoordinator(rid int32) bool {
	return rid == r.curCoordinator()
}

func (r *Replica) curDelegate() int32 {
	dlog.Printf("curCoordinator returning %v\n", r.curConfiguration().coordinator)
	return r.curConfiguration().delegate
}

func (r *Replica) isDelegate(rid int32) bool {
	return rid == r.curDelegate()
}

func (r *Replica) isStandby(rid int32) bool {
	_, ok := r.curConfiguration().standByReplicas[rid]
	return ok
}

func (r *Replica) isStandbyForPhase(rid int32, phase int32) bool {
	_, ok := r.configurationForPhase(phase).standByReplicas[rid]
	return ok
}

func (r *Replica) curConfiguration() *Configuration {
	return r.configurationForPhase(r.phase)
	// return &r.configurations[r.phase%int32(r.N)]
}

/*
* Should be defined by the application.
 */
// func objectiveFunction(iface interface{}, lat float64, real bool, atLeast bool) bool {
// 	return false
// }

func (r *Replica) expectedArrivalTime(cid int32, rid int32) time.Duration {

	return 0
}

func (r *Replica) setTimerIfEarlier(duration time.Duration) {
	if r.timer == nil {
		r.timer = time.NewTimer(duration)
		return
	}
	if r.endTime.IsZero() || time.Now().Add(duration).Before(r.endTime) {
		r.timer.Reset(duration)
	}
}

func nbWriteToChan(c *chan int32, v int32) bool {
	select {
	case *c <- v:
		return true
	default:
		return false
	}
}
func (r *Replica) nbWriteToSlowdownChan(phase int32) bool {
	// select {
	// case p := <-r.slowdownChan:
	// 	if p > phase {
	// 		return nbWriteToChan(&r.slowdownChan, p)
	// 		// select {
	// 		// case r.slowdownChan <- p:
	// 		// 	return true
	// 		// default:
	// 		// 	return false
	// 		// }
	// 	}
	// default:
	// }
	// return nbWriteToChan(&r.slowdownChan, phase)
	// ORIG WAS JUST THIS
	select {
	case r.slowdownChan <- phase:
		dlog.Printf("wrote to channel for phase %v\n", phase)
		return true
	default:
		return false
	}
}

// TODO r is unused...
func (r *Replica) idsFromCmds(cmds []state.CommandAvi) []state.CommandId {
	ret := make([]state.CommandId, len(cmds))
	for i, cmd := range cmds {
		ret[i].ClientId = cmd.Cmd.ClientId
		ret[i].OpId = cmd.Cmd.OpId
	}
	return ret
}

func (r *Replica) getAndDeleteQueue() []state.CommandAvi {
	if BYPASS_LATEST_SEEN {
		return nil
	}
	ret := make([]state.CommandAvi, len(r.latestCmdSeen))
	i := 0
	for cid, cmd := range r.latestCmdSeen {
		// ret[i] = state.CommandId{ClientId: cid, OpId: opid}
		ret[i] = cmd
		delete(r.latestCmdSeen, cid)
		i++
	}
	dlog.Printf("getAndDeleteQueue() cmdQ now %v returning %v\n", r.latestCmdSeen, ret)
	return ret
}

func (r *Replica) getAndDeleteQueueMock() []state.CommandAvi {
	ret := make([]state.CommandAvi, len(r.latestCmdSeenMock))
	i := 0
	for cid, cmd := range r.latestCmdSeenMock {
		// ret[i] = state.CommandId{ClientId: cid, OpId: cmd}
		ret[i] = cmd
		delete(r.latestCmdSeenMock, cid)
		i++
	}
	dlog.Printf("getAndDeleteQueueMock() cmdQ now %v returning %v\n", r.latestCmdSeenMock, ret)
	return ret
}

// func (r *Replica) deleteClientFromQueue(cid uint32) {
// dlog.Printf("deleteCmdsFromQueue() deleting %v from %v\n", cid, r.latestCmdSeen)
// 	delete(r.latestCmdSeen, cid)
// 	dlog.Printf("deleteCmdsFromQueue() cmdQ now %v\n", r.latestCmdSeen)
// }

func receiveCoordinatorAcceptCallback(i interface{}, frs fastrpc.Serializable) {
	// r := i.(*Replica)
	// accept := frs.(*avicennaproto.Accept)
	// if r.isCoordinator(accept.ReplicaId) {
	// 	dlog.Printf("Received a Accept resetting the timer\n")
	// 	r.resetTimer()
	// }
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	// var b [5]byte
	// binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	// b[4] = byte(inst.status)
	// r.StableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	// Comment out this block to test the logs are the same
	if !r.Durable {
		return
	}
	log.Panicf("Should not be durable")

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

// func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
// 	r.IsLeader = true
// 	return nil
// }

var clockChan chan bool

// todo make sure timing is correct?
func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(BATCH_INTERVAL)
		clockChan <- true
	}
}

func (r *Replica) drainClock() {
	for !r.Shutdown {
		if !r.isCoordinator(r.Id) && !r.isCurMockCoordinator() {
			time.Sleep(DRAIN_INTERVAL)
			clockChan <- true
		}
	}
}

func (r *Replica) surgMockTimer(trigger <-chan struct{}, expired chan<- struct{}, duration time.Duration) {
	var timer *time.Timer
	var timerC <-chan time.Time // nil if timer not active

	for {
		select {
		case <-trigger:
			if timer != nil {
				if !timer.Stop() {
					<-timer.C // drain if expired
				}
			}
			timer = time.NewTimer(duration)
			timerC = timer.C
			// fmt.Println("[Timer] Timer started or reset")

		case <-timerC:
			// Timer expired
			// fmt.Println("[Timer] Timer expired, notifying main")
			expired <- struct{}{}
			timer = nil
			timerC = nil
		}
	}
}

func (r *Replica) batcher() {
	// cmds := []state.CommandAvi{}
	// doMock := false
	// for !r.Shutdown {
	// 	select {
	// 	case <-clockChan:
	// 		if (r.isCoordinator(r.Id) || r.isCurMockCoordinator()) && len(cmds) > 0 {
	// 			r.batchedCmdsChan <- BatchedCmds{Cmds: cmds, DoMock: doMock}
	// 		}
	// 		cmds = []state.CommandAvi{}
	// 		doMock = false

	// 	default:
	// 		break
	// 	}

	// 	select {
	// 	case propose := <-r.ProposeWithExecTimeChan:
	// 		newCmd := state.CommandAvi{Cmd: propose.Command, DoMock: propose.CommandId > 0}
	// 		if newCmd.Cmd.OpId < 0 {
	// 			r.replyToClientPing(propose)
	// 		} else {
	// 			if propose.EndToEndLatency.Latency > 0 {
	// 				r.clientRealE2ELatChan <- avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: propose.Command.ClientId, OpId: propose.EndToEndLatency.CommandId}, Latency: propose.EndToEndLatency.Latency}
	// 			}

	// 			cmds = append(cmds, newCmd)
	// 			if propose.CommandId > 0 {
	// 				doMock = true
	// 			}
	// 		}
	// 	}
	// }

	cmds := []state.CommandAvi{}
	doMock := false
	for !r.Shutdown {
		select {
		case propose := <-r.ProposeWithExecTimeChan:
			newCmd := state.CommandAvi{Cmd: propose.Command, DoMock: propose.CommandId > 0}
			if newCmd.Cmd.OpId < 0 {
				r.replyToClientPing(propose)
			} else {
				if propose.EndToEndLatency.Latency > 0 {
					r.clientRealE2ELatChan <- avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: propose.Command.ClientId, OpId: propose.EndToEndLatency.CommandId}, Latency: propose.EndToEndLatency.Latency}
				}

				cmds = append(cmds, newCmd)
				if propose.CommandId > 0 {
					doMock = true
				}
			}

		case <-clockChan:
			if (r.isCoordinator(r.Id) || r.isCurMockCoordinator()) && len(cmds) > 0 {
				r.batchedCmdsChan <- BatchedCmds{Cmds: cmds, DoMock: doMock}
			}
			cmds = []state.CommandAvi{}
			doMock = false
		}
	}
}

func (r *Replica) broadcastCommitToClients() {
	for true {
		select {
		case committedInst := <-r.broadcastCommitToClientChan:
			// r.sync()
			// dlog.Printf("[BCAST-COMMIT] Receive a commit instance %v\n", committedInst)
			if r.isCoordinator(r.Id) {
				for _, cmd := range committedInst.Instance.cmds {
					if COMMIT_REPLY_ONLY_IF_MOCK_REQUESTED_FROM_CLIENT {
						if cmd.DoMock {
							r.sendRealCommitToClient(committedInst.instNo, cmd.Cmd.ClientId, cmd.Cmd.OpId)
						}
					} else {
						r.sendRealCommitToClient(committedInst.instNo, cmd.Cmd.ClientId, cmd.Cmd.OpId)
					}
				}
			} else if r.isCurMockCoordinator() {
				for _, cmd := range committedInst.Instance.cmds {
					if COMMIT_REPLY_ONLY_IF_MOCK_REQUESTED_FROM_CLIENT {
						if cmd.DoMock {
							r.sendMockCommitToClient(committedInst.instNo, cmd.Cmd.ClientId, cmd.Cmd.OpId)
						}
					} else {
						r.sendMockCommitToClient(committedInst.instNo, cmd.Cmd.ClientId, cmd.Cmd.OpId)
					}
				}
			}
		}
	}
}

func getKeyForExecMap(clientId uint32, opId int32) uint32 {
	return (clientId << 21) | uint32(opId)
}
func cmdKey(cmdId state.CommandId) uint32 { // uint32, opId int32) uint32 {
	return (cmdId.ClientId << 21) | uint32(cmdId.OpId)
}

func (r *Replica) addCmdIdsToQueueIfNotPresentMock(cmdIdsToAdd []state.CommandAvi, cmdIdsToCheck []state.CommandAvi) {
	// return
	dlog.Printf("addToQueueIfNotPresentMock() checking if any of %v is in %v and adding to Q if not\n", cmdIdsToAdd, cmdIdsToCheck)
	for _, cmdIdToAdd := range cmdIdsToAdd {
		// k := getKeyForExecMap(cmdIdToAdd.ClientId, cmdIdToAdd.OpId)
		// fmt.Printf("addCmdIdsToQueue key %v len(execMap) %v cmd %v %v\n", k, len(r.execMap), cmdIdToAdd.ClientId, cmdIdToAdd.OpId)
		// if r.execMap[k] {
		// 	continue
		// }

		// todo let's just create duplicates and see what throughput is
		found := false
		// for _, cmdIdToCheck := range cmdIdsToCheck {
		// 	if cmdIdToAdd.ClientId == cmdIdToCheck.ClientId && cmdIdToAdd.OpId == cmdIdToCheck.OpId {
		// 		found = true
		// 		break
		// 	}
		// }
		if !found {
			dlog.Printf("Adding to latestCmdSeenMock\n")
			r.updateLatestSeenMock(cmdIdToAdd.Cmd.ClientId, cmdIdToAdd)
		}
	}
	dlog.Printf("addToQueueIfNotPresentMock() queue is now %v\n", r.latestCmdSeenMock)
	if SANITY_CHECK && len(r.latestCmdSeenMock) > MAX_CLIENTS {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			r.printStats()
			fmt.Printf("cmdQ too large, addCmdIdsQueueMock called from %s:%v \n", details.Name(), no)
		}
		panic("cmdQ too large")
	}
}

func (r *Replica) addCmdIdsToQueueIfNotPresent(cmdIdsToAdd []state.CommandAvi, cmdIdsToCheck []state.CommandAvi) {
	// return
	dlog.Printf("addToQueueIfNotPresent() checking if any of %v is in %v and adding to Q if not\n", cmdIdsToAdd, cmdIdsToCheck)
	for _, cmdIdToAdd := range cmdIdsToAdd {
		// todo let's just create duplicates and see what throughput is
		found := false
		if !found {
			dlog.Printf("Adding to latestCmdSeen\n")
			r.updateLatestSeen(cmdIdToAdd.Cmd.ClientId, cmdIdToAdd)
		}
	}
	dlog.Printf("addToQueueIfNotPresent() queue is now %v\n", r.latestCmdSeen)
	if SANITY_CHECK && len(r.latestCmdSeen) > MAX_CLIENTS {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			r.printStats()
			fmt.Printf("cmdQ too large, addCmdIdsQueue called from %s:%v \n", details.Name(), no)
		}
		panic("cmdQ too large")
	}
}

func (r *Replica) addToQueueIfNotPresentMock(cmds []state.CommandAvi, cmdIds []state.CommandAvi) {
	dlog.Printf("addToQueueIfNotPresentMock() checking if any of %v is in %v and adding to Q if not\n", cmds, cmdIds)
	for _, cmd := range cmds {
		dlog.Printf("Adding to latestCmdSeenMock\n")
		r.updateLatestSeenMock(cmd.Cmd.ClientId, cmd)
	}
	dlog.Printf("addToQueueIfNotPresentMock() queue is now %v\n", r.latestCmdSeenMock)
	if SANITY_CHECK && len(r.latestCmdSeenMock) > MAX_CLIENTS {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			r.printStats()
			fmt.Printf("cmdQ too large, addToQueue called from %s:%v \n", details.Name(), no)
		}
		panic("cmdQ too large")
	}
}

func (r *Replica) addToQueueIfNotPresent(cmds []state.CommandAvi, cmdIds []state.CommandAvi) {
	if BYPASS_LATEST_SEEN {
		return
	}
	dlog.Printf("addToQueueIfNotPresent() checking if any of %v is in %v and adding to Q if not\n", cmds, cmdIds)
	for _, cmd := range cmds {
		dlog.Printf("Adding to latestCmdSeen\n")
		r.updateLatestSeen(cmd.Cmd.ClientId, cmd)
	}
	// log.Printf("addToQueueIfNotPresent() queue is now %v\n", r.latestCmdSeen)
	if SANITY_CHECK && len(r.latestCmdSeen) > MAX_CLIENTS {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			r.printStats()
			fmt.Printf("cmdQ too large, addToQueue called from %s:%v \n", details.Name(), no)
		}
		panic("cmdQ too large")
	}
}

func (r *Replica) addToQueueIfNotPresentOLD(cmds []state.CommandAvi, cmdIds []state.CommandAvi) {
	// return
	dlog.Printf("addToQueueIfNotPresent() checking if any of %v is in %v and adding to Q if not\n", cmds, cmdIds)
	for _, cmd := range cmds {

		// k := getKeyForExecMap(cmd.ClientId, cmd.OpId)
		// fmt.Printf("addCmdIdsToQueue key %v len(execMap) %v cmd %v %v\n", k, len(r.execMap), cmd.ClientId, cmd.OpId)
		// if r.execMap[k] {
		// 	continue
		// }

		// todo let's just create duplicates and see what throughput is
		found := false
		// for _, cmdId := range cmdIds {
		// 	if cmd.ClientId == cmdId.ClientId && cmd.OpId == cmdId.OpId {
		// 		found = true
		// 		break
		// 	}
		// }
		if !found {
			dlog.Printf("Adding to latestCmdSeen\n")
			r.updateLatestSeen(cmd.Cmd.ClientId, cmd)
		}
	}
	dlog.Printf("addToQueueIfNotPresent() queue is now %v\n", r.latestCmdSeen)
	if SANITY_CHECK && len(r.latestCmdSeen) > MAX_CLIENTS {
		if pc, _, no, ok := runtime.Caller(1); ok {
			details := runtime.FuncForPC(pc)
			r.printStats()
			fmt.Printf("cmdQ too large, addToQueue called from %s:%v \n", details.Name(), no)
		}
		panic("cmdQ too large")
	}
}

func (r *Replica) deleteLatestIfHighest(cid uint32, _opid int32) {
	if cmd, ok := r.latestCmdSeen[cid]; ok && cmd.Cmd.OpId <= _opid {
		delete(r.latestCmdSeen, cid)
	}
}

func (r *Replica) deleteLatestIfHighestMock(cid uint32, _opid int32) {
	if cmd, ok := r.latestCmdSeenMock[cid]; ok && cmd.Cmd.OpId <= _opid {
		delete(r.latestCmdSeenMock, cid)
	}
}

func (r *Replica) updateLatestSeenMock(cid uint32, _cmd state.CommandAvi) {
	if cmd, ok := r.latestCmdSeenMock[cid]; !ok || (ok && cmd.Cmd.OpId < _cmd.Cmd.OpId) {
		r.latestCmdSeenMock[cid] = _cmd
	}
}

func (r *Replica) updateLatestSeen(cid uint32, _cmd state.CommandAvi) {
	if BYPASS_LATEST_SEEN {
		return
	}
	if cmd, ok := r.latestCmdSeen[cid]; !ok || (ok && cmd.Cmd.OpId < _cmd.Cmd.OpId) {
		r.latestCmdSeen[cid] = _cmd
	}
}

func (r *Replica) gotRotateQuorum() bool {
	// configuration := r.curConfiguration()
	resp := 0
	// gotDelegateMessage := false

	if SANITY_CHECK && len(r.RotateMessages) != len(r.RotateMessagesMock) {
		log.Panicf("goRotateQuorum() len of RotateMessages (%v) != len of RotateMessagesMock (%v)\n",
			len(r.RotateMessages), len(r.RotateMessagesMock))
	}
	dlog.Printf("phase %v gotRotateQuorum() len(r.RotateMessages) %v\n", r.phase, len(r.RotateMessages))
	for i, _ := range r.RotateMessages {
		if r.isStandby(i) {
			continue
		} else {
			resp++
			// resp += int(configuration.nVotesForDelegate)
			// gotDelegateMessage = true
		}
		// if r.isDelegate(i) {
		// 	dlog.Printf("phase %v gotRotateQuorum() %v was delegate\n", r.phase, i)
		// 	resp += int(configuration.nVotesForDelegate)
		// 	gotDelegateMessage = true
		// } else {
		// 	dlog.Printf("phase %v gotRotateQuorum() %v was not delegate nor standby\n", r.phase, i)
		// 	resp++
		// }
		dlog.Printf("phase %v gotRotateQuorum() %v continuing\n", r.phase, i)
	}
	dlog.Printf("gotRotateQuorum() got %v votes before standby votes\n", resp)
	// if !gotDelegateMessage {
	// 	for rid, _ := range configuration.standByReplicas {
	// 		if _, ok := r.RotateMessages[rid]; ok {
	// 			resp++
	// 		}
	// 	}
	// }
	log.Printf("gotRotateQuorum() got %v total votes\n", resp)
	return resp > 1
}

// TODO make this much simpler...
func (r *Replica) createLocalRttTable() {
	sendTime := make(map[int32]time.Time)
	ping := avicennaproto.Ping{r.Id}
	id := int32(0)
	nResponses := 0
	nPings := 0
	nSent := make(map[int32]int)
	lats := make(map[int32][]float64)
	for id = 0; id < int32(r.N); id++ {
		if id == r.Id {
			continue
		}
		sendTime[id] = time.Now()
		nSent[id] = 1
		lats[id] = make([]float64, 0)
	}
	r.bcastMsg(r.pingRPC, &ping, false)

	// ping each node 100 times
	for nResponses < 100*(r.N-1) || nPings < 100*(r.N-1) {
		select {
		case respS := <-r.pingReplyChan:
			recvTime := time.Now()
			resp := respS.Obj.(*avicennaproto.PingReply)
			if nSent[resp.ReplicaId] > 50 {
				// lats[resp.ReplicaId][nSent[resp.ReplicaId]-51] = float64(recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds())
				lats[resp.ReplicaId] = append(lats[resp.ReplicaId], float64(recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds()))
				if _, ok := r.rttTable[r.Id][resp.ReplicaId]; ok {
					// sum them and take the average later
					// r.rttTable[r.Id][resp.ReplicaId] += recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds()

					// include this and remove average for using the min
					// if recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds() > rtt {
					// 	r.rttTable[r.Id][resp.ReplicaId] = recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds()
					// }
				} else { // create the map
					dlog.Printf("creating\n")
					r.rttTable[r.Id][resp.ReplicaId] = recvTime.Sub(sendTime[resp.ReplicaId]).Microseconds()
				}
			}
			nResponses++
			if nSent[resp.ReplicaId] < 100 {
				nSent[resp.ReplicaId]++
				sendTime[resp.ReplicaId] = time.Now()
				// r.replicaMu[resp.ReplicaId].Lock()
				r.SendMsg(resp.ReplicaId, r.pingRPC, &ping)
				// r.replicaMu[resp.ReplicaId].Unlock()
			}
			break
		case pingFromS := <-r.pingChan:
			pingFrom := pingFromS.Obj.(*avicennaproto.Ping)
			pingReply := avicennaproto.PingReply{r.Id}
			// r.replicaMu[pingFrom.ReplicaId].Lock()
			r.SendMsg(pingFrom.ReplicaId, r.pingReplyRPC, &pingReply)
			// r.replicaMu[pingFrom.ReplicaId].Unlock()
			nPings++
			break
		}
	}

	// average
	// this only needs to iterate through r.Id's map...
	// actually it does because the other maps aren't created yet
	// i rtt
	for i, rRtts := range r.rttTable {
		for j, _ := range rRtts {
			if i != r.Id {
				panic("I'm confused\n")
			}

			// was avg
			_, err := stats.Mean(lats[j])
			if err != nil {
				panic("error calculating mean")
			}

			stdDev, err := stats.StandardDeviation(lats[j])
			if err != nil {
				panic("error calculating stddev")
			}

			// r.rttTable[i][j] = int64(avg + 10*stdDev) //rtt / 50
			max, err := stats.Max(lats[j])
			// log.Printf("max %v avg %v stdDev %v\n", max, avg, stdDev)
			r.rttTable[i][j] = int64(max + 10*stdDev)
			if j != r.Id {
				latsSorted := make([]float64, len(lats[j]))
				copy(latsSorted, lats[j])
				sort.Float64s(latsSorted)
				// log.Printf("Latencies to %v are in time, sorted, min median 90 95 max\n", j)
				// log.Printf("%v\n%v\n%v %v %v %v %v\n", lats[j], latsSorted,
				// latsSorted[0], latsSorted[24], latsSorted[44], latsSorted[47], latsSorted[49])
			}
		}
	}
}

func (r *Replica) exchangeRttTables() {
	r.bcastRttTable()
	for i := 0; i < r.N-1; i++ {
		tableS := <-r.rttTableChan
		r.updateRttTable(tableS.Obj.(*avicennaproto.RttTable))
	}
}

func (r *Replica) setupConfigurations() {
	r.createLocalRttTable()
	r.exchangeRttTables()

	for i, _ := range r.configurations {
		r.configurations[i].coordinator = int32(i)
		r.configurations[i].delegate = int32((i + 1) % r.N)
		r.configurations[i].nVotesForDelegate = 2
		r.configurations[i].acceptors = make(map[int32]bool)
		r.configurations[i].standByReplicas = make(map[int32]bool)
		// r.configurations[i].mockCoordinators = make(map[int32]bool)
		for j := 0; j < r.N; j++ {
			if j != i && int32(j) != (int32((i+r.N-1)%r.N)) {
				r.configurations[i].acceptors[int32(j)] = true
			}
		}
		r.configurations[i].standByReplicas[int32((i+r.N-1)%r.N)] = true
		// }

		// nStandbys := (r.N >> 1) - 1 // a quorum of votes to rotate - 1 for delegate (implicit in >>1) - 1 for next coordinator
		// if SANITY_CHECK && r.N == 7 && nStandbys != 2 {
		// 	log.Panicf("nStandbys wrong, got %v instead of 2\n", nStandbys)
		// }

		// for i, _ := range r.configurations {
		// 	r.configurations[i].mockCoordinator = r.configurations[(i+1)%r.N].coordinator

		// 	for j := 0; j < nStandbys; j++ {
		// 		// choose a standby not a delegate/coordinator/mockcoordinator
		// 		var farthestFromCoordinator int32

		// 		// in a lan throughput maybe depends on if mock coordinator is not a standby
		// 		var k int32 = 0
		// 		for ; k < int32(r.N); k++ {
		// 			if r.configurations[i].coordinator != k && r.configurations[i].mockCoordinator != k && r.configurations[i].delegate != k {
		// 				if _, ok := r.configurations[i].standByReplicas[k]; !ok {
		// 					farthestFromCoordinator = k
		// 				}
		// 			}
		// 		}

		// 		delete(r.configurations[i].acceptors, farthestFromCoordinator)

		// 		r.configurations[i].standByReplicas[farthestFromCoordinator] = true
		// 	}
	}
	// for i, _ := range r.configurations {
	// 	r.configurations[i].mockCoordinator = int32((i + 1) % r.N)
	// }
	r.updateConfigurationsBasedOnRttTable()
	// log.Printf("Replica RTT Table is %v\n", r.rttTable)
	log.Printf("Set configurations: %v\n", r.configurations)
}

var totalMsgParseLat time.Duration
var averMsgParseLat time.Duration
var totalMsgCount int

func (r *Replica) printStats() {
	log.Printf("------------------------------------------------------------")
	// log.Printf("\tCommittedUpTo: %v MockCommittedupto: %v\n\tdups %v\n\tphase %v sentForPhase %v \n\tcrtInstance %v crtInstanceMock %v "+
	// 	"\n\tlen(cmdMap) %v len(cmdQ) %v \n\tcurCoord %v CurMock %v \n\tmaxRotateSize %v maxMockRotateSize %v\n\tnBatches %v avgBatchSize %v\n\n configs %v\n\n rtttable %v\n\n", // client rtts %v",
	// 	r.committedUpTo, r.mockCommittedUpTo,
	// 	dups,
	// 	r.phase, r.sentForPhase,
	// 	r.crtInstance, r.crtInstanceMock,
	// 	len(r.cmdMap), len(r.latestCmdSeen),
	// 	r.curCoordinator(), r.curConfiguration().mockCoordinator,
	// 	r.stats.maxRotateSize, r.stats.maxMockRotateSize,
	// 	r.stats.nBatches, float32(r.stats.total)/float32(r.stats.nBatches),
	// 	r.configurations, r.rttTable) //, r.clientRttTable)
	// if GC_DEBUG_ENABLED {
	// 	var garC debug.GCStats
	// 	debug.ReadGCStats(&garC)
	// 	log.Printf("NumGC: %v; PauseTotal: %v; Pause: %v; LastGC: %v\n", garC.NumGC, garC.PauseTotal, garC.Pause, garC.LastGC)
	// 	log.Printf("Average GC pause: %v\n", time.Duration(int64(garC.PauseTotal)/garC.NumGC))
	// }
	// log.Printf("Stats: %+v\n", r.stats)
	chanSize := len(r.MsgParseLatChan)
	for i := 0; i < chanSize; i++ {
		msgParseLat := <-r.MsgParseLatChan
		totalMsgParseLat += *msgParseLat
		totalMsgCount++
	}
	if chanSize > 0 {
		averMsgParseLat = time.Duration(int(totalMsgParseLat) / totalMsgCount)
		log.Printf("Total handle latency: %v, average latency: %v.\n", totalMsgParseLat, averMsgParseLat)
	}
}

/* Main event processing loop */

func (r *Replica) run() {

	r.ConnectToPeers()
	dlog.Println("Waiting for client connections")
	log.Printf("BATCH_INTERVAL %v\n", BATCH_INTERVAL)

	// Set up configurations
	r.setupConfigurations()
	// make sure genericsmr knows which replica to slowdown
	if r.isCoordinatorForPhase(r.Id, 0) {
		r.IsSlowdownReplica = true
	}

	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
		// go r.executeCommandsMock()
	}

	clockChan = make(chan bool, 1)
	go r.clock()
	go r.batcher()
	go r.evaluateRealAndGhost()
	go r.broadcastCommitToClients()

	totalMsgParseLat = 0
	averMsgParseLat = 0
	totalMsgCount = 0

	// if r.Id == 0 {
	// 	r.IsLeader = true
	// }

	// go r.drainClock()

	// onOffProposeChan := r.ProposeWithExecTimeChan

	warmupTimer := time.NewTimer(10 * time.Second)
	r.warmupDone = false

	if r.timer == nil {
		r.timer = time.NewTimer(progTimerDuration)
	}

	printStatsTimer := &time.Timer{}
	if PRINT_STATS {
		printStatsTimer = time.NewTimer(PRINT_STATS_INTERVAL)
	}

	log.Printf("Starting in phase %v", r.phase)

	// slowdownTimers := &slowdowntimers.SlowdownTimers{}
	log.Printf("Should I start timers?\n")
	if r.IsSlowdownReplica && INJECT_TRANSIENT_SLOWDOWN {
		// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
		r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	} else if r.IsSlowdownReplica && INJECT_LONGLIVED_SLOWDOWN {
		r.slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	} else {
		log.Printf("No.\n")
	}

	// surgMockTimerTrigger := make(chan struct{})
	// surgMockTimerExpired := make(chan struct{})
	// go r.surgMockTimer(surgMockTimerTrigger, surgMockTimerExpired, WINDOW_SIZE)
	// lastSent := time.Time{}
	// commitDiff := r.getMinQuorumLatencyForReplica(r.configurationForPhase(r.phase+1).coordinator, r.curCoordinator()) - r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1)
	// log.Printf("commitDiff between cur %v and next %v is %v\n", r.curCoordinator(), r.configurationForPhase(r.phase+1).coordinator, commitDiff)
	for !r.Shutdown {
		if !r.warmupDone {
			select {
			case <-warmupTimer.C:
				log.Printf("Warmup period is over. Allowing Rotation...\n")
				r.warmupDone = true

			default:
				break
			}
		}

		// inject slowdowns if I should
		// if r.IsSlowdownReplica && INJECT_TRANSIENT_SLOWDOWN {
		// 	slowdownTimers.CheckAndDoSlowdown()
		// }

		empty := false
		for !empty {
			select {
			case p := <-r.slowdownChan:
				// if p == r.phase && !r.sentForPhase && r.phase == 0 {
				if p == r.phase && !r.sentForPhase {
					log.Printf("Slowdown suspects in phase %v rotating\n", p)
					r.bcastRotate()
				} else {
					dlog.Printf("ignoring suspicion phase %v\n", p)
				}
				// r.setTimerIfEarlier(expectedDelay)

			case rotateS := <-r.rotateChan:
				rotate := rotateS.Obj.(*avicennaproto.Rotate)
				log.Printf("Handling RotateChan message for phase %v from %v, received at %v\n", rotate.Phase, rotate.ReplicaId, rotateS.RecvTime)
				// if r.phase == 0 {
				// 	r.handleRotate(rotate)
				// }
				r.handleRotate(rotate)

			// case <-r.timer.C:
			// 	if !r.sentForPhase {
			// 		log.Printf("Progress timer triggers rotation in phase %v\n", r.phase)
			// 		r.bcastRotate()
			// 	} else {
			// 		dlog.Printf("ignoring suspicion phase %v\n", r.phase)
			// 	}
			default:
				if !r.sentForPhase {
					empty = true
				}
			}
		}

		if r.sentForPhase {
			continue
		}
		// hopefully this doesn't get optimized...
		// end := time.Now().Add(2 * time.Microsecond)
		// for time.Now().Before(end) {
		// }

		select {
		case batchedCmds := <-r.batchedCmdsChan:
			if r.sentForPhase {
				log.Printf("We're scheduled for ProposeBatch()\n")
			}
			r.hangleProposeBatch(batchedCmds)

		// case <-clockChan: // batch clock
		// 	//activate the new proposals channel
		// 	// if time.Since(lastSent) > BATCH_INTERVAL {
		// 	onOffProposeChan = r.ProposeWithExecTimeChan
		// 	// }
		// 	break

		// case propose := <-onOffProposeChan:
		// 	//got a Propose from a client
		// 	// log.Printf("Propose with {ClientId %d, CommandId %d)\n", propose.Command.ClientId, propose.Command.OpId)
		// 	// if !receivedAPropose && propose.Command.OpId >= 0 {
		// 	// 	receivedAPropose = true
		// 	// 	genericsmr.ReceivedAPropose = true
		// 	// 	slowdowntimers.BeginFlag = true
		// 	// 	// slowdownTimers.initializeTimers()
		// 	// }
		// 	r.handlePropose(propose)
		// 	//deactivate the new proposals channel to prioritize the handling of protocol messages
		// 	if MAX_BATCH > 100 {
		// 		onOffProposeChan = nil
		// 	}
		// 	// lastSent = time.Now()
		// 	break

		case coordinatorAcceptS := <-r.acceptChan:
			accept := coordinatorAcceptS.Obj.(*avicennaproto.Accept)
			if r.sentForPhase {
				log.Printf("We're scheduled for handlingAccept()\n")
			}
			log.Printf("Handling LongAcceptChan message from replica %v, received at \n", accept.ReplicaId, coordinatorAcceptS.RecvTime)
			r.handleAccept(accept)
			break

		case acceptS := <-r.acceptExecTimeChan:
			if r.sentForPhase {
				log.Printf("We're scheduled for handleAcceptExecTime()\n")
			}
			acceptExectime := acceptS.Obj.(*avicennaproto.AcceptExecTime)
			// log.Printf("Hanlding AccpetChannel for phase %d, received at %v\n", acceptExectime.Phase, acceptS.RecvTime)
			r.handleAcceptExecTime(acceptExectime)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			if r.sentForPhase {
				log.Printf("We're scheduled for handleAcceptReply\n")
			}
			acceptReply := acceptReplyS.Obj.(*avicennaproto.AcceptReply)
			// log.Printf("Handling AcceptReplyChan message for instance %v\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break

		case p := <-r.slowdownChan:
			// if p == r.phase && !r.sentForPhase && r.phase == 0 {
			if p == r.phase && !r.sentForPhase {
				log.Printf("Slowdown suspects in phase %v rotating\n", p)
				r.bcastRotate()
			} else {
				dlog.Printf("ignoring suspicion phase %v\n", p)
			}

		case rotateS := <-r.rotateChan:
			rotate := rotateS.Obj.(*avicennaproto.Rotate)
			log.Printf("Handling RotateChan message for phase %d from %d, received at %v\n", rotate.Phase, rotate.ReplicaId, rotateS.RecvTime)
			// if r.phase == 0 {
			// 	r.handleRotate(rotate)
			// }
			r.handleRotate(rotate)
			break

		case commitS := <-r.commitChan:
			commit := commitS.Obj.(*avicennaproto.Commit)
			if r.sentForPhase {
				log.Printf("We're scheduled for handleCommit()\n")
			}
			//got a Commit message
			// log.Printf("Handling CommitChan message from replica %d, for instance %d, received at %v\n", commit.ReplicaId, commit.Instances[0].Instance, commitS.RecvTime)
			// log.Printf("[Phase %v] Got commit message from replica %v, commit: %v\n", r.phase, commit.ReplicaId, commit)
			r.handleCommit(commit)
			break

		// case <-r.timer.C:
		// 	dlog.Printf("My wait-for-coordinator timer expired not doing anything yet\n")
		// 	break
		// case l := <-r.LatencyChan:
		// 	log.Printf("Got latency message %v\n", *l)
		// 	// don't give it to objectiveFunction if we are trying to Rotate already
		// 	if !r.sentForPhase {
		// 		switch l.MockOrAtLeast {
		// 		case REALCOMMITATLEAST:
		// 		case MOCKCOMMITLATENCY:
		// 			rotate := r.objectiveFunc(r.objectiveFuncMD, avicennaproto.ClientLatency{CmdId: })
		// 		case REALCOMMITLATENCY:
		// 		}
		// 		rotate := r.objectiveFunc(r.objectiveFuncMD, getClientLatencyFromLatency(l),
		// 			l.MockOrAtLeast == REALCOMMITATLEAST, l.MockOrAtLeast == REALCOMMITATLEAST)
		// 		if rotate {
		// 			r.nbWriteToSlowdownChan(r.phase)
		// 			// log.Printf("Would have rotated\n")
		// 		}
		// 	}
		// case rc := <-r.RealCommittedFromClientChan:
		// 	r.stats.nCommittedFromClient++
		// 	dlog.Printf("Got real committed from client message %v\n", *rc)
		// 	// log.Printf("[Phase %v] Got real committed from client: %v\n", r.phase, *rc)
		// 	// commit before possibly rotating
		// 	if inst := r.instanceSpace[rc.Instance]; inst == nil || inst.status != COMMITTED {
		// 		// log.Printf("Committing in RealCommitted from client\n")
		// 		r.commit(rc.Instance, &rc.Commands, nil, true)
		// 		// should we bcast commit?
		// 	}
		// 	// TODO need to change objective function for this
		// 	if r.warmupDone {
		// 		rotate := r.objectiveFunc(r.objectiveFuncMD, avicennaproto.ClientLatency{state.CommandId{rc.ClientId, rc.OpId}, rc.Timestamp},
		// 			true, false, true, 0)
		// 		if rotate {
		// 			// log.Printf("Want to rotate from mock committed message\n")
		// 			r.nbWriteToSlowdownChan(r.phase)
		// 		}
		// 	}

		// case mc := <-r.MockCommittedFromClientChan:
		// 	r.stats.nMockCommittedFromClient++
		// 	// log.Printf("[Phase %v] Got mock committed from client message %v\n", r.phase, *mc)

		// 	if r.warmupDone {
		// 		rotate := r.objectiveFunc(r.objectiveFuncMD, avicennaproto.ClientLatency{state.CommandId{mc.ClientId, mc.OpId}, mc.Timestamp},
		// 			false, false, true, 0)
		// 		if rotate {
		// 			// log.Printf("Want to rotate from mock committed message\n")
		// 			r.nbWriteToSlowdownChan(r.phase)
		// 		}
		// 	}

		// case commitLat := <-r.CommitLatencyFeedBackChan:
		// 	// r.stats.nRealCommitAtLeast++
		// 	// log.Printf("[Phase %d] Got commit latency feedback %v\n", r.phase, commitLat)

		// 	if r.warmupDone {
		// 		r.clientCommitLatChan <- avicennaproto.ClientCommitLatency{CmdId: commitLat.CommandId,
		// 			RealCommitLatency: commitLat.RealCommitLatency, GhostCommitLatency: commitLat.GhostCommitLatency}
		// 	}

		// if r.warmupDone {
		// 	rotate := r.objectiveFuncCommit(r.objectiveFuncMD, avicennaproto.ClientCommitLatency{CmdId: commitLat.CommandId,
		// 		RealCommitLatency: commitLat.RealCommitLatency, GhostCommitLatency: commitLat.GhostCommitLatency})
		// 	if rotate {
		// 		// log.Printf("Want to rotate from RealCommitAtLeast message\n")
		// 		r.nbWriteToSlowdownChan(r.phase)
		// 	}
		// }

		// if realInst := r.instanceSpace[commitLat.RealInstance]; realInst == nil && realInst.status != COMMITTED {
		// 	r.commit(int32(realInst.accepts), &commitLat.RealInstCmds, nil, true)
		// }

		// case <-surgMockTimerExpired:
		// 	// log.Printf("Surg Mock Timer Expires\n")
		// 	r.surgMock = false

		case client := <-r.RegisterClientIdChan:
			// r.registerClient(client.ClientId, client.Reply)
			ok := r.registerClient(client.ClientId, client.Reply)
			if ok != TRUE {
				panic("Error registering client: writer mismatch\n")
			}
			// rciReply := &genericsmrproto.RegisterClientIdReply{ok}
			// r.ReplyRegisterClientId(rciReply, client.Reply)

		case crt := <-r.ClientRttChan:
			r.clientRttTable[crt.ClientId] = make(map[int32]int64)
			for rid, rtt := range crt.Rtts {
				r.clientRttTable[crt.ClientId][int32(rid)] = rtt
			}
			dlog.Printf("Updated client rtt table %v\n", r.clientRttTable)

		case <-printStatsTimer.C:
			r.printStats()
			printStatsTimer.Reset(PRINT_STATS_INTERVAL)
		default:
		}
		// rn := rand.Float64()
		// dlog.Printf("Rand got %v\n", rn)
		// if rn < .005 {
		// 	r.nbWriteToSlowdownChan(r.phase)
		// }
	}
	fmt.Printf("Server quiting...\n")
	r.printStats()
}

// Manage Client Writers
func (r *Replica) registerClient(clientId uint32, writer *bufio.Writer) uint8 {
	w, exists := r.clientWriters[clientId]
	dlog.Printf("Registering client: %v\n", clientId)

	if !exists {
		r.clientWriters[clientId] = writer
		return TRUE
	}

	if w == writer {
		return TRUE
	}

	return FALSE
}

func (r *Replica) updateCommittedUpTo() {
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		r.instanceSpace[r.committedUpTo+1].status == COMMITTED {
		r.committedUpTo++
	}
	dlog.Printf("updateCommittedUpTo() updated to %v\n", r.mockCommittedUpTo)
}

func (r *Replica) updateMockCommittedUpTo() {
	for r.instanceSpaceMock[r.mockCommittedUpTo+1] != nil &&
		r.instanceSpaceMock[r.mockCommittedUpTo+1].status == MOCKCOMMITTED {
		r.mockCommittedUpTo++
	}
	dlog.Printf("updateMockCommittedUpTo() updated to %v\n", r.mockCommittedUpTo)
}

// this shouldn't be copying as fastrpc.Serializable is an interface (which can be a pointer)
func (r *Replica) sendMsgToAllBut(rpcCode uint8, msg fastrpc.Serializable, rid int32) {
	n := r.N - 2
	q := r.Id
	sent := 0

	for sent < n {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] || q == rid {
			continue
		}
		sent++
		// r.replicaMu[q].Lock()
		r.SendMsg(q, rpcCode, msg)
		// r.replicaMu[q].Unlock()
	}
}

func (r *Replica) bcastAcceptMsg(rpcCode uint8, msg fastrpc.Serializable) {
	n := r.N - 1
	// todo no thrifty (yet!?)
	if r.Thrifty {
		panic("No Thrifty yet in Avicenna!")
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
		if r.isStandby(q) {
			// sent++
			continue
		}
		sent++
		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
		// 	r.replicaMu[q].Lock()
		// 	defer r.replicaMu[q].Unlock()

		// 	r.SendMsg(q, rpcCode, msg)
		// }(q, rpcCode, msg)
		// r.replicaMu[q].Lock()
		r.SendMsg(q, rpcCode, msg)
		// r.replicaMu[q].Unlock()
	}
	r.stats.nMsgsSent++
}

func (r *Replica) bcastMsg(rpcCode uint8, msg fastrpc.Serializable, doLog bool) {
	n := r.N - 1
	// todo no thrifty (yet!?)
	if r.Thrifty {
		panic("No Thrifty yet in Avicenna!")
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

		r.SendMsg(q, rpcCode, msg)

		// go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
		// 	if doLog {
		// 		log.Printf("Before sending Rotate to replica %d\n", q)
		// 	}
		// 	r.replicaMu[q].Lock()
		// 	defer r.replicaMu[q].Unlock()
		// 	r.SendMsg(q, rpcCode, msg)
		// 	if doLog {
		// 		log.Printf("Finish sending Rotate to replica %d\n", q)
		// 	}
		// }(q, rpcCode, msg)
	}
	r.stats.nMsgsSent++
}

// this shouldn't be copying as fastrpc.Serializable is an interface (which can be a pointer)
func (r *Replica) bcastRotateMsg(rpcCode uint8, msg fastrpc.Serializable, doLog bool) {
	n := r.N - 1
	// todo no thrifty (yet!?)
	if r.Thrifty {
		panic("No Thrifty yet in Avicenna!")
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
		if r.isMockCoordinatorForPhase(q, r.phase) {
			if doLog {
				log.Printf("Before sending Rotate to replica %d\n", q)
			}
			// r.replicaMu[q].Lock()
			r.SendRotateMsg(q, rpcCode, msg)
			// r.replicaMu[q].Unlock()
			if doLog {
				log.Printf("Finish sending Rotate to replica %d\n", q)
			}
		} else {
			go func(q int32, rpcCode uint8, msg fastrpc.Serializable) {
				if doLog {
					log.Printf("Before sending Rotate to replica %d\n", q)
				}
				// r.replicaMu[q].Lock()
				// defer r.replicaMu[q].Unlock()
				r.SendRotateMsg(q, rpcCode, msg)
				if doLog {
					log.Printf("Finish sending Rotate to replica %d\n", q)
				}
			}(q, rpcCode, msg)
		}
	}
	r.stats.nMsgsSent++
}

// advances the crtInstance to the next nil value and returns it
func (r *Replica) advanceCrtInstanceToNextNil() int32 {
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}
	dlog.Printf("advanceCrtInstance returning %v\n", r.crtInstance)
	return r.crtInstance
}

// advances the crtInstance to the next nil value and returns it
func (r *Replica) advanceMockCrtInstanceToNextNil() int32 {
	for r.instanceSpaceMock[r.crtInstanceMock] != nil {
		r.crtInstanceMock++
	}
	dlog.Printf("advanceMockCrtInstance returning %v\n", r.crtInstanceMock)
	return r.crtInstanceMock
}

// TODO
func (r *Replica) minPathReplicaToNextCoordinator(cfgIdx int) int32 {
	cId := r.configurations[cfgIdx].coordinator
	// ncId := r.configurations[(cfgIdx+1)%r.N].coordinator
	ncId := r.configurations[(cfgIdx+1)%len(r.configurations)].coordinator

	minPathLength := MAXINT64
	ret := int32(0)
	for i := int32(0); i < int32(r.N); i++ {
		if i == cId {
			continue
		}
		pathLength := r.rttTable[cId][i] + r.rttTable[i][ncId]
		if pathLength < minPathLength {
			minPathLength = pathLength
			ret = i
			dlog.Printf("minPath got new min %v %v\n", minPathLength, ret)
		}
	}

	dlog.Printf("minPath returning %v\n", ret)
	return ret
}

type IdLat struct {
	id  int32
	lat int64
}

func (r *Replica) nthFarthestReplicaFrom(rid int32, n int) int32 {
	lats := make([]IdLat, len(r.rttTable[rid]))
	for i, lat := range r.rttTable[rid] {
		lats[i] = IdLat{i, lat}
	}
	sort.SliceStable(lats, func(i, j int) bool { return lats[i].lat < lats[j].lat })
	// log.Printf("nthFarthestReplicaFrom() n %v %v is %v\n", n, r.rttTable[rid], lats[len(lats)-n-1].id)
	return lats[len(lats)-n-1].id
}

func (r *Replica) farthestReplicaFrom(rid int32) int32 {
	max := MININT64
	ret := int32(0)
	for i, _ := range r.rttTable[rid] {
		if r.rttTable[rid][i] > max {
			max = r.rttTable[rid][i]
			ret = int32(i)
		} else if r.rttTable[rid][i] == max {
			// this is required to ensure which replica is chosen is deterministic
			// if there are multiple nodes eqully farthest
			// (map iterations are in an arbitrary order)
			if i > ret {
				ret = i
			}
		}
	}
	dlog.Printf("farthestReplicaFrom() %v is %v\n", r.rttTable[rid], ret)
	return ret
}

func (r *Replica) getMaxEndToEndClientLatencyForReplica(rid int32) int64 {
	return r.getMinQuorumLatencyForReplica(rid, -1) + r.getMaxClientLatencyForReplica(rid)
}

// TODO atm clients are in the same site as replicas. using replica latency
//
//	as client latency for the moment. Should use client latency though.
func (r *Replica) getMaxClientLatencyForReplica(rid int32) int64 {
	// log.Printf("in getMax\n")
	max := int64(0) //;r.rttTable[rid][0]
	for _, rtt := range r.rttTable[rid] {
		if rtt > max {
			max = rtt
		}
	}
	// log.Printf("Max latency for replica %v is %v all rtts %v\n", rid, max, r.rttTable[rid])
	return max
	// tmp := make([]int64, len(r.rttTable[rid]))
	// for i, rtt := range r.rttTable[rid] {
	// 	// if i is woRid we want to ignore it, just make the RTT huge so it is last
	// 	tmp[i] = rtt
	// 	if i == woRid {
	// 		tmp[i] = MAXINT64
	// 	}
	// }
	// sort.SliceStable(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	// if SANITY_CHECK {
	// 	if len(tmp) != r.N {
	// 		panic("getMinQuorumLatencyForReplica() wrong map size")
	// 	}
	// }
	// return tmp[(r.N >> 1)]
}

// woRid < 0 compares with all replicas
func (r *Replica) getMinQuorumLatencyForReplica(rid int32, woRid int32) int64 {
	tmp := make([]int64, len(r.rttTable[rid]))
	for i, rtt := range r.rttTable[rid] {
		// if i is woRid we want to ignore it, just make the RTT huge so it is last
		tmp[i] = rtt
		if i == woRid {
			tmp[i] = MAXINT64
		}
	}
	sort.SliceStable(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	if SANITY_CHECK {
		if len(tmp) != r.N {
			panic("getMinQuorumLatencyForReplica() wrong map size")
		}
	}
	return tmp[(r.N >> 1)]
}

func copyConfig(config Configuration) Configuration {
	ret := Configuration{coordinator: config.coordinator,
		delegate:          config.delegate,
		nVotesForDelegate: config.nVotesForDelegate,
		acceptors:         make(map[int32]bool),
		mockCoordinator:   config.mockCoordinator,
		// mockCoordinators:  make(map[int32]bool),
		standByReplicas: make(map[int32]bool)}
	for k, v := range config.acceptors {
		ret.acceptors[k] = v
	}
	// for k, v := range config.mockCoordinators {
	// 	ret.acceptors[k] = v
	// }
	for k, v := range config.standByReplicas {
		ret.acceptors[k] = v
	}
	return ret
}

func (r *Replica) updateConfigurationsBasedOnRttTable() {
	// sort based on quorum latencies to get the coordinator in the right order
	if SORT_RTT {
		sort.SliceStable(r.configurations, func(i, j int) bool {
			// return r.getMaxClientLatencyForReplica(int32(i)) < r.getMaxClientLatencyForReplica(int32(j))
			maxe2eI := r.getMaxEndToEndClientLatencyForReplica(int32(i))
			maxe2eJ := r.getMaxEndToEndClientLatencyForReplica(int32(j))
			if (maxe2eI-maxe2eJ < int64(5*time.Millisecond)) || (maxe2eJ-maxe2eI < int64(5*time.Millisecond)) {
				return r.getMinQuorumLatencyForReplica(int32(i), -1) < r.getMinQuorumLatencyForReplica(int32(j), -1)
			}
			// return r.getMaxEndToEndClientLatencyForReplica(int32(i)) < r.getMaxEndToEndClientLatencyForReplica(int32(j))
			return maxe2eI < maxe2eJ
		})
	}

	log.Printf("Configurations after first sort %v\n", r.configurations)
	// log.Printf("Configurations after second sort %v\n", r.configurations)

	// r.configurations = r.configurations[:SLOWDOWNS_TO_TOLERATE+1] // 0:2 gives [0], [1]
	for i := range r.configurations {
		// log.Printf("setting config %v %v\n", i, r.configurations[i])
		r.configurations[i].mockCoordinator = r.configurations[(i+1)%len(r.configurations)].coordinator
		// log.Printf("set config %v %v\n", i, r.configurations[i])
	}
	// log.Printf("Rtt table %v\n", r.rttTable)
	for i, _ := range r.configurations {
		r.configurations[i].delegate = r.minPathReplicaToNextCoordinator(i)
		for sbr := range r.configurations[i].standByReplicas {
			delete(r.configurations[i].standByReplicas, sbr)
			r.configurations[i].acceptors[sbr] = true // I add to standby add to accept add back to standby?
		}
		// log.Printf("Configurations %v\n", r.configurations)
		nStandbys := (r.N >> 1) - 1 // a quorum of votes to rotate - 1 for delegate (implicit in >>1) - 1 for next coordinator
		if SANITY_CHECK && r.N == 7 && nStandbys != 2 {
			log.Panicf("nStandbys wrong, got %v instead of 2\n", nStandbys)
		}

		for j := 0; j < nStandbys; j++ {
			// choose a standby not a delegate/coordinator/mockcoordinator
			var farthestFromCoordinator int32

			// in a lan throughput maybe depends on if mock coordinator is not a standby
			if ARBITRARY_STANDBY_NO_ROLE {
				var k int32 = 0
				for ; k < int32(r.N); k++ {
					if r.configurations[i].coordinator != k && r.configurations[i].mockCoordinator != k && r.configurations[i].delegate != k {
						if _, ok := r.configurations[i].standByReplicas[k]; !ok {
							farthestFromCoordinator = k
						}
					}
				}
			} else {
				farthestFromCoordinator = r.nthFarthestReplicaFrom(r.configurations[i].coordinator, j)
			}
			delete(r.configurations[i].acceptors, farthestFromCoordinator)

			r.configurations[i].standByReplicas[farthestFromCoordinator] = true
		}

		// log.Printf("Configurations %v\n", r.configurations)
		// update nVotesForDelegate
		r.configurations[i].nVotesForDelegate = int32(1 + len(r.configurations[i].standByReplicas))

		// sanity check(s)
		if SANITY_CHECK {
			if int(r.configurations[i].nVotesForDelegate)-len(r.configurations[i].standByReplicas) != 1 ||
				int(r.configurations[i].nVotesForDelegate)+len(r.configurations[i].acceptors) != r.N {
				p := fmt.Sprintf("Configuration %v %v is incorrect\n", i, r.configurations[i])
				panic(p)
			}
		}
	}

	for i, _ := range r.configArmageddon {
		r.configArmageddon[i].coordinator = r.configurations[i].coordinator
		r.configArmageddon[i].mockCoordinator = r.configArmageddon[i].mockCoordinator
		r.configArmageddon[i].standByReplicas = make(map[int32]bool)
		for j := 0; j < r.N; j++ {
			r.configArmageddon[i].standByReplicas[int32(j)] = false
		}
	}
	log.Printf("Config 0 standbys: %v\n", r.configurations[0].standByReplicas)
	log.Printf("Config 1 standbys: %v\n", r.configurations[1].standByReplicas)

	// // the first configuration is fastest, it is deleted after rotating once.
	// r.configurations[len(r.configurations)-1] = initialConf
	// r.configurations = r.configurations[:SLOWDOWNS_TO_TOLERATE+1]
	// log.Printf("Updated each configuration: %v\n", r.configurations)
	if SANITY_CHECK {
		configString := fmt.Sprintf("then %v \n", r.configurations)
		f := fmt.Sprintf("configs%v", r.Id)
		os.WriteFile(f, []byte(configString), 0644)
	}
}

func (r *Replica) updateConfigurationsBasedOnRttTableOLD() {
	// sort based on quorum latencies to get the coordinator in the right order
	// TODO we should no longer sort based on min quorum latency
	sort.SliceStable(r.configurations, func(i, j int) bool {
		// return r.getMinQuorumLatencyForReplica(int32(i), -1) < r.getMinQuorumLatencyForReplica(int32(j), -1)
		return r.getMaxClientLatencyForReplica(int32(i)) < r.getMaxClientLatencyForReplica(int32(j))
	})
	dlog.Printf("Configurations after first sort %v\n", r.configurations)
	initialConf := copyConfig(r.configurations[0])

	// we need to index configurations while sorting it
	// sorts based on min quorum latency without the previous coordinator
	tmp := make([]Configuration, len(r.configurations))
	copy(tmp, r.configurations)
	sort.SliceStable(tmp, func(i, j int) bool {
		// prevConfIdx := int32(math.Abs(float64((j-1)%len(r.configurations))))

		// was these three
		// prevConfIdx := (j + r.N - 1) % r.N // add N to wrap around -1, keeps it positive
		// return r.getMinQuorumLatencyForReplica(int32(i), r.configurations[prevConfIdx].coordinator) <
		// 	r.getMinQuorumLatencyForReplica(int32(j), r.configurations[prevConfIdx].coordinator)
		return r.getMaxClientLatencyForReplica(int32(i)) < r.getMaxClientLatencyForReplica(int32(j))
	})
	copy(r.configurations, tmp)
	dlog.Printf("Configurations after second sort %v\n", r.configurations)

	// FIXME: something is broken here
	// We want SLOWDOWNS_TO_TOLERATE+1 configurations to be able to rotate through +1 for the initial configuration
	// for i, config := range r.configurations {
	for i := range r.configurations {

		dlog.Printf("setting config %v %v\n", i, r.configurations[i])
		// config.mockCoordinator = r.configurations[(i+1)%(SLOWDOWNS_TO_TOLERATE+2)].coordinator
		r.configurations[i].mockCoordinator = r.configurations[(i+1)%(SLOWDOWNS_TO_TOLERATE+2)].coordinator
		dlog.Printf("set config %v %v\n", i, r.configurations[i])
		// config.mockCoordinators[r.configurations[(i+1)%(SLOWDOWNS_TO_TOLERATE+2)].coordinator] = true
		// config.mockCoordinators[r.configurations[(i+2)%(SLOWDOWNS_TO_TOLERATE+2)].coordinator] = true
	}
	j := 0
	// for i := 0; j < 2; i++ {
	for i := 0; j < 1; i++ {
		if r.configurations[i].coordinator != initialConf.coordinator {
			initialConf.mockCoordinator = r.configurations[i].coordinator
			// initialConf.mockCoordinators[r.configurations[i].coordinator] = true
			j++
		}
	}
	dlog.Printf("Configurations after second sort %v\n", r.configurations)
	// it's possible without the prev coordinator the coordinator is the first conf and second
	if len(r.configurations) > 1 && r.configurations[0].coordinator == initialConf.coordinator {
		tmp := r.configurations[1]
		r.configurations[1] = r.configurations[0]
		r.configurations[0] = tmp
		dlog.Printf("after swapping init config %v 0 %v  1 %v\n", initialConf, r.configurations[0], r.configurations[1])
	}
	// r.configurations = append([]Configuration{firstConfig}, r.configurations...)
	dlog.Printf("Configurations after adding initial %v\n", r.configurations)
	dlog.Printf("Rtt table %v\n", r.rttTable)

	if SLOWDOWNS_TO_TOLERATE >= r.N {
		panic("Trying to tolerate too many slowdowns\n")
	}

	// the first configuration is fastest, it is deleted after rotating once.
	r.configurations[len(r.configurations)-1] = initialConf
	// append doesn't permanently append it
	for i, _ := range r.configurations {
		r.configurations[i].delegate = r.minPathReplicaToNextCoordinator(i)
		for sbr := range r.configurations[i].standByReplicas {
			delete(r.configurations[i].standByReplicas, sbr)
			r.configurations[i].acceptors[sbr] = true
		}
		nStandbys := (r.N >> 1) - 1 // a quorum of votes to rotate - 1 for delegate (implicit in >>1) - 1 for next coordinator
		if SANITY_CHECK && r.N == 7 && nStandbys != 2 {
			log.Panicf("nStandbys wrong, got %v instead of 2\n", nStandbys)
		}

		// cch added recently...
		for j := 0; j < nStandbys; j++ {
			farthestFromCoordinator := r.nthFarthestReplicaFrom(r.configurations[i].coordinator, j)
			delete(r.configurations[i].acceptors, farthestFromCoordinator)
			r.configurations[i].standByReplicas[farthestFromCoordinator] = true
		}

		// update nVotesForDelegate
		r.configurations[i].nVotesForDelegate = int32(1 + len(r.configurations[i].standByReplicas))

		// sanity check(s)
		if SANITY_CHECK {
			if int(r.configurations[i].nVotesForDelegate)-len(r.configurations[i].standByReplicas) != 1 ||
				int(r.configurations[i].nVotesForDelegate)+len(r.configurations[i].acceptors) != r.N {
				p := fmt.Sprintf("Configuration %v %v is incorrect\n", i, r.configurations[i])
				panic(p)
			}
		}
	}

	// // the first configuration is fastest, it is deleted after rotating once.
	// r.configurations[len(r.configurations)-1] = initialConf
	// r.configurations = r.configurations[:SLOWDOWNS_TO_TOLERATE+1]
	dlog.Printf("Updated each configuration: %v\n", r.configurations)
	if SANITY_CHECK {
		configString := fmt.Sprintf("%v then %v \n", initialConf, r.configurations)
		f := fmt.Sprintf("configs%v", r.Id)
		os.WriteFile(f, []byte(configString), 0644)
	}
}

func (r *Replica) updateRttTable(table *avicennaproto.RttTable) {
	for idx, rtt := range table.RttTable {
		i := int32(idx / r.N)
		j := int32(idx % r.N)
		dlog.Printf("local index for idx %v is %v %v\n", idx, i, j)
		if _, iok := r.rttTable[i]; !iok {
			r.rttTable[i] = make(map[int32]int64)
		}
		if rtt > -1 {
			r.rttTable[i][j] = rtt
		}
	}
	dlog.Printf("updateRttTable() updated table from message: %v is now %v\n",
		table, r.rttTable)
}

func (r *Replica) bcastRttTable() {
	// gobin-codegen can't do maps or nest arrays
	// RttTable is thus the flattened map
	// as of now rttTable is at most r.Nxr.N
	var table avicennaproto.RttTable
	table.RttTable = make([]int64, r.N*r.N)
	for i := 0; i < r.N; i++ {
		for j := 0; j < r.N; j++ {
			idx := r.N*i + j
			if i == j {
				table.RttTable[idx] = 0
				continue
			}
			if _, iok := r.rttTable[int32(i)]; iok {
				if _, jok := r.rttTable[int32(i)][int32(j)]; jok {
					table.RttTable[idx] = r.rttTable[int32(i)][int32(j)]
				} else {
					table.RttTable[idx] = -1
				}
			} else {
				table.RttTable[idx] = -1
			}
		}
	}
	dlog.Printf("bcastRttTable() in mem: %v in message: %v\n", r.rttTable, table.RttTable)

	r.bcastMsg(r.rttTableRPC, &table, false)
}

func (r *Replica) replyToClientPing(prop *genericsmr.ProposeWithExecTime) {
	if SANITY_CHECK && prop.Command.OpId >= 0 {
		panic(fmt.Sprintf("Client OpId >= 0 in replyToClientPing %v %v\n", prop.Command.ClientId, prop.Command.OpId))
	}
	for {
		if writer, ok := r.clientWriters[prop.Command.ClientId]; ok {
			propreply := &genericsmrproto.ProposeReplyTS{
				OK:        genericsmrproto.CLIENT_PING_REPLY,
				CommandId: -r.Id,
				Value:     0,
				Timestamp: prop.EndToEndLatency.Latency}
			if err := r.ReplyProposeTS(propreply, writer); err != nil {
				panic(fmt.Sprintf("Error replying to client: %v", err))
			}
			break // have the writer
		} else {
			select {
			case client := <-r.RegisterClientIdChan:
				dlog.Printf("Registering client %v in replyToClientPing\n", client.ClientId)
				r.registerClient(client.ClientId, client.Reply)
			default:
				log.Panicf("No client registration with missing writer for pinging client %v\n", prop.Command.ClientId)
			}
		}
		// log.Panicf("No client writer for ping %v %v\n", prop.Command.ClientId, prop.Command.OpId)
	}
}

// func (r *Replica) bcastMockAccept() {
// 	// cmdMetadataLock.Lock()
// 	// defer cmdMetadataLock.Unlock()

// 	propsToPlaceBack := make([]*genericsmr.Propose, 0)
// 	propsToMockAccept := make([]*genericsmr.Propose, 0)
// 	batchSize := len(r.ProposeWithExecTimeChan)
// 	dlog.Printf("bcastMockAccept len ProposeWithExecTimeChan is %v \n", batchSize)
// 	if batchSize > MAX_BATCH {
// 		batchSize = MAX_BATCH
// 	}

// 	for i := 0; i < batchSize; i++ {
// 		prop := <-r.ProposeWithExecTimeChan
// 		if prop.Command.OpId < 0 {
// 			r.replyToClientPing(prop)
// 			continue
// 		}
// 		// if prop.Command.ClientId
// 		// for cmdId := range r.latestCmdSeen {
// 		// 	if prop.Command.ClientId == cmdId.ClientId && cmdId.OpId < prop.Command.OpId {
// 		// 		delete(r.latestCmdSeen, cmdId)
// 		// 	}
// 		// }
// 		// todo this all needs to be fixed and done properly
// 		cmdId := state.CommandId{prop.Command.ClientId, prop.Command.OpId}
// 		r.cmdMap[cmdId] = prop.Command
// 		r.updateLatestSeen(cmdId.ClientId, cmdId.OpId)
// 		propsToPlaceBack = append(propsToPlaceBack, prop)
// 		if _, ok := r.cmdMetadata[state.CommandId{prop.Command.ClientId, prop.Command.OpId}]; ok {
// 			continue
// 		}

// 		propsToMockAccept = append(propsToMockAccept, prop)
// 		r.cmdMetadata[state.CommandId{prop.Command.ClientId, prop.Command.OpId}] = &CommandMetadata{status: NULL}
// 	}
// 	// for _, prop := range propsToPlaceBack {
// 	// 	dlog.Printf("Putting %v back into ProposeWithExecTimeChan\n", prop)
// 	// 	r.ProposeWithExecTimeChan <- prop
// 	// }

// 	batchSize = len(propsToMockAccept)
// 	if batchSize == 0 {
// 		return
// 	}

// 	mockInstNo := r.advanceMockCrtInstanceToNextNil()

// 	// why is 0 case done here?
// 	cmds := make([]state.Command, batchSize)
// 	var cmdId state.CommandId
// 	for i := 0; i < batchSize; i++ {
// 		prop := propsToMockAccept[i]

// 		cmds[i] = prop.Command

// 		// add to cmdMap that holds CommandId->Command
// 		cmdId.ClientId = cmds[i].ClientId
// 		cmdId.OpId = cmds[i].OpId
// 		r.cmdMap[cmdId] = cmds[i]
// 		// dlog.Printf("handlePropose() Added %v %v to cmdMap\n", cmdId.ClientId, cmdId.OpId)
// 	}

// 	// create cmdIds for the accept request
// 	cmdIds := make([]state.CommandId, batchSize)
// 	for i, cmd := range cmds {
// 		cmdIds[i] = state.CommandId{ClientId: cmd.ClientId, OpId: cmd.OpId}
// 	}
// 	if MOCK {
// 		r.instanceSpaceMock[mockInstNo] = createInstance(cmds, MOCKACCEPTED, 0)
// 		r.crtInstanceMock++
// 	}

// 	if !r.isCoordinator(r.Id) {
// 		// I still want to send this if I sent rotate
// 		// and am mock coordinator (other nodes still need to be able to detect if the coordinator is slow)
// 		if !r.sentForPhase {
// 			dlog.Printf("handlePropose() Creating instance %v in handlePropose as not coordinator\n", mockInstNo)
// 			r.instanceSpace[mockInstNo] = createInstance(cmds, NULL, 0)
// 			r.crtInstance++
// 		}
// 		if MOCK && r.isCurMockCoordinator() {
// 			// dlog.Println("handlePropose() Sending MockAccept ", r.Id, " lp ", mockInstNo)
// 			r.bcastAccept(mockInstNo, cmdIds)
// 		}
// 	}
// }

// when it's piggybacked on a propose it is for the OpId before it
func getClientLatency(propose *genericsmr.ProposeWithExecTime) avicennaproto.ClientLatency {
	return avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: propose.Command.ClientId,
		OpId: propose.Command.OpId - 1}, Latency: propose.EndToEndLatency.Latency}
}

func (r *Replica) hangleProposeBatch(batchedCmds BatchedCmds) {
	startProp := time.Now()
	if SANITY_CHECK && r.isCoordinator(r.Id) && r.isCurMockCoordinator() {
		log.Panicf("Replica is the coordinator and mock coordinator curCoordinator is %v mockCoordinators %v phase %v\n",
			r.curCoordinator(), r.curConfiguration().mockCoordinator, r.phase)
	}

	if r.sentForPhase {
		r.batchedCmdsChan <- batchedCmds
		return
	}

	batchSliceNum := len(r.batchedCmdsChan) + 1

	instNo := r.advanceCrtInstanceToNextNil()
	mockInstNo := r.advanceMockCrtInstanceToNextNil()

	DoMock := batchedCmds.DoMock
	cmds := batchedCmds.Cmds

	for i := 1; i < batchSliceNum; i++ {
		newBatchCmds := <-r.batchedCmdsChan
		if newBatchCmds.DoMock {
			DoMock = true
		}

		cmds = append(cmds, newBatchCmds.Cmds...)
	}

	batchSize := len(cmds)
	r.stats.total += int32(batchSize)
	r.stats.nBatches++

	if r.isCoordinator(r.Id) {
		dlog.Printf("handlePropose() Creating instance %v in handlePropose as coordinator\n", instNo)

		// TODO Why do we even do this as the follower?
		// real leader
		r.instanceSpace[instNo] = createInstance(cmds, ACCEPTED, 0, DoMock, r.phase)
		// r.instanceSpace[instNo] = createInstance(batchedCmds.Cmds, ACCEPTED, 0, DoMock)
		r.crtInstance++
		if DoMock {
			// mock follower
			// only want to do the mock work if requsted, including adding to the log
			// but it still needs to be contiguous
			r.instanceSpaceMock[mockInstNo] = createInstance(cmds, RECEIVED, 0, DoMock, r.phase)
			// r.instanceSpaceMock[mockInstNo] = createInstance(batchedCmds.Cmds, RECEIVED, 0, DoMock)
			r.crtInstanceMock++
		}

		if cmds == nil {
			log.Printf("This inst is nil! instNum %d\n", instNo)
		}

		// log.Printf("[Phase %d] Leader about to send inst %d propose %v\n", r.phase, instNo, r.instanceSpace[instNo])
		dlog.Println("handlePropose() Sending Accept ", r.Id, " lp ", instNo)
		r.stats.nAcceptTx++
		var mockExecTimes []genericsmrproto.MockExecTime_
		mockExecTimes = make([]genericsmrproto.MockExecTime_, 0)
		// r.bcastAccept(instNo, cmds)
		r.sync()
		r.bcastAcceptExectime(instNo, cmds, mockExecTimes)
		// r.bcastAcceptExectime(instNo, batchedCmds.Cmds, mockExecTimes)
	} else if DoMock && r.isCurMockCoordinator() {
		// log.Panicf("bad")
		// I still want to send this if I sent rotate
		// and am mock coordinator (other nodes still need to be able to detect if the coordinator is slow)
		dlog.Printf("handlePropose() Creating instance %v in handlePropose as not coordinator\n", instNo)
		// mock leader
		r.instanceSpaceMock[mockInstNo] = createInstance(cmds, MOCKACCEPTED, 0, DoMock, r.phase)
		// r.instanceSpaceMock[mockInstNo] = createInstance(batchedCmds.Cmds, MOCKACCEPTED, 0, DoMock)
		r.crtInstanceMock++
		// real follower
		r.instanceSpace[instNo] = createInstance(cmds, RECEIVED, 0, DoMock, r.phase)
		// r.instanceSpace[instNo] = createInstance(batchedCmds.Cmds, RECEIVED, 0, DoMock)
		r.crtInstance++
		// log.Printf("[Phase %d] GhostLeader about to send inst %d propose %v\n", r.phase, mockInstNo, r.instanceSpaceMock[mockInstNo])

		dlog.Printf("handlePropose() Sending MockAccept for instance %v %v", mockInstNo, *r.instanceSpaceMock[mockInstNo])

		// log.Printf("nCmds in mockAccept %v\n", batchSize)
		r.stats.nMockAcceptTx++
		// r.bcastAccept(mockInstNo, cmds)

		var mockExecTimes []genericsmrproto.MockExecTime_

		select {
		case execTime := <-r.mockExecTime:
			execTimeSize := len(r.mockExecTime) + 1

			// log.Printf("Ghost leader sends execution time, size %d\n", execTimeSize)
			if execTimeSize > MAX_BATCH {
				execTimeSize = MAX_BATCH
			}

			mockExecTimes = make([]genericsmrproto.MockExecTime_, execTimeSize)
			mockExecTimes[0] = execTime
			// if cmdDoMock, exists := r.cmdMap[execTime.CommandId]; exists && cmdDoMock.DoMock {
			// 	mockExecTimes[0].DoMock = true
			// }

			for i := 1; i < execTimeSize; i++ {
				exec := <-r.mockExecTime
				mockExecTimes[i] = exec
				// if cmdDoMock, exists := r.cmdMap[exec.CommandId]; exists && cmdDoMock.DoMock {
				// 	mockExecTimes[i].DoMock = true
				// }
			}
		default:

		}
		if len(mockExecTimes) > 0 {
			r.ghostExecLatChan <- mockExecTimes
		}

		r.sync()
		r.bcastAcceptExectime(mockInstNo, cmds, mockExecTimes)
		// r.bcastAcceptExectime(mockInstNo, batchedCmds.Cmds, mockExecTimes)
	} else { // only a follower
		// real follower
		r.instanceSpace[instNo] = createInstance(cmds, RECEIVED, 0, DoMock, r.phase)
		// r.instanceSpace[instNo] = createInstance(batchedCmds.Cmds, RECEIVED, 0, DoMock)
		r.crtInstance++
		if DoMock {
			if SANITY_CHECK && r.isCurMockCoordinator() {
				log.Panicf("MockCoordinator mocking but acting as follower\n")
			}
			// mock follower
			r.instanceSpaceMock[mockInstNo] = createInstance(cmds, RECEIVED, 0, DoMock, r.phase)
			// r.instanceSpaceMock[mockInstNo] = createInstance(batchedCmds.Cmds, RECEIVED, 0, DoMock)
			r.crtInstanceMock++
		}
	} // can probably simplify this
	propLat := time.Since(startProp)
	r.MsgParseLatChan <- &propLat
}

// func DoMockRequest(status int32) bool {
// 	return status == int32(MOCK_THIS_DID_NOT_MOCK_LAST) ||
// 		status == int32(MOCK_THIS_MOCK_LAST)
// }
// func MockedLastRequest(status int32) bool {
// 	return status == int32(MOCK_THIS_MOCK_LAST) ||
// 		status == int32(DO_NOT_MOCK_THIS_DID_MOCK_LAST)
// }

// the batching mechanism calls this when the batch is closed
// every replica actually does all of this except create the instance
// mockers create the instance and put it in the mock instance space
func (r *Replica) handlePropose(propose *genericsmr.ProposeWithExecTime) {
	// log.Printf("Handling propose %v\n", propose)
	if SANITY_CHECK && r.isCoordinator(r.Id) && r.isCurMockCoordinator() {
		log.Panicf("Replica is the coordinator and mock coordinator curCoordinator is %v mockCoordinators %v phase %v\n",
			r.curCoordinator(), r.curConfiguration().mockCoordinator, r.phase)
	}
	for propose.Command.OpId < 0 {
		r.replyToClientPing(propose)
		select {
		case propose = <-r.ProposeWithExecTimeChan:
		default:
			return
		}
	}
	// don't bother with new requests if I'm trying to rotate
	// if I've sent Rotate for this phase, we should queue the request explicitly?
	// and pull from that queue when we send proposals (or even empty rotate messages?)
	// we even want to mock, but we  can't change our rotate message...

	// todo we can't change the state of the real log, but we want to keep sending mock messages // no we don't
	if r.sentForPhase {
		dlog.Printf("phase %v rotate sent already in handlePropose; doing nothing\n", r.phase)
		dlog.Printf("I already sent rotate for this phase, ignoring propose\n")
		r.ProposeWithExecTimeChan <- propose
		// r.bcastMockAccept()
		return
	}

	batchSize := len(r.ProposeWithExecTimeChan) + 1
	dlog.Printf("handlePropose len ProposeWithExecTimeChan taking one out is %v \n", batchSize)
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	instNo := r.advanceCrtInstanceToNextNil()
	mockInstNo := r.advanceMockCrtInstanceToNextNil()
	dlog.Printf("handlePropose() Batched %v for instance %v mock instance %v phase %v\n", batchSize, instNo, mockInstNo, r.phase)
	r.stats.total += int32(batchSize)
	r.stats.nBatches++

	doMock := false
	cmdDoMock := false
	cmds := make([]state.CommandAvi, batchSize)

	if propose.EndToEndLatency.Latency > 0 && r.warmupDone {
		r.clientRealE2ELatChan <- avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: propose.Command.ClientId, OpId: propose.EndToEndLatency.CommandId}, Latency: propose.EndToEndLatency.Latency}
	}

	// if propose.Command.OpId >= r.latestCmdSeen[propose.Command.ClientId].OpId {
	// if propose.EndToEndLatency.Latency > 0 && r.warmupDone { //!= -1 {
	// 	log.Printf("Propose with e2e latency, client: %d, e2eLatency: %d, e2eLatCmdId: %d\n", propose.Command.ClientId, propose.EndToEndLatency.Latency, propose.EndToEndLatency.CommandId)
	// 	if r.ObjectiveFuncRealE2E(r.objectiveFuncMD, // interface
	// 		avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: propose.Command.ClientId, OpId: propose.EndToEndLatency.CommandId}, Latency: propose.EndToEndLatency.Latency}) {
	// 		r.nbWriteToSlowdownChan(r.phase)
	// 	}
	// }

	if propose.CommandId > 0 {
		cmdDoMock = true
		doMock = true
	}

	cmds[0] = state.CommandAvi{Cmd: propose.Command, DoMock: cmdDoMock == true}
	cmdId := state.CommandId{ClientId: cmds[0].Cmd.ClientId, OpId: cmds[0].Cmd.OpId}
	dlog.Printf("handlePropose() Added %v %v to cmdMap\n", cmdId.ClientId, cmdId.OpId)

	// r.cmdMapLock.Lock()
	r.cmdMap[cmdId] = CommandDoMock{cmds[0].Cmd, cmdDoMock}
	// r.cmdMapLock.Unlock()
	// } else {
	// log.Printf("Receive an old command: client %d, cmdId %d\n", propose.Command.ClientId, propose.Command.OpId)
	// }

	// log.Printf("Propose with {ClientId %d, CommandId %d)\n", propose.Command.ClientId, propose.Command.OpId)

	// todo I don't know why followers not advancing this hurts latency

	// cch: there is a clock channel that waits and turns on the propose channel
	//		after the batch timeout

	// if r.surgMock {
	// 	cmdDoMock = true
	// 	doMock = true
	// } else if propose.CommandId > 0 {
	// 	cmdDoMock = true
	// 	doMock = true
	// }

	// cmdsMap := make(map[uint32]state.Command)

	// only take the highest command from the same client, cmdsMap copied below
	// if cmds[0].OpId >= cmdsMap[cmds[0].ClientId].OpId {
	// 	cmdsMap[cmds[0].ClientId] = cmds[0]
	// }

	// r.deleteLatestIfHighest(propose.Command.ClientId, propose.Command.OpId)

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeWithExecTimeChan
		if prop.Command.OpId < 0 {
			r.replyToClientPing(prop)
			continue
		}

		if prop.EndToEndLatency.Latency > 0 && r.warmupDone {
			r.clientRealE2ELatChan <- avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: prop.Command.ClientId, OpId: prop.EndToEndLatency.CommandId}, Latency: prop.EndToEndLatency.Latency}
		}
		// if prop.Command.OpId >= r.latestCmdSeen[prop.Command.ClientId].OpId {
		// if prop.EndToEndLatency.Latency > 0 && r.warmupDone { //!= -1 {
		// 	log.Printf("Propose with e2e latency, client: %d, e2eLatency: %d, e2eLatCmdId: %d\n", prop.Command.ClientId, prop.EndToEndLatency.Latency, prop.EndToEndLatency.CommandId)
		// 	if r.ObjectiveFuncRealE2E(r.objectiveFuncMD, // interface
		// 		avicennaproto.ClientLatency{CmdId: state.CommandId{ClientId: prop.Command.ClientId, OpId: prop.EndToEndLatency.CommandId}, Latency: prop.EndToEndLatency.Latency}) {
		// 		r.nbWriteToSlowdownChan(r.phase)
		// 		// log.Printf("Would have rotated in handlePropose()\n")
		// 	}
		// }

		if prop.CommandId > 0 {
			cmdDoMock = true
			doMock = true
		} else {
			cmdDoMock = false
		}

		cmds[i] = state.CommandAvi{Cmd: prop.Command, DoMock: cmdDoMock}
		cmdId := state.CommandId{ClientId: prop.Command.ClientId, OpId: prop.Command.OpId}
		cmdId.ClientId = cmds[i].Cmd.ClientId
		cmdId.OpId = cmds[i].Cmd.OpId

		// r.cmdMapLock.Lock()
		r.cmdMap[cmdId] = CommandDoMock{cmds[i].Cmd, cmdDoMock}
		// r.cmdMapLock.Unlock()
		dlog.Printf("handlePropose() Added %v %v to cmdMap\n", cmdId.ClientId, cmdId.OpId)
		// } else {
		// 	log.Printf("Receive an old command: client %d, cmdId %d\n", prop.Command.ClientId, prop.Command.OpId)
		// }

		// If any one command in an instance requires Mock, the entire instance is Mock processed
		// if r.surgMock {
		// 	cmdDoMock = true
		// 	doMock = true
		// } else if prop.CommandId > 0 {
		// 	cmdDoMock = true
		// 	doMock = true
		// } else {
		// 	cmdDoMock = false
		// }

		// doMock = prop.CommandId > 0

		// log.Printf("Propose with {ClientId %d, CommandId %d)\n", prop.Command.ClientId, prop.Command.OpId)

		// only take the highest command from the same client, cmdsMap copied below
		// log.Printf("cmdsi %v opid %v map %v elem %v opid %v\n", cmds[i], cmds[i].OpId, cmdsMap, cmdsMap[cmds[i].ClientId], cmdsMap[cmds[i].ClientId].OpId)
		// if cmds[i].OpId >= cmdsMap[cmds[i].ClientId].OpId {
		// 	// log.Printf("In if\n")
		// 	cmdsMap[cmds[i].ClientId] = cmds[i]
		// }

		// r.deleteLatestIfHighest(prop.Command.ClientId, prop.Command.OpId)
		// add to cmdMap that holds CommandId->Command

	}

	// TODO why have this intermediate copy?
	// create cmdIds for the accept request
	cmdIds := make([]state.CommandId, batchSize)
	for i, cmd := range cmds {
		cmdIds[i] = state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}
	}

	// original attempt
	// mockCmdsMap := make(map[uint32]state.Command)
	// log.Printf("cmds %v\n", cmds)
	// for _, cmd := range cmds {
	// 	if cmd.OpId >= mockCmdsMap[cmd.ClientId].OpId {
	// 		mockCmdsMap[cmd.ClientId] = cmd
	// 	}
	// }
	// log.Printf("mockCmdsMap %v\n", mockCmdsMap)
	// cmdsMock := make([]state.Command, 0)
	// for _, cmd := range mockCmdsMap {
	// 	cmdsMock = append(cmdsMock, cmd)
	// }
	// log.Printf("cmdsMock %v\n", cmdsMock)
	// cmdsMock = cmds

	// log.Printf("before cmds %v cmdsMap %v\n", cmds, cmdsMap)
	// cmdsTmp := make([]state.Command, 0)
	// for _, cmd := range cmdsMap {
	// 	cmdsTmp = append(cmdsTmp, cmd)
	// }
	// cmds = cmdsTmp
	// log.Printf("after cmdsTmp %v cmds %v cmdsMap %v\n", cmdsTmp, cmds, cmdsMap)

	// if MOCK { // true
	// 	r.instanceSpaceMock[mockInstNo] = createInstance(cmds, MOCKACCEPTED, 0)
	// 	r.crtInstanceMock++
	// }

	// log.Printf("Calling sync() in handlePropose()\n")
	r.sync()

	if r.isCoordinator(r.Id) {
		dlog.Printf("handlePropose() Creating instance %v in handlePropose as coordinator\n", instNo)

		// TODO Why do we even do this as the follower?
		// real leader
		r.instanceSpace[instNo] = createInstance(cmds, ACCEPTED, 0, doMock, r.phase)
		r.crtInstance++
		if doMock {
			// mock follower
			// only want to do the mock work if requsted, including adding to the log
			// but it still needs to be contiguous
			r.instanceSpaceMock[mockInstNo] = createInstance(cmds, NULL, 0, doMock, r.phase)
			r.crtInstanceMock++
		}

		if cmds == nil {
			log.Printf("This inst is nil! instNum %d\n", instNo)
		}

		dlog.Printf("handlePropose() Sending Real Accept for instance %v %v", instNo, *r.instanceSpace[instNo])
		dlog.Println("handlePropose() Sending Accept ", r.Id, " lp ", instNo)
		r.stats.nAcceptTx++
		var mockExecTimes []genericsmrproto.MockExecTime_
		mockExecTimes = make([]genericsmrproto.MockExecTime_, 0)
		// r.bcastAccept(instNo, cmds)
		r.bcastAcceptExectime(instNo, cmds, mockExecTimes)
	} else if doMock && r.isCurMockCoordinator() {
		// log.Panicf("bad")
		// I still want to send this if I sent rotate
		// and am mock coordinator (other nodes still need to be able to detect if the coordinator is slow)
		dlog.Printf("handlePropose() Creating instance %v in handlePropose as not coordinator\n", instNo)
		// mock leader
		r.instanceSpaceMock[mockInstNo] = createInstance(cmds, MOCKACCEPTED, 0, doMock, r.phase)
		r.crtInstanceMock++
		// real follower
		r.instanceSpace[instNo] = createInstance(cmds, NULL, 0, doMock, r.phase)
		r.crtInstance++

		dlog.Printf("handlePropose() Sending MockAccept for instance %v %v", mockInstNo, *r.instanceSpaceMock[mockInstNo])

		// log.Printf("nCmds in mockAccept %v\n", batchSize)
		r.stats.nMockAcceptTx++
		// r.bcastAccept(mockInstNo, cmds)

		var mockExecTimes []genericsmrproto.MockExecTime_

		select {
		case execTime := <-r.mockExecTime:
			execTimeSize := len(r.mockExecTime) + 1

			// log.Printf("Ghost leader sends execution time, size %d\n", execTimeSize)
			if execTimeSize > MAX_BATCH {
				execTimeSize = MAX_BATCH
			}

			mockExecTimes = make([]genericsmrproto.MockExecTime_, execTimeSize)
			mockExecTimes[0] = execTime
			if cmdDoMock, exists := r.cmdMap[execTime.CommandId]; exists && cmdDoMock.DoMock {
				mockExecTimes[0].DoMock = true
			}

			for i := 1; i < execTimeSize; i++ {
				exec := <-r.mockExecTime
				mockExecTimes[i] = exec
				if cmdDoMock, exists := r.cmdMap[exec.CommandId]; exists && cmdDoMock.DoMock {
					mockExecTimes[i].DoMock = true
				}
			}
		default:

		}

		r.bcastAcceptExectime(mockInstNo, cmds, mockExecTimes)
	} else { // only a follower
		// real follower
		r.instanceSpace[instNo] = createInstance(cmds, NULL, 0, doMock, r.phase)
		r.crtInstance++
		if doMock {
			if SANITY_CHECK && r.isCurMockCoordinator() {
				log.Panicf("MockCoordinator mocking but acting as follower\n")
			}
			// mock follower
			r.instanceSpaceMock[mockInstNo] = createInstance(cmds, NULL, 0, doMock, r.phase)
			r.crtInstanceMock++
		}
	} // can probably simplify this

	// if rand.Intn(1000) > 900 {
	// if r.crtInstanceMock%50 == 0 {
	// r.bcastRotate()
	// }

	// if MOCK { // true
	// 	r.instanceSpaceMock[mockInstNo] = createInstance(cmds, MOCKACCEPTED, 0)
	// 	r.crtInstanceMock++
	// }
	// if r.isCoordinator(r.Id) {
	// 	dlog.Printf("handlePropose() Creating instance %v in handlePropose as coordinator\n", instNo)
	// 	r.instanceSpace[instNo] = createInstance(cmds, ACCEPTED, 0)
	// 	r.crtInstance++
	// 	dlog.Println("handlePropose() Sending Accept ", r.Id, " lp ", instNo)
	// 	r.bcastAccept(instNo, cmdIds)
	// } else if !r.isCoordinator(r.Id) {
	// 	// I still want to send this if I sent rotate
	// 	// and am mock coordinator (other nodes still need to be able to detect if the coordinator is slow)
	// 	if !r.sentForPhase {
	// 		dlog.Printf("handlePropose() Creating instance %v in handlePropose as not coordinator\n", instNo)
	// 		r.instanceSpace[instNo] = createInstance(cmds, NULL, 0)
	// 		r.crtInstance++
	// 	}
	// 	if MOCK && r.isCurMockCoordinator() {
	// 		dlog.Println("handlePropose() Sending MockAccept ", r.Id, " lp ", mockInstNo)
	// 		r.bcastAccept(mockInstNo, cmdIds)
	// 	}
	// }

	// to test rotation
	// if !r.sentForPhase {
	// r.bcastRotate()
	// }

	// TODO please simplify this logic and function
	// added because we need to still Mock even when we have sent rotate
	// if r.sentForPhase {
	// 	dlog.Printf("Adding to queue at end of Propose\n")
	// 	r.addToQueueIfNotPresent(cmds, nil)
	// }

	// far far away (or never) todo writing to stable storage
	// r.recordInstanceMetadata(r.instanceSpace[instNo])
	// r.recordCommands(cmds)
	// r.sync()
}

func (r *Replica) bcastLongMockAccept() {
	if SANITY_CHECK {
		if r.sentForPhase {
			panic("MOCK: Sending long accept when I already sent a rotate message\n")
		}
	}
	// send everything we can from last known commit instance
	firstInst := r.mockCommittedUpTo + 1
	lastInst := r.advanceMockCrtInstanceToNextNil() - 1 //send everything this replica knows about
	nInstancesToSend := lastInst - firstInst + 1
	if nInstancesToSend == 0 {
		// dlog.Printf("MOCK: bcastAccept called for 0 instances first %v last %v total %v\n",
		// firstInst, lastInst, nInstancesToSend)
		// return
	}

	var a avicennaproto.Accept
	a.Instances = make([]avicennaproto.InstanceCommands, nInstancesToSend)
	a.Phase = r.phase
	a.ReplicaId = r.Id

	if r.isCurMockCoordinator() {
		for i := 0; i < int(nInstancesToSend); i++ {
			instanceSpaceIdx := firstInst + int32(i)
			if SANITY_CHECK {
				if r.instanceSpaceMock[instanceSpaceIdx] == nil {
					panic("MOCK: nil instance in sending Accept range")
				}
			}
			// cmdIds := r.idsFromCmds(r.instanceSpaceMock[instanceSpaceIdx].cmds)
			a.Instances[i] = avicennaproto.InstanceCommands{Instance: instanceSpaceIdx,
				Status: avicennaproto.MOCKACCEPT, Commands: r.instanceSpaceMock[instanceSpaceIdx].cmds}
			if SANITY_CHECK {
				if len(a.Instances[i].Commands) <= 0 {
					dlog.Printf("MOCK: bcastAccept() bcasting a MockAccept with no command ids instance %v\n", instanceSpaceIdx)
					// panic("bcasting an Accept with no command ids")
				}
			}

			dlog.Printf("MOCK: instance %v sending in LongMockAccept in phase %v for instanceId %v\n",
				instanceSpaceIdx, r.phase, a.Instances[i])
		}

		// add another instance for the cmds that were overwritten
		if len(r.latestCmdSeenMock) > 0 {
			instNo := lastInst + 1
			// dlog.Printf("Calling getAndDeleteQueue from bcastAccept\n")
			cmdIds := r.getAndDeleteQueueMock() // what if mocking was requested for these?
			// maybe it doesn't matter since bcastLong is only for after rotating
			r.instanceSpaceMock[instNo] = createInstance(cmdIds, MOCKACCEPTED, 0, false, r.phase)
			r.crtInstanceMock++
			a.Instances = append(a.Instances, avicennaproto.InstanceCommands{instNo, avicennaproto.MOCKACCEPT, cmdIds, r.phase, FALSE})
		}

		// dlog.Printf("MOCK: Sending MockAccept (LongAccept?) in phase %v for instances %v to %v instanceId message: %v\n",
		// a.Phase, firstInst, lastInst, a)

		if len(a.Instances) <= 0 {
			return
		}
		r.bcastMsg(r.acceptRPC, &a, false)
	} else {
		if MOCK {
			panic("MOCK: Trying to send long Accept as follower?")
		}
	}
}

// TODO convert to new
func (r *Replica) bcastLongAccept() {
	if SANITY_CHECK {
		if r.sentForPhase {
			panic("REAL: Sending long accept when I already sent a rotate message\n")
		}
	}
	// send everything we can from last known commit instance
	firstInst := r.committedUpTo + 1
	lastInst := r.advanceCrtInstanceToNextNil() - 1 //send everything this replica knows about
	nInstancesToSend := lastInst - firstInst + 1
	if nInstancesToSend == 0 {
		// dlog.Printf("REAL: bcastAccept called for 0 instances first %v last %v total %v\n",
		// firstInst, lastInst, nInstancesToSend)
		// return
	}

	var a avicennaproto.Accept
	a.Instances = make([]avicennaproto.InstanceCommands, nInstancesToSend)
	a.Phase = r.phase
	a.ReplicaId = r.Id

	if r.isCoordinator(r.Id) {
		log.Printf("I'm the new real leader, about to send LongAccept, my minimum uncommitted inst: %d\n", firstInst)
		for i := 0; i < int(nInstancesToSend); i++ {
			instanceSpaceIdx := firstInst + int32(i)
			if SANITY_CHECK {
				if r.instanceSpace[instanceSpaceIdx] == nil {
					panic("REAL: nil instance in sending Accept range")
				}
			}
			// cmdIds := r.idsFromCmds(r.instanceSpace[instanceSpaceIdx].cmds)
			a.Instances[i] = avicennaproto.InstanceCommands{Instance: instanceSpaceIdx,
				Status: avicennaproto.ACCEPT, Commands: r.instanceSpace[instanceSpaceIdx].cmds}
			log.Printf("Command in Long accept, inst %d, cmds %v\n", instanceSpaceIdx, a.Instances[i].Commands)
			if SANITY_CHECK {
				if len(a.Instances[i].Commands) <= 0 {
					// dlog.Printf("REAL: bcastAccept() bcasting an Accept with no command ids instance %v\n", instanceSpaceIdx)
					// panic("bcasting an Accept with no command ids")
				}
			}

			// dlog.Printf("REAL: instance %v sending in long Accept in phase %v for instanceId %v\n",
			// instanceSpaceIdx, r.phase, a.Instances[i])
		}

		// add another instance for the cmds that were overwritten
		haveLatestSeem := false
		if len(r.latestCmdSeen) > 0 {
			haveLatestSeem = true
			instNo := lastInst + 1
			// dlog.Printf("REAL: Calling getAndDeleteQueue from bcastAccept\n")
			cmdIds := r.getAndDeleteQueue()
			r.instanceSpace[instNo] = createInstance(cmdIds, ACCEPTED, 0, false, r.phase)
			r.crtInstance++
			a.Instances = append(a.Instances, avicennaproto.InstanceCommands{instNo, avicennaproto.ACCEPT, cmdIds, r.phase, FALSE})
		}

		if len(r.batchedCmdsChan) > 0 {
			batchSliceNum := len(r.batchedCmdsChan) + 0

			var instNo int32
			if haveLatestSeem {
				instNo = lastInst + 2
			} else {
				instNo = lastInst + 1
			}

			cmds := make([]state.CommandAvi, 0)

			for i := 0; i < batchSliceNum; i++ {
				newBatchCmds := <-r.batchedCmdsChan

				cmds = append(cmds, newBatchCmds.Cmds...)
			}
			r.instanceSpace[instNo] = createInstance(cmds, ACCEPTED, 0, false, r.phase)
			r.crtInstance++
			a.Instances = append(a.Instances, avicennaproto.InstanceCommands{instNo, avicennaproto.ACCEPT, cmds, r.phase, FALSE})
		}

		// dlog.Printf("REAL: Sending Accept (LongAccept?) in phase %v for instances %v to %v instanceId message: %v\n",
		// a.Phase, firstInst, lastInst, a)

		if len(a.Instances) <= 0 {
			return
		}
		log.Printf("Broadcast Long Accept!\n")
		r.bcastMsg(r.acceptRPC, &a, false)
	} else {
		if MOCK {
			panic("REAL: Trying to send long Accept as follower?")
		}
	}
}

func (r *Replica) bcastAcceptExectime(instance int32, _cmds []state.CommandAvi, _execTimes []genericsmrproto.MockExecTime_) {
	if SANITY_CHECK && !(r.isCurMockCoordinator() || r.isCoordinator(r.Id)) {
		log.Panicf("REAL: Replica in bcastAccept as neither coordinator nor mock coordinator\n")
	}
	if SANITY_CHECK && r.sentForPhase {
		log.Panicf("REAL: Replica in bcastAccept but sentForPhase\n")
	}

	var a avicennaproto.AcceptExecTime
	a.Instances = make([]avicennaproto.InstanceCommands, 1)
	a.Phase = r.phase
	a.ReplicaId = r.Id
	a.MockExecTimes = _execTimes
	if r.isCoordinator(r.Id) { //&& !r.sentForPhase { // sentForPhase check should be unnecessary
		a.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.ACCEPT, Commands: _cmds}
		// dlog.Printf("REAL: instance %v sending single instance Accept in phase %v for instanceId %v\n", instance, r.phase, a.Instances[0])
	} else { // if current mock coordinator
		if !r.isCurMockCoordinator() {
			log.Panicf("Not current mock coordiantor but I want to send an accept\n")
		}
		a.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.MOCKACCEPT, Commands: _cmds}
		// dlog.Printf("REAL: instance %v sending single instance MockAccept in phase %v for instanceId %v\n", instance, r.phase, a.Instances[0])
	}
	if SANITY_CHECK && len(a.Instances[0].Commands) <= 0 {
		log.Panicf("REAL: bcasting Accept or MockAccept with 0 command ids instance %v\n", instance)
	}

	// r.bcastMsg(r.acceptExecTimeRPC, &a)

	if r.isCoordinator(r.Id) {
		r.bcastAcceptMsg(r.acceptExecTimeRPC, &a)
	} else {
		r.bcastMsg(r.acceptExecTimeRPC, &a, false)
	}
}

func (r *Replica) bcastAccept(instance int32, _cmds []state.CommandAvi) {
	if SANITY_CHECK && !(r.isCurMockCoordinator() || r.isCoordinator(r.Id)) {
		log.Panicf("REAL: Replica in bcastAccept as neither coordinator nor mock coordinator\n")
	}
	if SANITY_CHECK && r.sentForPhase {
		log.Panicf("REAL: Replica in bcastAccept but sentForPhase\n")
	}

	// if instance >= 0 {
	var a avicennaproto.Accept
	a.Instances = make([]avicennaproto.InstanceCommands, 1)
	a.Phase = r.phase
	a.ReplicaId = r.Id
	if r.isCoordinator(r.Id) { //&& !r.sentForPhase { // sentForPhase check should be unnecessary
		a.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.ACCEPT, Commands: _cmds}
		// dlog.Printf("REAL: instance %v sending single instance Accept in phase %v for instanceId %v\n", instance, r.phase, a.Instances[0])
	} else { // if current mock coordinator
		if !r.isCurMockCoordinator() {
			log.Panicf("Not current mock coordiantor but I want to send an accept\n")
		}
		a.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.MOCKACCEPT, Commands: _cmds}
		// dlog.Printf("REAL: instance %v sending single instance MockAccept in phase %v for instanceId %v\n", instance, r.phase, a.Instances[0])
	}
	if SANITY_CHECK && len(a.Instances[0].Commands) <= 0 {
		log.Panicf("REAL: bcasting Accept or MockAccept with 0 command ids instance %v\n", instance)
	}
	r.bcastAcceptMsg(r.acceptRPC, &a)

	// 	dlog.Printf("instance %v sending single instance Accept in phase %v for instanceId %v\n",
	// 		instance, r.phase, a.Instances[0])
	// 	if SANITY_CHECK {
	// 		if len(a.Instances[0].CommandIds) <= 0 {
	// 			dlog.Printf("bcastAccept() bcasting an Accept with no command ids instance %v\n", instance)
	// 			panic("bcasting an Accept with no command ids")
	// 		}
	// 	}
	// 	r.bcastMsg(r.acceptRPC, &a)
	// } else {
	// 	if SANITY_CHECK && !r.isCurMockCoordinator() {
	// 		log.Panicf("Replica not mockCoordinator or real coordinator in bcastAccept\n")
	// 	}
	// 	a.Instances[0] = avicennaproto.InstanceCommandIds{Instance: instance, Status: avicennaproto.MOCKACCEPT, CommandIds: _cmdIds}
	// 	dlog.Printf("instance %v Sending MockAccept in phase %v\n", instance, r.phase)
	// 	if SANITY_CHECK {
	// 		if len(a.Instances[0].CommandIds) <= 0 {
	// 			panic("bcasting a MockAccept with no command ids")
	// 		}
	// 	}
	// 	// the leader should receive it as well with the new slowdown detector
	// 	// r.sendMsgToAllBut(r.acceptRPC, &a, r.curCoordinator())
	// 	r.bcastMsg(r.acceptRPC, &a)
	// }
}

func int64AsMicrosecondDuration(i int64) time.Duration {
	return time.Duration(i) * time.Microsecond
}

// func metadataAsStr(md *CommandMetadata) string {
// 	return fmt.Sprintf("status %v AcceptStart %v.%v AcceptDuration %v AcceptStop %v.%v AcceptFired %v.%v"+
// 		"CommitStart %v.%v CommitDuration %v CommitStop %v.%v CommitFired %v.%v", md.status,
// 		md.acceptTimerStart.Second(), md.acceptTimerStart.Nanosecond()*int(time.Microsecond),
// 		md.acceptTimerDuration,
// 		md.acceptTimerStop.Second(), md.acceptTimerStop.Nanosecond()*int(time.Microsecond),
// 		// md.acceptTimerStop.String(),
// 		md.acceptTimerFired.Second(), md.acceptTimerFired.Nanosecond()*int(time.Microsecond),
// 		// md.acceptTimerFired.String(),
// 		// md.commitTimerStart.String(),
// 		md.commitTimerStart.Second(), md.commitTimerStart.Nanosecond()*int(time.Microsecond),
// 		md.commitTimerDuration,
// 		md.commitTimerStop.Second(), md.commitTimerStop.Nanosecond()*int(time.Microsecond),
// 		md.commitTimerFired.Second(), md.commitTimerFired.Nanosecond()*int(time.Microsecond),
// 	)
// 	// md.commitTimerStop.String(),
// 	// md.commitTimerFired.String())
// }

// The expected time the Accept should arrive given the MockAccept has just arrived
func (r *Replica) getExpectedCommitArrivalTime(clientId uint32, mcId int32) time.Duration {
	// need to start both from a point in time we know was the same (the client sending the command)
	elapsedTime := r.clientRttTable[clientId][mcId]/2 + r.getMinQuorumLatencyForReplica(mcId, r.curCoordinator()) + r.rttTable[mcId][r.Id]/2
	eta := r.clientRttTable[clientId][r.curCoordinator()]/2 + r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1) + r.rttTable[r.curCoordinator()][r.Id]/2

	// the path r.Id -> mock coordinator -> r.Id has already occurred
	// assume rtts are 2x one-way delay
	// elapsedTime := r.rttTable[r.Id][mcId]/2 + r.rttTable[mcId][r.Id]/2
	// the path client -> coord -> r.Id is what we are waiting for
	// assume rtts are 2x one-way delay
	// eta := r.rttTable[r.Id][r.curCoordinator()]/2 + r.rttTable[r.curCoordinator()][r.Id]/2
	timeLeft := eta - elapsedTime

	// dlog.Printf("Calculating commit arrival time \n\tClient->Mock %v MockQuorum %v MockLeader to me %v"+
	// 	"\n\tClient->Leader %v Quorum %v Leader to me %v\n. timeLeft %v",
	// 	r.clientRttTable[clientId][mcId]/2, r.getMinQuorumLatencyForReplica(mcId, r.curCoordinator()), r.rttTable[mcId][r.Id]/2,
	// 	r.clientRttTable[clientId][r.curCoordinator()]/2, r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1), r.rttTable[r.curCoordinator()][r.Id]/2, timeLeft)
	// dlog.Printf("timeleft %v as duration %v as ret val %v\n", timeLeft, time.Duration(timeLeft), time.Duration(timeLeft)*time.Microsecond)
	commitDiff := r.getMinQuorumLatencyForReplica(r.configurationForPhase(r.phase+1).coordinator, r.curCoordinator()) - r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1)
	// dlog.Printf("commitDiff between cur %v and next %v is %v\n", r.curCoordinator(), r.configurationForPhase(r.phase+1).coordinator, commitDiff)
	return int64AsMicrosecondDuration(timeLeft) + time.Duration(commitDiff)
	//r.rttTable[r.curCoordinator()][r.Id]) // * 2 // + 10*time.Millisecond //+ 1*time.Second //remove
}

// The expected time the Accept should arrive given the MockAccept has just arrived
func (r *Replica) getExpectedAcceptArrivalTime(clientId uint32, mcId int32) time.Duration {
	// the path client -> mock coordinator -> r.Id has already occurred
	// assume rtts are 2x one-way delay
	elapsedTime := r.clientRttTable[clientId][mcId]/2 + r.rttTable[mcId][r.Id]/2
	// the path client -> coord -> r.Id is what we are waiting for
	// assume rtts are 2x one-way delay
	eta := r.clientRttTable[clientId][r.curCoordinator()]/2 + r.rttTable[r.curCoordinator()][r.Id]/2
	timeLeft := eta - elapsedTime

	dlog.Printf("Calculating ExpectedArrivalTime %v %v %v %v\n", r.clientRttTable[clientId][mcId]/2, r.rttTable[mcId][r.Id]/2,
		r.clientRttTable[clientId][r.curCoordinator()]/2, r.rttTable[r.curCoordinator()][r.Id]/2)

	dlog.Printf("getExpected elapsedTime %v eta %v\n", elapsedTime, eta)
	dlog.Printf("timeleft %v as duration %v as ret val %v\n", timeLeft, time.Duration(timeLeft), time.Duration(timeLeft)*time.Microsecond)
	commitDiff := r.getMinQuorumLatencyForReplica(r.configurationForPhase(r.phase+1).coordinator, r.curCoordinator()) - r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1)
	dlog.Printf("commitDiff between cur %v and next %v is %v\n", r.curCoordinator(), r.configurationForPhase(r.phase+1).coordinator, commitDiff)
	return int64AsMicrosecondDuration(timeLeft+r.rttTable[r.curCoordinator()][r.Id]) + time.Duration(commitDiff) //* 2 // + 10*time.Millisecond //+ 1*time.Second //TODO ++++++++++++++++++++++++++++++++++++++++++++remove
}

func (r *Replica) stopAllTimers(status InstanceStatus) {
	// cmdMetadataLock.Lock()
	// defer cmdMetadataLock.Unlock()

	for _, md := range r.cmdMetadata {
		if md != nil {
			if md.timer != nil {
				md.timer.Stop()
			}
			md.status = status
		}
	}
}

func (r *Replica) stopTimers(cmdIds []state.CommandId, status InstanceStatus) {
	// cmdMetadataLock.Lock()
	// defer cmdMetadataLock.Unlock()

	for _, cmdId := range cmdIds {
		if md, ok := r.cmdMetadata[cmdId]; ok && md != nil {
			if md.timer != nil {
				md.timer.Stop()
				// if status == ACCEPTED {
				// 	md.acceptTimerStop = time.Now()
				// } else if status == COMMITTED {
				// 	md.commitTimerStop = time.Now()
				// }
				dlog.Printf("cmd %v stopped timer\n", cmdId)
				// delete(r.cmdTimers, cmdId)
			}
		} else {
			r.cmdMetadata[cmdId] = &CommandMetadata{status: NULL}
		}
		r.cmdMetadata[cmdId].status = status
	}
}

// var cmdMetadataLock sync.Mutex

func (r *Replica) startTimerIfNotExist(cmdId state.CommandId, d time.Duration, status InstanceStatus) {

	md := r.cmdMetadata[cmdId]
	if md == nil || md.timer == nil {
		r.cmdMetadata[cmdId] = &CommandMetadata{timer: time.NewTimer(d), status: status}
		// *time.NewTimer(d)
		timer := r.cmdMetadata[cmdId].timer
		// if status == NULL {
		// 	r.cmdMetadata[cmdId].acceptTimerStart = time.Now()
		// 	r.cmdMetadata[cmdId].acceptTimerDuration = d
		// } else if status == ACCEPTED {
		// 	r.cmdMetadata[cmdId].commitTimerStart = time.Now()
		// 	r.cmdMetadata[cmdId].commitTimerDuration = d
		// }

		go func(p int32) {
			// cmdMetadataLock.Lock()
			// dlog.Printf("cmd %v Thread starting to wait for timer in %v md %v\n", cmdId, d, metadataAsStr(r.cmdMetadata[cmdId]))
			// cmdMetadataLock.Unlock()

			<-timer.C
			r.slowdownChan <- p

			// cmdMetadataLock.Lock()
			// defer cmdMetadataLock.Unlock()

			// if status == NULL {
			// 	r.cmdMetadata[cmdId].acceptTimerFired = time.Now()
			// } else if status == ACCEPTED {
			// 	r.cmdMetadata[cmdId].commitTimerFired = time.Now()
			// }

			// dlog.Printf("cmd %v Thread timer fired was duration %v md: %v\n", cmdId, d, metadataAsStr(r.cmdMetadata[cmdId]))
			// dlog.Printf("Thread timer for %v fired\n", cmdId)
		}(r.phase)
	}
}

// func (r *Replica) startTimersIfNotExist(cmdIds []state.CommandId, d time.Duration, status InstanceStatus) {
// 	for _, cmdId := range cmdIds {
// 		r.startTimerIfNotExist(cmdId, d, status)
// 		// 	r.cmdTimers[cmdId] = *time.NewTimer(d)
// 		// 	_, ok := r.cmdTimers[cmdId]
// 		// 	if !ok {
// 		// 		r.cmdTimers[cmdId] = *time.NewTimer(d)
// 		// 		timer := r.cmdTimers[cmdId]

// 		// 		go func(p int32) {
// 		// 			dlog.Printf("cmd %v Thread starting to wait for timer in %v\n", cmdId, d)
// 		// 			<-timer.C
// 		// 			r.slowdownChan <- p
// 		// 			dlog.Printf("cmd %v Thread timer fired was duration %v\n", cmdId, d)
// 		// 			// dlog.Printf("Thread timer for %v fired\n", cmdId)
// 		// 		}(r.phase)
// 		// 	}
// 	}
// }

// func (r *Replica) stoptTimers(cmdIds []state.CommandId) {
// 	for _, cmdId := range cmdIds {
// 		// commit should arrive in one RTT to the coordinator
// 		// timeLeft := int64AsMicrosecondDuration(r.rttTable[r.curCoordinator()][r.Id]) + 10*time.Second // time.Duration(r.rttTable[r.curCoordinator()][r.Id])*time.Microsecond + 10*time.Second // todo remove +
// 		timer, ok := r.cmdTimers[cmdId]
// 		if ok {
// 			timer.Stop()
// 		}
// 	}
// }

// TODO TODO TODO
// func (r *Replica) stopAcceptTimersStartCommitTimers(cmdIds []state.CommandId) {
// 	dlog.Printf("Warmup not done not starting timers\n")
// 	if !r.warmupDone {
// 		return
// 	}
// 	if r.isCoordinator(r.Id) {
// 		panic("Coordinator trying to start timers")
// 		dlog.Printf("Coordinator doesn't think itself is slow?\n")
// 		return
// 	}
// 	// r.stopTimers(cmdIds)
// 	// r.startTimers(cmdIds)
// 	for _, cmdId := range cmdIds {
// 		// commit should arrive in one RTT to the coordinator
// 		timeLeft := int64AsMicrosecondDuration(r.rttTable[r.curCoordinator()][r.Id]) + 10*time.Second // time.Duration(r.rttTable[r.curCoordinator()][r.Id])*time.Microsecond + 10*time.Second // todo remove +
// 		timer, ok := r.cmdTimers[cmdId]
// 		if ok {
// 			timer.Stop()
// 		}
// 		r.cmdTimers[cmdId] = *time.NewTimer(timeLeft)
// 		timer = r.cmdTimers[cmdId]
// 		go func(p int32) {
// 			dlog.Printf("Thread starting to wait for timer for commit for %v\n", cmdId)
// 			<-timer.C
// 			r.slowdownChan <- p
// 			dlog.Printf("Thread timer for %v fired\n", cmdId)
// 		}(r.phase)
// 		dlog.Printf("Setting timer for %v to %v from now\n", cmdId, timeLeft*time.Nanosecond)
// 		timer.Reset(timeLeft)
// 	}
// }

func (r *Replica) startAcceptTimers(cmdIds []state.CommandId, mcId int32) {
	// cmdMetadataLock.Lock()
	// defer cmdMetadataLock.Unlock()
	if SANITY_CHECK && r.isCoordinator(r.Id) {
		panic("Coordinator trying to start timers")
	}
	for _, cmdId := range cmdIds {

		if r.cmdMetadata[cmdId] != nil {

			// md := r.cmdMetadata[cmdId]
			// md.mockAccept = time.Now()
			// md.mockAcceptTime = md.mockAccept.Sub(md.startTime)
			if r.cmdMetadata[cmdId].status == ACCEPTED || r.cmdMetadata[cmdId].status == COMMITTED {
				dlog.Printf("cmd %v Already received real Accept not starting timer\n", cmdId)
				continue
			}
		}
		timeLeft := r.getExpectedAcceptArrivalTime(cmdId.ClientId, mcId) // + 1*time.Millisecond // todo remove +
		r.startTimerIfNotExist(cmdId, timeLeft, NULL)
		// timer := r.cmdTimers[cmdId]
		// r.cmdTimers[cmdId] = *time.NewTimer(timeLeft)
		// timer = r.cmdTimers[cmdId]
		// go func(p int32) {
		// 	dlog.Printf("Thread starting to wait for timer for %v\n", cmdId)
		// 	<-timer.C
		// 	r.slowdownChan <- p
		// 	dlog.Printf("Thread timer for %v fired\n", cmdId)
		// }(r.phase)
		// dlog.Printf("Setting timer for %v to %v from now\n", cmdId, timeLeft*time.Nanosecond)
		// r.stopTimers(cmdIds) // TODO remove
	}
}

// TODO OH WE COULD HAVE ALREADY RECEIVED THE ACCEPT FOR THE CORRESPONDING COMMAND...
func (r *Replica) handleMockAccept(accept *avicennaproto.Accept) {
	// old version
	// var areply *avicennaproto.AcceptReply
	// dlog.Println("instance", accept.Instances[0].Instance, "Received MockAccept from", accept.ReplicaId, "with phase", accept.Phase,
	// 	"my id", r.Id, "phase", r.phase)
	// if SANITY_CHECK {
	// 	if accept.Instances[0].Status != avicennaproto.MOCKACCEPT {
	// 		panic("Received an accept message from non-coordinator not marked MOCKACCEPT")
	// 	}
	// }
	// accept.Instances[0].Status = avicennaproto.MOCKACCEPT // why did I do this?
	// areply = &avicennaproto.AcceptReply{Phase: r.phase, Instance: accept.Instances[0].Instance, OK: MOCKREPLY}
	// // peculiar to wrap a single line...
	// r.replyAccept(accept.ReplicaId, areply)

	// // if I'm not the coordinator then start detection
	// if r.phase == accept.Phase && !r.isCoordinator(r.Id) {
	// 	// set timer for the request
	// 	r.startAcceptTimers(accept.Instances[0].CommandIds, accept.ReplicaId)
	// }

	// dlog.Println("MOCK: instance", accept.Instances[0].Instance, "Received MockAccept from", accept.ReplicaId, "with phase", accept.Phase,
	// "my id", r.Id, "phase", r.phase)
	if SANITY_CHECK && accept.Instances[0].Status != avicennaproto.MOCKACCEPT {
		panic("MOCK: Received an accept message from non-coordinator not marked MOCKACCEPT")
	}

	var areply *avicennaproto.AcceptReply
	if accept.Phase == r.phase {
		// dlog.Println("MOCK: Phase is correct", r.phase)

		if SANITY_CHECK {
			if accept.Instances[0].Status != avicennaproto.MOCKACCEPT {
				panic("MOCK: Received an accept message from coordinator not marked MOCKACCEPT")
			}
		}

		// dlog.Println("MOCK: handleAccept() Received MockAccept from", accept.ReplicaId, "with phase", accept.Phase,
		// "my id", r.Id, "phase", r.phase)

		// cch: we don't have timers anymore
		// if r.isStandbyForPhase(r.Id, accept.Phase) {
		// 	for _, acceptInstance := range accept.Instances {
		// 		dlog.Printf("instance %v Standby received accept, stopping timers for cmds %v\n", acceptInstance.Instance, acceptInstance.CommandIds)
		// 		r.stopTimers(acceptInstance.CommandIds, ACCEPTED)
		// 	}
		// }

		// I promised not to participate in Accept quorums for this phase
		if r.sentForPhase || r.isStandbyForPhase(r.Id, accept.Phase) {
			// dlog.Printf("MOCK: handleAccept() I already sent Rotate for this phase %v, or am a standby replica %v ignoring MockAccept\n",
			// r.sentForPhase, r.isStandby(r.Id))
			return
		}

		// for each instance in the accept message
		// (this is normally only done when the coordinator was rotated)
		for _, acceptInst := range accept.Instances {
			inst := r.instanceSpaceMock[acceptInst.Instance]
			// dlog.Printf("MOCK: instance %v received MockAccept from %v\n", acceptInst.Instance, accept.ReplicaId)
			if inst == nil {
				// dlog.Printf("MOCK: Received MockAccept for nil instance %v\n", acceptInst.Instance)
				for lastKnownNil := r.advanceMockCrtInstanceToNextNil(); lastKnownNil <= acceptInst.Instance; lastKnownNil++ {
					// todo test
					if lastKnownNil < acceptInst.Instance {
						// TODO this will cause performance problems. why am I doing this?
						// dlog.Printf("MOCK: received out-of-order accept putting it back in the channel until I receive the right one\n")
						r.acceptChan <- genericsmr.SerializableWithRecvTime{Obj: accept, RecvTime: time.Time{}}
						return
					}
					// dlog.Printf("MOCK: Creating instance %v in handleAccept from coordinator\n", lastKnownNil)
					r.instanceSpaceMock[lastKnownNil] = createInstance(
						make([]state.CommandAvi, 0), NULL, 0, acceptInst.DoGhost > 0, r.phase) // mock accepts were mocked yes?
					inst = r.instanceSpaceMock[lastKnownNil]
				}
				r.instanceSpaceMock[acceptInst.Instance].cmds = acceptInst.Commands
				r.instanceSpaceMock[acceptInst.Instance].doGhost = acceptInst.DoGhost > 0
			}
			// dlog.Printf("MOCK: handleAccept() working on instance %v in instanceSpaceMock: %v in message: %v\n",
			// acceptInst.Instance, inst, acceptInst.CommandIds)

			// if we already committed this accept was just reordered
			// or it is part of a long Accept
			if inst.status == MOCKCOMMITTED {
				// dlog.Printf("MOCK: instance %v Received Accept for already COMMITTED instance sending commit\n", acceptInst.Instance)
				// This commit message may be unnecessary, but should be safe.
				// r.bcastCommit(acceptInst.Instance, inst.cmds, false)
				continue
			}
			// no timers anymore
			// if !r.isCoordinator(r.Id) {
			// 	// we expect a commit within 1 RTT
			// 	r.stopTimers(acceptInst.CommandIds, ACCEPTED)
			// }
			if SANITY_CHECK && (inst.status == MOCKACCEPTED) && len(inst.cmds) != len(acceptInst.Commands) {
				log.Printf("Got a potential conflict instance, phase in accept: %d\n", accept.Phase)
				// log.Panicf("MOCK: Already accepted instance %v (in space) %v and got a second Accept?\n", acceptInst.Instance, inst)
			}
			// "accept" this and make sure it has the correct commands
			inst.status = MOCKACCEPTED
			// cmds may be overwritten here add them to the queue if so
			// dlog.Printf("MOCK: instance %v handleMockAccept() overwriting %v with %v\n", acceptInst.Instance, inst.cmds, acceptInst.CommandIds)
			r.addToQueueIfNotPresentMock(inst.cmds, acceptInst.Commands)
			inst.cmds = acceptInst.Commands
			inst.doGhost = acceptInst.DoGhost > 0
			for _, cmdId := range acceptInst.Commands {
				r.deleteLatestIfHighestMock(cmdId.Cmd.ClientId, cmdId.Cmd.OpId)
			}
			// dlog.Printf("MOCK: handleMockAccept() set commands for inst %v %v\n", acceptInst.Instance, inst)
			// This replica is being told this instance is committed.
			if acceptInst.Status == avicennaproto.COMMITTED {
				// cmds are in the instance from the line above, prevent the copy
				r.mockCommit(acceptInst.Instance, &inst.cmds, nil, acceptInst.DoGhost > 0)
				continue
			}
			// replies once for every instance
			areply = &avicennaproto.AcceptReply{Phase: accept.Phase,
				Instance: acceptInst.Instance, OK: MOCKREPLY}
			// dlog.Printf("MOCK: instance %v handleMockAccept sending reply: %v\n", acceptInst.Instance, areply)
			r.stats.nMockAcceptReplyTx++

			// log.Printf("Calling sync() in handleMockAccept()\n")
			r.sync()
			r.replyAccept(accept.ReplicaId, areply)
		}
	} else if accept.Phase < r.phase {
		// we can ignore this, the other replica will eventually get enough rotate messages to move on
		// dlog.Printf("MOCK: TODO Accept is behind got phase %v have phase %v\n", accept.Phase, r.phase)
	} else {
		// TODO rotation case (receiver is behind), we can't just ignore, if enough ignore it won't commit
		// dlog.Println("MOCK: TODO in bad (accept.Phase > r.phase) accept received case atm\n")
		r.acceptChan <- genericsmr.SerializableWithRecvTime{accept, time.Time{}}
		return
	}
}

func (r *Replica) handleRealAccept(accept *avicennaproto.Accept) {
	var areply *avicennaproto.AcceptReply
	if accept.Phase >= r.phase {
		// dlog.Println("REAL: Phase is correct", r.phase)

		if SANITY_CHECK {
			if accept.Instances[0].Status != avicennaproto.ACCEPT {
				panic("REAL: Received an accept message from coordinator not marked ACCEPT")
			}
		}

		// dlog.Println("REAL: handleAccept() Received Accept from", accept.ReplicaId, "with phase", accept.Phase,
		// "my id", r.Id, "phase", r.phase)

		// cch: we don't have timers anymore
		// if r.isStandby(r.Id) {
		// 	for _, acceptInstance := range accept.Instances {
		// 		dlog.Printf("instance %v Standby received accept, stopping timers for cmds %v\n", acceptInstance.Instance, acceptInstance.CommandIds)
		// 		r.stopTimers(acceptInstance.CommandIds, ACCEPTED)
		// 	}
		// }

		// I promised not to participate in Accept quorums for this phase
		// if r.sentForPhase || r.isStandby(r.Id) {
		if accept.Phase < r.phase {
			return
		} else if r.sentForPhase || r.isStandbyForPhase(r.Id, accept.Phase) {
			dlog.Printf("REAL: handleAccept() I already sent Rotate for this phase %v, or am a standby replica %v ignoring Accept\n",
				r.sentForPhase, r.isStandby(r.Id))
			return
		}

		// for each instance in the accept message
		// (this is normally only done when the coordinator was rotated)
		log.Printf("Start processing LongAccept.\n")
		for _, acceptInst := range accept.Instances {
			inst := r.instanceSpace[acceptInst.Instance]
			// dlog.Printf("REAL: instance %v received Accept from %v\n", acceptInst.Instance, accept.ReplicaId)
			if inst == nil {
				// dlog.Printf("REAL: Received Accept for nil instance %v\n", acceptInst.Instance)
				for lastKnownNil := r.advanceCrtInstanceToNextNil(); lastKnownNil <= acceptInst.Instance; lastKnownNil++ {
					// todo test
					if lastKnownNil < acceptInst.Instance {
						// TODO this will cause performance problems. why am I doing this?
						// dlog.Printf("REAL: received out-of-order accept putting it back in the channel until I receive the right one\n")
						r.acceptChan <- genericsmr.SerializableWithRecvTime{Obj: accept, RecvTime: time.Time{}}
						return
					}
					// dlog.Printf("REAL: Creating instance %v in handleAccept from coordinator\n", lastKnownNil)
					r.instanceSpace[lastKnownNil] = createInstance(
						make([]state.CommandAvi, 0), NULL, 0, acceptInst.DoGhost > 0, r.phase)
					inst = r.instanceSpace[lastKnownNil]
				}
				r.instanceSpace[acceptInst.Instance].cmds = acceptInst.Commands
			}
			// dlog.Printf("REAL: handleAccept() working on instance %v in instanceSpace: %v in message: %v\n",
			// acceptInst.Instance, inst, acceptInst.CommandIds)

			// if we already committed this accept was just reordered
			// or it is part of a long Accept
			if inst.status == COMMITTED {
				// dlog.Printf("REAL: instance %v Received Accept for already COMMITTED instance sending commit\n", acceptInst.Instance)
				// This commit message may be unnecessary, but should be safe.
				// r.bcastCommit(acceptInst.Instance, inst.cmds, false)
				continue
			}
			// no timers anymore
			// if !r.isCoordinator(r.Id) {
			// 	// we expect a commit within 1 RTT
			// 	r.stopTimers(acceptInst.CommandIds, ACCEPTED)
			// }
			if SANITY_CHECK && (inst.status == ACCEPTED) && len(inst.cmds) != len(acceptInst.Commands) {
				log.Printf("Got a potential conflict instance, phase in accept: %d\n", accept.Phase)
				// log.Panicf("REAL: Already accepted instance %v (in space) %v and got a second Accept?\n", acceptInst.Instance, inst)
			}
			// "accept" this and make sure it has the correct commands
			inst.status = ACCEPTED
			// cmds may be overwritten here add them to the queue if so
			// dlog.Printf("REAL: instance %v handleAccept() overwriting %v with %v\n", acceptInst.Instance, inst.cmds, acceptInst.CommandIds)
			r.addToQueueIfNotPresent(inst.cmds, acceptInst.Commands)
			inst.cmds = acceptInst.Commands
			inst.doGhost = acceptInst.DoGhost > 0
			// for _, cmd := range acceptInst.Commands {
			// 	r.deleteLatestIfHighest(cmd.ClientId, cmd.OpId)
			// }
			// dlog.Printf("REAL: handleAccept() set commands for inst %v %v\n", acceptInst.Instance, inst)
			// This replica is being told this instance is committed.
			if acceptInst.Status == avicennaproto.COMMITTED {
				// cmds are in the instance from the line above, prevent the copy
				r.commit(acceptInst.Instance, &inst.cmds, nil, acceptInst.DoGhost > 0)
				continue
			}
			// replies once for every instance
			areply = &avicennaproto.AcceptReply{Phase: accept.Phase,
				Instance: acceptInst.Instance, OK: TRUE}
			// dlog.Printf("REAL: instance %v handleAccept sending reply: %v\n", acceptInst.Instance, areply)
			r.stats.nAcceptReplyTx++

			// log.Printf("Calling sync() in handleRealAccept()\n")
			r.sync()
			log.Printf("Start bcasting LongAcceptReply.\n")
			r.replyAccept(accept.ReplicaId, areply)
			log.Printf("Finish bcasting LongAcceptReply.\n")
		}
		log.Printf("Finish processing LongAccept.\n")
	} else {
		// TODO rotation case (receiver is behind), we can't just ignore, if enough ignore it won't commit
		// dlog.Println("REAL: TODO in bad (accept.Phase > r.phase) accept received case atm\n")
		// r.acceptChan <- genericsmr.SerializableWithRecvTime{accept, time.Time{}}

		// return
	}
}

func (r *Replica) handleAccept(accept *avicennaproto.Accept) {
	if SANITY_CHECK && len(accept.Instances) <= 0 {
		panic("Received empty accept")
	}
	// if the sender is not the coordinator this is a MockAccept
	if !r.isCoordinatorForPhase(accept.ReplicaId, accept.Phase) {
		// log.Printf("Receive message from accept channel, probably longAccept, from ghost leader?")
		if SANITY_CHECK && !r.isMockCoordinatorForPhase(accept.ReplicaId, accept.Phase) {
			log.Printf("Got Accept from neither coordinator nor mock coordinator %v\n", accept)
		}
		r.stats.nMockAcceptRx++
		r.handleMockAccept(accept)
	} else {
		// log.Panicf()
		// log.Printf("Receive message from accept channel, probably longAccept, from real leader")
		r.stats.nAcceptRx++
		r.handleRealAccept(accept)
	}
	return
}

func (r *Replica) handleMockAcceptExecTime(acceptExecTime *avicennaproto.AcceptExecTime) {
	if SANITY_CHECK && acceptExecTime.Instances[0].Status != avicennaproto.MOCKACCEPT {
		panic("MOCK: Received an accept message from non-coordinator not marked MOCKACCEPT")
	}

	var areply *avicennaproto.AcceptReply
	if acceptExecTime.Phase == r.phase {
		// dlog.Println("MOCK: Phase is correct", r.phase)

		if SANITY_CHECK {
			if acceptExecTime.Instances[0].Status != avicennaproto.MOCKACCEPT {
				panic("MOCK: Received an accept message from coordinator not marked MOCKACCEPT")
			}
		}

		// dlog.Println("MOCK: handleAccept() Received MockAccept from", accept.ReplicaId, "with phase", accept.Phase,
		// "my id", r.Id, "phase", r.phase)

		// cch: we don't have timers anymore
		// if r.isStandbyForPhase(r.Id, accept.Phase) {
		// 	for _, acceptInstance := range accept.Instances {
		// 		dlog.Printf("instance %v Standby received accept, stopping timers for cmds %v\n", acceptInstance.Instance, acceptInstance.CommandIds)
		// 		r.stopTimers(acceptInstance.CommandIds, ACCEPTED)
		// 	}
		// }
		r.ghostExecLatChan <- acceptExecTime.MockExecTimes
		// for _, execTime := range acceptExecTime.MockExecTimes {
		// 	if execTime.DoMock && execTime.ExecTime > 0 && r.warmupDone {
		// 		// log.Printf("Got ghost execution time, client: %d, mockExecTime: %d, mockExecCmdId: %d\n", execTime.CommandId.ClientId, execTime.ExecTime, execTime.CommandId.OpId)
		// 		r.ghostExecLatChan <- avicennaproto.GhostExecLatency{CmdId: execTime.CommandId, ExecTime: execTime.ExecTime}
		// 	}
		// }
		// for _, execTime := range acceptExecTime.MockExecTimes {
		// 	if execTime.DoMock && execTime.ExecTime > 0 {
		// 		log.Printf("Got ghost execution time, client: %d, mockExecTime: %d, mockExecCmdId: %d\n", execTime.CommandId.ClientId, execTime.ExecTime, execTime.CommandId.OpId)
		// 		if r.ObjectiveFuncGhostExec(r.objectiveFuncMD, avicennaproto.GhostExecLatency{CmdId: execTime.CommandId, ExecTime: execTime.ExecTime}) && r.warmupDone {
		// 			r.nbWriteToSlowdownChan(r.phase)
		// 		}
		// 	}
		// }

		// I promised not to participate in Accept quorums for this phase
		if r.sentForPhase || r.isStandbyForPhase(r.Id, acceptExecTime.Phase) {
			// dlog.Printf("MOCK: handleAccept() I already sent Rotate for this phase %v, or am a standby replica %v ignoring MockAccept\n",
			// r.sentForPhase, r.isStandby(r.Id))
			return
		}

		// for each instance in the accept message
		// (this is normally only done when the coordinator was rotated)
		for _, acceptInst := range acceptExecTime.Instances {
			inst := r.instanceSpaceMock[acceptInst.Instance]
			// dlog.Printf("MOCK: instance %v received MockAccept from %v\n", acceptInst.Instance, accept.ReplicaId)
			if inst == nil {
				// dlog.Printf("MOCK: Received MockAccept for nil instance %v\n", acceptInst.Instance)
				r.instanceSpaceMock[acceptInst.Instance] = createInstance(make([]state.CommandAvi, 0), NULL, 0, acceptInst.DoGhost > 0, r.phase)
				inst = r.instanceSpaceMock[acceptInst.Instance]

				// for lastKnownNil := r.advanceMockCrtInstanceToNextNil(); lastKnownNil <= acceptInst.Instance; lastKnownNil++ {
				// 	// todo test
				// 	if lastKnownNil < acceptInst.Instance {
				// 		// TODO this will cause performance problems. why am I doing this?
				// 		// dlog.Printf("MOCK: received out-of-order accept putting it back in the channel until I receive the right one\n")
				// 		r.acceptExecTimeChan <- genericsmr.SerializableWithRecvTime{Obj: acceptExecTime, RecvTime: time.Time{}}
				// 		return
				// 	}
				// 	// dlog.Printf("MOCK: Creating instance %v in handleAccept from coordinator\n", lastKnownNil)
				// 	r.instanceSpaceMock[lastKnownNil] = createInstance(
				// 		make([]state.Command, 0), NULL, 0, acceptInst.DoMock > 0) // mock accepts were mocked yes?
				// 	inst = r.instanceSpaceMock[lastKnownNil]
				// }
				// r.instanceSpaceMock[acceptInst.Instance].cmds = acceptInst.Commands
				// r.instanceSpaceMock[acceptInst.Instance].doMock = acceptInst.DoMock > 0
			}
			// dlog.Printf("MOCK: handleAccept() working on instance %v in instanceSpaceMock: %v in message: %v\n",
			// acceptInst.Instance, inst, acceptInst.CommandIds)

			// if we already committed this accept was just reordered
			// or it is part of a long Accept
			if inst.status == MOCKCOMMITTED {
				// dlog.Printf("MOCK: instance %v Received Accept for already COMMITTED instance sending commit\n", acceptInst.Instance)
				// This commit message may be unnecessary, but should be safe.
				// r.bcastCommit(acceptInst.Instance, inst.cmds, false)
				continue
			}
			// no timers anymore
			// if !r.isCoordinator(r.Id) {
			// 	// we expect a commit within 1 RTT
			// 	r.stopTimers(acceptInst.CommandIds, ACCEPTED)
			// }
			if SANITY_CHECK && (inst.status == MOCKACCEPTED) && len(inst.cmds) != len(acceptInst.Commands) {
				// log.Panicf("MOCK: Already accepted instance %v (in space) %v and got a second Accept?\n", acceptInst.Instance, inst)
				log.Printf("Phase in GHOST accept: %d\n", acceptExecTime.Phase)
			}
			// "accept" this and make sure it has the correct commands
			inst.status = MOCKACCEPTED
			// cmds may be overwritten here add them to the queue if so
			// dlog.Printf("MOCK: instance %v handleMockAccept() overwriting %v with %v\n", acceptInst.Instance, inst.cmds, acceptInst.CommandIds)
			r.addToQueueIfNotPresentMock(inst.cmds, acceptInst.Commands)
			inst.cmds = acceptInst.Commands
			inst.doGhost = acceptInst.DoGhost > 0
			for _, cmdId := range acceptInst.Commands {
				r.deleteLatestIfHighestMock(cmdId.Cmd.ClientId, cmdId.Cmd.OpId)
			}
			// dlog.Printf("MOCK: handleMockAccept() set commands for inst %v %v\n", acceptInst.Instance, inst)
			// This replica is being told this instance is committed.
			if acceptInst.Status == avicennaproto.COMMITTED {
				// cmds are in the instance from the line above, prevent the copy
				r.mockCommit(acceptInst.Instance, &inst.cmds, nil, acceptInst.DoGhost > 0)
				continue
			}
			// replies once for every instance
			areply = &avicennaproto.AcceptReply{Phase: acceptExecTime.Phase,
				Instance: acceptInst.Instance, OK: MOCKREPLY}
			// dlog.Printf("MOCK: instance %v handleMockAccept sending reply: %v\n", acceptInst.Instance, areply)
			r.stats.nMockAcceptReplyTx++

			// log.Printf("Calling sync() in handleMockAccept()\n")
			// r.sync()
			r.replyAccept(acceptExecTime.ReplicaId, areply)
		}

	} else if acceptExecTime.Phase < r.phase {
		// we can ignore this, the other replica will eventually get enough rotate messages to move on
		// dlog.Printf("MOCK: TODO Accept is behind got phase %v have phase %v\n", accept.Phase, r.phase)
	} else {
		// TODO rotation case (receiver is behind), we can't just ignore, if enough ignore it won't commit
		// dlog.Println("MOCK: TODO in bad (accept.Phase > r.phase) accept received case atm\n")
		r.acceptExecTimeChan <- genericsmr.SerializableWithRecvTime{acceptExecTime, time.Time{}}
		return
	}
}

func (r *Replica) handleRealAcceptExecTime(acceptExecTime *avicennaproto.AcceptExecTime) {
	var areply *avicennaproto.AcceptReply
	if acceptExecTime.Phase == r.phase {
		// dlog.Println("REAL: Phase is correct", r.phase)

		if SANITY_CHECK {
			if acceptExecTime.Instances[0].Status != avicennaproto.ACCEPT {
				panic("REAL: Received an accept message from coordinator not marked ACCEPT")
			}
		}

		// dlog.Println("REAL: handleAccept() Received Accept from", accept.ReplicaId, "with phase", accept.Phase,
		// "my id", r.Id, "phase", r.phase)

		// cch: we don't have timers anymore
		// if r.isStandby(r.Id) {
		// 	for _, acceptInstance := range accept.Instances {
		// 		dlog.Printf("instance %v Standby received accept, stopping timers for cmds %v\n", acceptInstance.Instance, acceptInstance.CommandIds)
		// 		r.stopTimers(acceptInstance.CommandIds, ACCEPTED)
		// 	}
		// }

		// I promised not to participate in Accept quorums for this phase
		// if r.sentForPhase || r.isStandby(r.Id) {
		if r.sentForPhase || r.isStandbyForPhase(r.Id, acceptExecTime.Phase) {
			dlog.Printf("REAL: handleAccept() I already sent Rotate for this phase %v, or am a standby replica %v ignoring Accept\n",
				r.sentForPhase, r.isStandby(r.Id))
			return
		}

		// for each instance in the accept message
		// (this is normally only done when the coordinator was rotated)
		for _, acceptInst := range acceptExecTime.Instances {
			inst := r.instanceSpace[acceptInst.Instance]
			// dlog.Printf("REAL: instance %v received Accept from %v\n", acceptInst.Instance, accept.ReplicaId)
			if inst == nil {
				// dlog.Printf("REAL: Received Accept for nil instance %v\n", acceptInst.Instance)
				for lastKnownNil := r.advanceCrtInstanceToNextNil(); lastKnownNil <= acceptInst.Instance; lastKnownNil++ {
					// todo test
					// if lastKnownNil < acceptInst.Instance {
					// 	// TODO this will cause performance problems. why am I doing this?
					// 	// dlog.Printf("REAL: received out-of-order accept putting it back in the channel until I receive the right one\n")
					// 	r.acceptExecTimeChan <- genericsmr.SerializableWithRecvTime{Obj: acceptExecTime, RecvTime: time.Time{}}
					// 	return
					// }
					// dlog.Printf("REAL: Creating instance %v in handleAccept from coordinator\n", lastKnownNil)
					r.instanceSpace[lastKnownNil] = createInstance(
						make([]state.CommandAvi, 0), NULL, 0, acceptInst.DoGhost > 0, r.phase)
					inst = r.instanceSpace[lastKnownNil]
				}
				r.instanceSpace[acceptInst.Instance].cmds = acceptInst.Commands
			}
			// dlog.Printf("REAL: handleAccept() working on instance %v in instanceSpace: %v in message: %v\n",
			// acceptInst.Instance, inst, acceptInst.CommandIds)

			// if we already committed this accept was just reordered
			// or it is part of a long Accept
			if inst.status == COMMITTED {
				// dlog.Printf("REAL: instance %v Received Accept for already COMMITTED instance sending commit\n", acceptInst.Instance)
				// This commit message may be unnecessary, but should be safe.
				// r.bcastCommit(acceptInst.Instance, inst.cmds, false)
				continue
			}
			// no timers anymore
			// if !r.isCoordinator(r.Id) {
			// 	// we expect a commit within 1 RTT
			// 	r.stopTimers(acceptInst.CommandIds, ACCEPTED)
			// }
			if SANITY_CHECK && (inst.status == ACCEPTED) && len(inst.cmds) != len(acceptInst.Commands) {
				// log.Panicf("REAL: Already accepted instance %v (in space) %v and got a second Accept?\n", acceptInst.Instance, inst)
				log.Printf("Phase in REAL accept: %d\n", acceptExecTime.Phase)
			}
			// "accept" this and make sure it has the correct commands
			inst.status = ACCEPTED
			// cmds may be overwritten here add them to the queue if so
			// dlog.Printf("REAL: instance %v handleAccept() overwriting %v with %v\n", acceptInst.Instance, inst.cmds, acceptInst.CommandIds)
			r.addToQueueIfNotPresent(inst.cmds, acceptInst.Commands)
			inst.cmds = acceptInst.Commands
			inst.doGhost = acceptInst.DoGhost > 0
			// for _, cmd := range acceptInst.Commands {
			// 	r.deleteLatestIfHighest(cmd.ClientId, cmd.OpId)
			// }
			// dlog.Printf("REAL: handleAccept() set commands for inst %v %v\n", acceptInst.Instance, inst)
			// This replica is being told this instance is committed.
			if acceptInst.Status == avicennaproto.COMMITTED {
				// cmds are in the instance from the line above, prevent the copy
				r.commit(acceptInst.Instance, &inst.cmds, nil, acceptInst.DoGhost > 0)
				continue
			}
			// replies once for every instance
			areply = &avicennaproto.AcceptReply{Phase: acceptExecTime.Phase,
				Instance: acceptInst.Instance, OK: TRUE}
			// dlog.Printf("REAL: instance %v handleAccept sending reply: %v\n", acceptInst.Instance, areply)
			r.stats.nAcceptReplyTx++

			// log.Printf("Calling sync() in handleRealAccept()\n")
			r.sync()
			r.replyAccept(acceptExecTime.ReplicaId, areply)

		}
	} else if acceptExecTime.Phase < r.phase {
		// we can ignore this, the other replica will eventually get enough rotate messages to move on
		// dlog.Printf("REAL: TODO Accept is behind got phase %v have phase %v\n", accept.Phase, r.phase)
	} else {
		// TODO rotation case (receiver is behind), we can't just ignore, if enough ignore it won't commit
		// dlog.Println("REAL: TODO in bad (accept.Phase > r.phase) accept received case atm\n")
		r.acceptExecTimeChan <- genericsmr.SerializableWithRecvTime{acceptExecTime, time.Time{}}
		return
	}
}

func (r *Replica) handleAcceptExecTime(acceptExecTime *avicennaproto.AcceptExecTime) {
	// beforeHanlde := time.Now()
	if SANITY_CHECK && len(acceptExecTime.Instances) <= 0 {
		panic("Received empty accept")
	}

	if !r.isCoordinatorForPhase(acceptExecTime.ReplicaId, acceptExecTime.Phase) {
		if SANITY_CHECK && !r.isMockCoordinatorForPhase(acceptExecTime.ReplicaId, acceptExecTime.Phase) {
			log.Printf("Wierd, got accept from neither coordinator nor mock coordinator %v\n", acceptExecTime)
		}
		r.stats.nMockAcceptReplyRx++
		r.handleMockAcceptExecTime(acceptExecTime)
	} else {
		r.stats.nAcceptReplyRx++
		r.handleRealAcceptExecTime(acceptExecTime)
	}
	// handleLat := time.Since(beforeHanlde)
	// r.MsgParseLatChan <- &handleLat
}

func (r *Replica) updateGobalMaxCommitReal(instNo int32) {
	if r.globalMaxCommittedReal == 0 {
		return
	}
	if r.globalMaxCommittedReal < instNo-1 {
		return
	}

	if r.globalMaxCommittedReal >= instNo {
		log.Printf("Wierd about updating global maximum committed real instance. Working on %d, but the gloablMaxRealCommit is %d\n", instNo, r.globalMaxCommittedReal)
		return
	}

	i := instNo
	for {
		inst := r.instanceSpace[i+1]
		if inst != nil && inst.accepts == (r.N>>1)+1 {
			i++
		} else {
			break
		}
	}

	r.globalMaxCommittedReal = i
}

func (r *Replica) handleAcceptReplyReal(areply *avicennaproto.AcceptReply) {
	inst := r.instanceSpace[areply.Instance]
	if SANITY_CHECK && inst == nil {
		panic("nil instance in handleAcceptReply")
	}

	inst.accepts++

	if inst.status == COMMITTED {
		if inst.accepts == (r.N>>1)+1 {
			r.updateGobalMaxCommitReal(areply.Instance)
		}
		return
	}

	dlog.Printf("instance %v handleAcceptReplyReal() good reply for instance %v in space %v reply: %v\n",
		areply.Instance, areply.Instance, inst, areply)

	if inst.accepts+1 > r.N>>1 {
		if r.phase == 1 {
			// log.Printf("Commit inst %d in new phase, cmds: %v\n", areply.Instance, inst.cmds)
		}
		// log.Printf("Collected enought AcceptReplys, commit inst %d, cmds: %v\n", areply.Instance, inst.cmds)
		r.commit(areply.Instance, &inst.cmds, nil, inst.doGhost)
		r.stats.nCommitsTx++
		r.bcastCommit(areply.Instance, inst.cmds)
		if inst.doGhost {
			r.stats.nCommitsToClientsTx++
			r.broadcastCommitToClientChan <- InstWithNum{Instance: *inst, instNo: areply.Instance}
			// r.bcastRealCommitToClients(areply.Instance)
		}
		// TODO only do this if a request in this batch was also mocked
	}
}

func (r *Replica) updateGobalMaxCommitGhost(instNo int32) {
	if r.globalMaxCommittedGhost < instNo-1 {
		return
	}

	if r.globalMaxCommittedGhost >= instNo {
		log.Printf("Wierd about updating global maximum committed real instance. Working on %d, but the gloablMaxRealCommit is %d\n", instNo, r.globalMaxCommittedReal)
		return
	}

	i := instNo
	for {
		inst := r.instanceSpaceMock[i+1]
		if inst != nil && inst.accepts == (r.N>>1)+1 {
			i++
		} else {
			break
		}
	}

	r.globalMaxCommittedGhost = i
}

func (r *Replica) handleAcceptReplyMock(areply *avicennaproto.AcceptReply) {
	inst := r.instanceSpaceMock[areply.Instance]
	if SANITY_CHECK && inst == nil {
		panic("nil instance in handleAcceptReply for a MockAccept")
	}
	inst.accepts++
	// probably can use the normal statuses
	if inst.status == MOCKCOMMITTED {
		if inst.accepts == (r.N>>1)+1 {
			r.updateGobalMaxCommitGhost(areply.Instance)
		}
		return
	}

	dlog.Printf("instance %v handleAcceptReplyMock() good reply for instance %v in space %v reply: %v\n",
		areply.Instance, areply.Instance, inst, areply)

	if inst.accepts+1 > r.N>>1 {
		// inst.status = MOCKCOMMITTED
		r.mockCommit(areply.Instance, &inst.cmds, nil, inst.doGhost)
		r.stats.nMockCommitsTx++
		r.bcastMockCommit(areply.Instance, inst.cmds)
		r.stats.nMockCommitsToClientsTx++
		// r.bcastMockCommitToClients(areply.Instance)
		r.broadcastCommitToClientChan <- InstWithNum{Instance: *inst, instNo: areply.Instance}
	}
}

func (r *Replica) handleAcceptReply(areply *avicennaproto.AcceptReply) {
	// we rotated since I sent this
	if areply.Phase != r.phase {
		// dlog.Printf("Received an AcceptReply that is behind got %v have %v\n", areply.Phase, r.phase)
		return
	}

	if !r.sentForPhase {
		if areply.OK == TRUE {
			r.stats.nAcceptReplyRx++
			r.handleAcceptReplyReal(areply)
			return

		} else if areply.OK == MOCKREPLY {

			r.stats.nMockAcceptReplyRx++
			r.handleAcceptReplyMock(areply)
			return

		}
	}
}

var pcs avicennaproto.CommitShort

func (r *Replica) sendMockCommitToClient(instNo int32, clientId uint32, opId int32) error {
	cmdIds := make([]state.CommandId, 0)
	for _, avicmdId := range r.idsFromCmds(r.instanceSpaceMock[instNo].cmds) {
		cmdIds = append(cmdIds, state.CommandId{ClientId: avicmdId.ClientId, OpId: avicmdId.OpId})
	}
	if writer, ok := r.clientWriters[clientId]; ok {
		mockCommit := &genericsmrproto.MockCommitted{Instance: instNo, OpId: opId, Commands: nil} //r.instanceSpaceMock[instNo].cmds}
		dlog.Printf("Sending MockCommitted to a client for instance %v cmdIds %v\n", instNo, cmdIds)
		r.ClientWriteLock[clientId].Lock()
		defer r.ClientWriteLock[clientId].Unlock()
		writer.WriteByte(genericsmrproto.MOCK_COMMITTED)
		mockCommit.Marshal(writer)
		err := writer.Flush()
		if err != nil {
			// log.Printf("Error writing MockCommitted instance %v cmdIds %v\n", instNo, cmdIds)

		}
		return err
	} else {
		// log.Printf("Error finding writer for MockCommitted instance %v cmdIds %v\n", instNo, cmdIds)
	}
	return nil
}

func (r *Replica) sendRealCommitToClient(instNo int32, clientId uint32, opId int32) error {
	cmdIds := make([]state.CommandId, 0)
	for _, avicmdId := range r.idsFromCmds(r.instanceSpace[instNo].cmds) {
		cmdIds = append(cmdIds, state.CommandId{ClientId: avicmdId.ClientId, OpId: avicmdId.OpId})
	}
	r.ClientWriteLock[clientId].Lock()
	defer r.ClientWriteLock[clientId].Unlock()
	writer, ok := r.clientWriters[clientId]
	if !ok {
		return nil
	}

	// dlog.Printf("Sending RealCommitted to a client for instance %v cmdIds %v\n", instNo, cmds)

	writer.WriteByte(genericsmrproto.REAL_COMMITTED)
	realCommit := genericsmrproto.RealCommitted{Instance: instNo, OpId: opId, Commands: r.instanceSpace[instNo].cmds}
	realCommit.Marshal(writer)
	// if err != nil {
	// dlog.Printf("Error writing RealCommitted instance %v cmdIds %v\n", instNo, cmdIds)
	// }

	return writer.Flush()
}

func (r *Replica) bcastMockCommitToClients(instNo int32) {
	// if !MOCK {
	// 	log.Panicf("Don't bcast commit messages while not mocking\n")
	// }
	// for _, cmd := range r.instanceSpaceMock[instNo].cmds {
	// 	if COMMIT_REPLY_ONLY_IF_MOCK_REQUESTED_FROM_CLIENT {
	// 		// r.cmdMapLock.RLock()
	// 		if cmdDoMock, exists := r.cmdMap[state.CommandId{cmd.ClientId, cmd.OpId}]; exists && cmdDoMock.DoMock {
	// 			go r.sendMockCommitToClient(instNo, cmd.ClientId, cmd.OpId)
	// 		}
	// 		// r.cmdMapLock.RUnlock()
	// 	} else {
	// 		go r.sendMockCommitToClient(instNo, cmd.ClientId, cmd.OpId)
	// 	}
	// }
}

// should we send this to all clients or to one? oh, this is why t/p is bad...
func (r *Replica) bcastRealCommitToClients(instNo int32) {
	// if !MOCK {
	// 	log.Panicf("Don't bcast commit messages while not mocking\n")
	// }
	// // log.Printf("Sending realCommitted message to clients for instance %v\n", instNo)
	// for _, cmd := range r.instanceSpace[instNo].cmds {
	// 	if COMMIT_REPLY_ONLY_IF_MOCK_REQUESTED_FROM_CLIENT {
	// 		// r.cmdMapLock.RLock()
	// 		if cmdDoMock, exists := r.cmdMap[state.CommandId{cmd.ClientId, cmd.OpId}]; exists && cmdDoMock.DoMock {
	// 			go r.sendRealCommitToClient(instNo, cmd.ClientId, cmd.OpId)
	// 		}
	// 		// r.cmdMapLock.RUnlock()
	// 	} else {
	// 		go r.sendRealCommitToClient(instNo, cmd.ClientId, cmd.OpId)
	// 	}
	// }
	// log.Printf("Finishing sent realCommitted message to clients for instance %v\n", instNo)
}

// Commits can be sent by non leaders, so we need to separate the message types
func (r *Replica) bcastMockCommit(instance int32, commands []state.CommandAvi) {
	var pc avicennaproto.Commit
	pc.Instances = make([]avicennaproto.InstanceCommands, 1)
	pc.Phase = r.phase
	pc.ReplicaId = r.Id
	pc.Mock = TRUE
	// if mock {
	// 	pc.Mock = TRUE
	// }

	cmdIds := make([]state.CommandId, len(commands))
	for i, command := range commands {
		cmdIds[i].ClientId = command.Cmd.ClientId
		cmdIds[i].OpId = command.Cmd.OpId
	}

	// TODO one instance commits for now
	pc.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.MOCKCOMMIT, Commands: commands}

	pc.GlobalMaxCommit = r.globalMaxCommittedGhost
	// dlog.Printf("bcastCommit() bcasting MockCommit message: %v\n", pc)
	r.bcastMsg(r.commitRPC, &pc, false)
}

// Commits can be sent by non leaders, so we need to separate the message types
func (r *Replica) bcastCommit(instance int32, commands []state.CommandAvi) {
	var pc avicennaproto.Commit
	pc.Instances = make([]avicennaproto.InstanceCommands, 1)
	pc.Phase = r.phase
	pc.ReplicaId = r.Id
	pc.Mock = FALSE
	// if mock {
	// 	pc.Mock = TRUE
	// }

	cmdIds := make([]state.CommandId, len(commands))
	for i, command := range commands {
		cmdIds[i].ClientId = command.Cmd.ClientId
		cmdIds[i].OpId = command.Cmd.OpId
	}

	// TODO one instance commits for now
	pc.Instances[0] = avicennaproto.InstanceCommands{Instance: instance, Status: avicennaproto.COMMITTED, Commands: commands}

	pc.GlobalMaxCommit = r.globalMaxCommittedReal
	// dlog.Printf("bcastCommit() bcasting Commit message: %v\n", pc)
	r.bcastMsg(r.commitRPC, &pc, false)
	if !r.isCoordinator(r.Id) && !r.isCurMockCoordinator() {
		log.Printf("Broadcasting commit message for inst %d, but I'm not real or mock leader.\n", instance)
	}
}

func (r *Replica) replyAccept(CoordinatorId int32, areply *avicennaproto.AcceptReply) {
	r.stats.nMsgsSent++
	// r.replicaMu[CoordinatorId].Lock()
	r.SendMsg(CoordinatorId, r.acceptReplyRPC, areply)
	// r.replicaMu[CoordinatorId].Unlock()
}

// if cmds is nil it finds the cmds for cmdIds
func (r *Replica) mockCommit(instNo int32, cmds *[]state.CommandAvi, cmdIds *[]state.CommandId, doMock bool) {
	// dlog.Printf("mockCommit() for instNo %v\n", instNo)

	// if dlog.DLOG {
	// 	if pc, _, no, ok := runtime.Caller(1); ok {
	// 		details := runtime.FuncForPC(pc)
	// 		dlog.Printf("mockCommit instance %v %v %v (from %s:%v) \n", instNo, cmds, cmdIds, details.Name(), no)
	// 	} else {
	// 		dlog.Printf("mockCommit instance %v %v %v\n", instNo, cmds, cmdIds)
	// 	}
	// }

	inst := r.instanceSpaceMock[instNo]

	if inst == nil {
		// dlog.Printf("Creating instance %v in mockCommit\n", instNo)
		r.instanceSpaceMock[instNo] = createInstance(nil, MOCKACCEPTED, 0, doMock, r.phase)
		inst = r.instanceSpaceMock[instNo]
	}

	// it's possible that cmds is nil when we aren't committing via AcceptReplys
	if cmds != nil {
		if len(*cmds) == 0 {
			// panic("Commands empty in commit")
		}

		if SANITY_CHECK {
			if inst.status == MOCKCOMMITTED {
				pc, _, no, ok := runtime.Caller(1)
				details := runtime.FuncForPC(pc)
				if ok && details != nil {
					dlog.Printf("commit() called from %s line %v refusing to commit twice\n", details.Name(), no)
				}
				panic("commit() refusing to commit twice")
			}
		}

		// this copy is often redundant
		inst.cmds = *cmds
		inst.doGhost = doMock
		// dlog.Printf("mock commit != nil set commands for inst %v %v\n", instNo, inst)
		for _, cmd := range *cmds {
			r.deleteLatestIfHighestMock(cmd.Cmd.ClientId, cmd.Cmd.OpId)
		}
		// TODO multi-paxos has a check here to reply to the client if it can
		// r.recordInstanceMetadata(inst)
		// r.sync()
	} else { // handleCommit goes here because the commands aren't sent in the commit message, just the IDs
		log.Panicf("No cmdIds\n")
		// panic("mockCommit() shouldn't be used like this")
		if SANITY_CHECK {
			if cmdIds == nil {
				panic("nil cmdIds when expected")
			}
			if inst.status == MOCKCOMMITTED {
				pc, _, no, ok := runtime.Caller(1)
				details := runtime.FuncForPC(pc)
				if ok && details != nil {
					dlog.Printf("commit() called from %s line %v refusing to commit twice\n", details.Name(), no)
				}
				panic("commit() refusing to commit twice")
			}
		}
		// inst.cmds =  r.cmdsFromIds(*cmdIds)
		inst.doGhost = doMock
		// todo was
		for _, cmdId := range *cmdIds {
			r.deleteLatestIfHighestMock(cmdId.ClientId, cmdId.OpId)
			// delete(r.latestCmdSeen, cmdId.ClientId)
		}
		// dlog.Printf("instance %v commit == nil set commands for inst %v\n", instNo, inst)
	}
	inst.status = MOCKCOMMITTED
	r.updateMockCommittedUpTo()
}

// if cmds is nil it finds the cmds for cmdIds
// do we need to know do mock here?
func (r *Replica) commit(instNo int32, cmds *[]state.CommandAvi, cmdIds *[]state.CommandId, doMock bool) {
	// dlog.Printf("commit() for instNo %v\n", instNo)
	// if dlog.DLOG {
	// 	if pc, _, no, ok := runtime.Caller(1); ok {
	// 		details := runtime.FuncForPC(pc)
	// 		dlog.Printf("instance %v commit %v %v (from %s:%v) \n", instNo, cmds, cmdIds, details.Name(), no)
	// 	} else {
	// 		dlog.Printf("instance %v commit %v %v\n", instNo, cmds, cmdIds)
	// 	}
	// }
	if r.timer != nil {
		r.timer.Stop()
	}
	r.timer = time.NewTimer(progTimerDuration)
	inst := r.instanceSpace[instNo]

	if inst == nil {
		// dlog.Printf("instance %v Creating in commit\n", instNo)
		r.instanceSpace[instNo] = createInstance(nil, ACCEPTED, 0, doMock, r.phase)
		inst = r.instanceSpace[instNo]
	}

	// it's possible that cmds is nil when we aren't committing via AcceptReplys
	if cmds != nil {
		if len(*cmds) == 0 {
			// panic("Commands empty in commit")
		}

		if SANITY_CHECK {
			if inst.status == COMMITTED {
				pc, _, no, ok := runtime.Caller(1)
				details := runtime.FuncForPC(pc)
				if ok && details != nil {
					dlog.Printf("commit() called from %s line %v refusing to commit twice\n", details.Name(), no)
				}
				panic("commit() refusing to commit twice")
			}
		}

		// this copy is often redundant
		inst.cmds = *cmds
		inst.doGhost = doMock
		// dlog.Printf("commit != nil set commands for inst %v %v\n", instNo, inst)
		for _, cmd := range *cmds {
			// r.deleteLatestIfHighest(cmd.ClientId, cmd.OpId)
			r.updateLatestSeen(cmd.Cmd.ClientId, cmd)
		}
		// TODO multi-paxos has a check here to reply to the client if it can
		// r.recordInstanceMetadata(inst)
		// r.sync()
	} else { // handleCommit goes here because the commands aren't sent in the commit message, just the IDs
		log.Panicf("No cmdids in commit\n")
		if SANITY_CHECK {
			if cmdIds == nil {
				panic("nil cmdIds when expected")
			}
			if inst.status == COMMITTED {
				pc, _, no, ok := runtime.Caller(1)
				details := runtime.FuncForPC(pc)
				if ok && details != nil {
					dlog.Printf("commit() called from %s line %v refusing to commit twice\n", details.Name(), no)
				}
				panic("commit() refusing to commit twice")
			}
		}
		// inst.cmds = r.cmdsFromIds(*cmdIds)
		inst.doGhost = doMock
		// todo was
		for _, cmdId := range *cmdIds {
			r.deleteLatestIfHighest(cmdId.ClientId, cmdId.OpId)
			// delete(r.latestCmdSeen, cmdId.ClientId)
		}
		// dlog.Printf("instance %v commit == nil set commands for inst %v\n", instNo, inst)
	}
	inst.status = COMMITTED
	inst.commitTime = time.Now()
	// for _, cmd := range inst.cmds {
	// 	delete(r.cmdQ, state.CommandId{cmd.ClientId, cmd.OpId})
	// }
	r.updateCommittedUpTo()
}

// instance might be nil if we didn't receive anything from the coordinator
func (r *Replica) bcastRotate() {
	log.Printf("Start preparing Rotate message\n")
	if !r.warmupDone {
		dlog.Printf("Warmup not done not sending rotate!\n")
		return
	}
	r.warmupDone = false
	r.surgMock = false
	timer := time.NewTimer(ROTATION_DELAY)
	r.stopEva <- struct{}{}
	go func() {
		<-timer.C
		r.warmupDone = true
		// log.Printf("Allowing rotation again after duration %v...\n", ROTATION_DELAY)
	}()

	// send all instances from committedUpTo until crtInstance-1 crtInstance should be nil
	rotate := &avicennaproto.Rotate{}
	rotate.Phase = r.phase
	rotate.ReplicaId = r.Id
	// firstInst := r.committedUpTo + 1
	firstInst := r.globalMaxCommittedReal
	lastInst := r.advanceCrtInstanceToNextNil() - 1
	if lastInst-firstInst > r.stats.maxRotateSize {
		r.stats.maxRotateSize = lastInst - firstInst
		// log.Printf("Updating maxRotateSize last %v first %v max %v\n,", lastInst, firstInst, r.stats.maxRotateSize)
	}
	// firstInst -= 50
	// if firstInst < 0 {
	// 	firstInst = 0
	// }

	// firstInstMock := r.mockCommittedUpTo + 1
	firstInstMock := r.globalMaxCommittedGhost
	lastInstMock := r.advanceMockCrtInstanceToNextNil() - 1
	if lastInstMock-firstInstMock > r.stats.maxMockRotateSize {
		r.stats.maxMockRotateSize = lastInstMock - firstInstMock
		// log.Printf("Updating maxRotateSize last %v first %v max %v\n,", lastInstMock, firstInstMock, r.stats.maxMockRotateSize)
	}
	// firstInstMock -= 20
	// if firstInstMock < 0 {
	// 	firstInstMock = 0
	// }

	// if lastInst < 0 || firstInst > lastInst {
	// dlog.Printf("No instances to send but am still going to rotate\n")
	// }
	// if lastInstMock < 0 || firstInstMock > lastInstMock {
	// dlog.Printf("No mock instances to send but am still going to rotate\n")
	// }
	log.Printf("Preparing Rotate message, step 1.\n")
	rotate.Instances = make([]avicennaproto.InstanceCommands, lastInst-firstInst+1)
	rotate.MockInstances = make([]avicennaproto.InstanceCommands, lastInstMock-firstInstMock+1)
	log.Printf("In Rotate, real entry num %d, ghost entry num %d\n", lastInst-firstInst+1, lastInstMock-firstInstMock+1)
	for i := firstInst; i <= lastInst; i++ {
		// dlog.Printf("instance %v working on for Rotate message\n", i)
		inst := r.instanceSpace[i]
		if SANITY_CHECK {
			if inst == nil {
				// dlog.Printf("nil instance in middle of Rotate first %v nil %v last %v\n", firstInst, i, lastInst)
				panic("nil instance in Rotate")
			}
		}

		// maybe have a copy function...
		sendingInstance := &rotate.Instances[i-firstInst]
		sendingInstance.Instance = i

		// dlog.Println("Instance in rotate", inst, "instno", i)
		// fill in cmdIds from the instance in the log
		if inst.cmds == nil || len(inst.cmds) <= 0 {
			// dlog.Printf("bcastRotate() empty commands instance %v %v\n", i, inst)
		}
		sendingInstance.Commands = inst.cmds
		// for i, cmd := range inst.cmds {
		// 	sendingInstance.Commands[i].ClientId = cmd.ClientId
		// 	sendingInstance.Commands[i].OpId = cmd.OpId
		// }
		sendingInstance.DoGhost = FALSE
		if inst.doGhost {
			sendingInstance.DoGhost = TRUE
		}

		// COMMITTED and ACCEPTED are the only messages that can be received from the coordinator
		if inst.status == COMMITTED {
			sendingInstance.Status = avicennaproto.COMMITTED
		} else if inst.status == ACCEPTED {
			if SANITY_CHECK {
				if r.isStandby(r.Id) {
					panic("standby replica has an instance with status ACCEPTED in line 4204")
				}
			}
			sendingInstance.Status = avicennaproto.ACCEPTED
		} else {
			// don't send cmdQ, every replica receives every client request
			// and they will have it by the time this arrives if there are no
			// triangle inequalities
			// if len(r.cmdQ) > 0 {
			// 	sendingInstance.CommandIds = append(sendingInstance.CommandIds, r.getAndDeleteQueue()...)
			// }
			sendingInstance.Status = avicennaproto.RECEIVED
		}
		// if sendingInstance.CommandIds == nil || len(sendingInstance.CommandIds) == 0 {
		// 	dlog.Printf("Empty commands in Rotate instance %v\n", i)
		// }
	}
	log.Printf("Preparing Rotate message, step 2.\n")

	for i := firstInstMock; i <= lastInstMock; i++ {
		// dlog.Printf("instance %v working on for Rotate message\n", i)
		inst := r.instanceSpaceMock[i]
		if SANITY_CHECK {
			if inst == nil {
				dlog.Printf("nil instance in middle of Rotate first %v nil %v last %v\n", firstInstMock, i, lastInstMock)
				panic("nil instance in Rotate")
			}
		}

		sendingInstance := &rotate.MockInstances[i-firstInstMock]
		sendingInstance.Instance = i

		// sendingInstance.CommandIds = make([]state.CommandId, len(inst.cmds))
		sendingInstance.Commands = inst.cmds
		// for i, cmd := range inst.cmds {
		// 	sendingInstance.CommandIds[i].ClientId = cmd.ClientId
		// 	sendingInstance.CommandIds[i].OpId = cmd.OpId
		// }
		sendingInstance.DoGhost = FALSE
		if inst.doGhost {
			sendingInstance.DoGhost = TRUE
		}

		// COMMITTED and ACCEPTED are the only messages that can be received from the coordinator
		if inst.status == MOCKCOMMITTED {
			sendingInstance.Status = avicennaproto.GHOSTCOMMITTED
		} else if inst.status == MOCKACCEPTED {
			if SANITY_CHECK {
				if r.isStandbyForPhase(r.Id, r.phase) {
					panic("standby replica has an instance with status ACCEPTED in line 4570")
				}
			}
			sendingInstance.Status = avicennaproto.GHOSTACCEPTED // todo used for both mock and not mock, okay?
		} else {
			// don't send cmdQ, every replica receives every client request
			// and they will have it by the time this arrives if there are no
			// triangle inequalities
			// if len(r.cmdQ) > 0 {
			// 	sendingInstance.CommandIds = append(sendingInstance.CommandIds, r.getAndDeleteQueue()...)
			// }
			sendingInstance.Status = avicennaproto.RECEIVED
		}
		// if sendingInstance.CommandIds == nil || len(sendingInstance.CommandIds) == 0 {
		// 	dlog.Printf("Empty commands in Rotate mock instance %v\n", i)
		// }
	}
	// log.Printf("bcasting Rotate message %v\n", rotate)
	log.Printf("Calling sync() in bcastRotate()\n")
	r.sync()
	r.bcastRotateMsg(r.rotateRPC, rotate, true)
	r.sentForPhase = true
	log.Printf("Finish bcasting Rotate.\n")
	r.trackRotate(rotate)
}

func (r *Replica) handleMockCommit(commit *avicennaproto.Commit) {
	r.stats.nMockCommitsRx++
	r.globalMaxCommittedGhost = commit.GlobalMaxCommit
	for _, instanceCmdId := range commit.Instances {
		if SANITY_CHECK {
			if len(instanceCmdId.Commands) <= 0 {
				// panic("nil commands in commit")
			}
		}
		// stop the timers that might ahve been running
		// it's possible to receive a commit before the Accept
		// r.stopTimers(instanceCmdId.CommandIds, COMMITTED)

		// dlog.Printf("instance %v received MockCommit\n", commit.Instances[0].Instance)
		if inst := r.instanceSpaceMock[instanceCmdId.Instance]; inst != nil {
			if inst.status == MOCKCOMMITTED {
				// dlog.Printf("instance %v Received MockCommit for an already MockCommitted instance %v\n",
				// instanceCmdId.Instance, instanceCmdId.Instance)
				if SANITY_CHECK {
					if len(instanceCmdId.Commands) != len(inst.cmds) {
						pstring := fmt.Sprintf("Commit message had wrong size commands instance %v got %v have %v instanceSpace %v\n",
							instanceCmdId.Instance, len(instanceCmdId.Commands), len(inst.cmds), inst)
						panic(pstring)
					}
				}
				// TODO should maybe make sure they have the same operations
				continue
			}
			// added recently
			// if I am the coordinator and I'm receiving a commit for a non-nil instance
			// then I could have commands in this instance that are not being committed here
			// add them to the cmdQ and retry at the next nil instance
			// if r.isCoordinator(r.Id) {
			// dlog.Printf("instance %v is not committed I am coordinator status is %v\n", instanceCmdId.Instance, inst.status)
			r.addToQueueIfNotPresentMock(inst.cmds, instanceCmdId.Commands)
			// }
		}
		r.mockCommit(instanceCmdId.Instance, &instanceCmdId.Commands, nil, true)
		// added recently
		// todo this is also done as part of long accept, should maybe just make this a function and don't send as part of long accept just the next accept
		// TODO I think there needs to be a !r.sentForPhase check here
		if r.isCurMockCoordinator() && len(r.latestCmdSeenMock) > 0 && !r.sentForPhase {
			// dlog.Printf("instance %v overwritten in commit and cmdQ not empty sending Accept\n", instanceCmdId.Instance)
			instNo := r.advanceMockCrtInstanceToNextNil()
			// dlog.Printf("Calling getAndDeleteQueue from handleMockCommit\n")
			cmds := r.getAndDeleteQueueMock()
			r.instanceSpaceMock[instNo] = createInstance(cmds, MOCKACCEPTED, 0, true, r.phase)
			r.crtInstanceMock++
			// log.Printf("Calling sync() in handleMockCommit()\n")
			// r.sync()
			r.bcastAccept(instNo, cmds)
		}
	}
}

func (r *Replica) handleRealCommit(commit *avicennaproto.Commit) {
	r.stats.nCommitsRx++
	r.globalMaxCommittedReal = commit.GlobalMaxCommit
	for _, instanceCmd := range commit.Instances {
		if SANITY_CHECK {
			if len(instanceCmd.Commands) <= 0 {
				// panic("nil commands in commit")
			}
		}
		// stop the timers that might ahve been running
		// it's possible to receive a commit before the Accept
		// r.stopTimers(instanceCmdId.CommandIds, COMMITTED)

		// dlog.Printf("instance %v received REAL commit\n", commit.Instances[0].Instance)
		if inst := r.instanceSpace[instanceCmd.Instance]; inst != nil {
			if inst.status == COMMITTED {
				// dlog.Printf("instance %v Received Commit for an already committed instance %v\n",
				// instanceCmdId.Instance, instanceCmdId.Instance)
				if SANITY_CHECK {
					if len(instanceCmd.Commands) != len(inst.cmds) {
						pstring := fmt.Sprintf("Commit message had wrong size commands instance %v got %v have %v instanceSpace %v\n",
							instanceCmd.Instance, len(instanceCmd.Commands), len(inst.cmds), inst)
						panic(pstring)
					}
				}
				// TODO should maybe make sure they have the same operations
				continue
			}
			// added recently
			// if I am the coordinator and I'm receiving a commit for a non-nil instance
			// then I could have commands in this instance that are not being committed here
			// add them to the cmdQ and retry at the next nil instance
			// if r.isCoordinator(r.Id) {
			// dlog.Printf("instance %v is not committed I am coordinator status is %v\n", instanceCmdId.Instance, inst.status)
			r.addToQueueIfNotPresent(inst.cmds, instanceCmd.Commands)
			// }
		}
		r.commit(instanceCmd.Instance, &instanceCmd.Commands, nil, instanceCmd.DoGhost > 0)
		// added recently
		// todo this is also done as part of long accept, should maybe just make this a function and don't send as part of long accept just the next accept
		if r.isCoordinator(r.Id) && len(r.latestCmdSeen) > 0 && !r.sentForPhase {
			// dlog.Printf("instance %v overwritten in commit and cmdQ not empty sending Accept\n", instanceCmdId.Instance)
			instNo := r.advanceCrtInstanceToNextNil()
			// dlog.Printf("Calling getAndDeleteQueue from handleCommit\n")
			cmds := r.getAndDeleteQueue()
			// TODO this will lose the request to mock from the client - I guess this could result in the client sending AtLeast messages when it is slow
			r.instanceSpace[instNo] = createInstance(cmds, ACCEPTED, 0, false, r.phase)
			r.crtInstance++
			// log.Printf("Calling sync() in handleRealCommit()\n")
			// r.sync()
			r.bcastAccept(instNo, cmds)
		}
	}
}

func (r *Replica) handleCommit(commit *avicennaproto.Commit) {
	if commit.Mock == TRUE {
		// part of old version
		// Real Coordinators also mock commit now
		// if r.isCoordinator(r.Id) {
		// 	return
		// }
		// dlog.Printf("instance %v Notification of MockCommit from %v for their inst %v",
		// commit.Instances[0].Instance, commit.ReplicaId, commit.Instances[0].Instance)
		r.handleMockCommit(commit)
		return
	} else {
		// startHandle := time.Now()
		// new version
		r.handleRealCommit(commit)
		// handleLat := time.Since(startHandle)
		// r.MsgParseLatChan <- &handleLat
		return
	}

	// old version
	// for _, instanceCmdId := range commit.Instances {
	// 	if SANITY_CHECK {
	// 		if len(instanceCmdId.CommandIds) <= 0 {
	// 			// panic("nil commands in commit")
	// 		}
	// 	}
	// 	// stop the timers that might ahve been running
	// 	// it's possible to receive a commit before the Accept
	// 	r.stopTimers(instanceCmdId.CommandIds, COMMITTED)

	// 	dlog.Printf("instance %v received commit\n", commit.Instances[0].Instance)
	// 	if inst := r.instanceSpace[instanceCmdId.Instance]; inst != nil {
	// 		if inst.status == COMMITTED {
	// 			dlog.Printf("instance %v Received Commit for an already committed instance %v\n",
	// 				instanceCmdId.Instance, instanceCmdId.Instance)
	// 			if SANITY_CHECK {
	// 				if len(instanceCmdId.CommandIds) != len(inst.cmds) {
	// 					pstring := fmt.Sprintf("Commit message had wrong size commands instance %v got %v have %v instanceSpace %v\n",
	// 						instanceCmdId.Instance, len(instanceCmdId.CommandIds), len(inst.cmds), inst)
	// 					panic(pstring)
	// 				}
	// 			}
	// 			// TODO should maybe make sure they have the same operations
	// 			continue
	// 		}
	// 		// added recently
	// 		// if I am the coordinator and I'm receiving a commit for a non-nil instance
	// 		// then I could have commands in this instance that are not being committed here
	// 		// add them to the cmdQ and retry at the next nil instance
	// 		// if r.isCoordinator(r.Id) {
	// 		dlog.Printf("instance %v is not committed I am coordinator status is %v\n", instanceCmdId.Instance, inst.status)
	// 		r.addToQueueIfNotPresent(inst.cmds, instanceCmdId.CommandIds)
	// 		// }
	// 	}
	// 	r.commit(instanceCmdId.Instance, nil, &instanceCmdId.CommandIds)
	// 	// added recently
	// 	// todo this is also done as part of long accept, should maybe just make this a function and don't send as part of long accept just the next accept
	// 	if r.isCoordinator(r.Id) && len(r.latestCmdSeen) > 0 {
	// 		dlog.Printf("instance %v overwritten in commit and cmdQ not empty sending Accept\n", instanceCmdId.Instance)
	// 		instNo := r.advanceCrtInstanceToNextNil()
	// 		dlog.Printf("Calling getAndDeleteQueue from handleCommit\n")
	// 		cmdIds := r.getAndDeleteQueue()
	// 		r.instanceSpace[instNo] = createInstance(r.cmdsFromIds(cmdIds), ACCEPTED, 0)
	// 		r.crtInstance++
	// 		r.bcastAccept(instNo, cmdIds)
	// 	}
	// }

	// r.updateCommittedUpTo()
	// r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	// r.recordCommands(commit.Command)
}

// Metadata Struct to help process Received Quorums
type instlb struct {
	instance int32
	Commands []state.CommandAvi // TODO I think I need to change this to CommandIds???
	status   uint8
	phase    int32
}

func (r *Replica) trackRotate(rotate *avicennaproto.Rotate) bool {
	r.nRotateMessages++
	// wish I just kept the message...
	// add instances
	if len(rotate.Instances) > 0 {
		r.RotateMessages[rotate.ReplicaId] = &rotate.Instances
	} else {
		r.RotateMessages[rotate.ReplicaId] = nil
	}
	// add Mock instances
	if len(rotate.MockInstances) > 0 {
		r.RotateMessagesMock[rotate.ReplicaId] = &rotate.MockInstances
	} else {
		r.RotateMessagesMock[rotate.ReplicaId] = nil
	}

	if len(r.RotateMessages) > 1 {
		return r.gotRotateQuorum()
	}
	return false
}

func (r *Replica) goToPhase(phase int32) {
	r.sync()
	r.phase = phase
	r.nRotateMessages = 0
	for id := range r.RotateMessages {
		delete(r.RotateMessages, id)
	}
	for id := range r.RotateMessagesMock {
		delete(r.RotateMessagesMock, id)
	}
	r.sentForPhase = false
	if r.timer != nil {
		r.timer.Stop()
	}
	r.timer = time.NewTimer(progTimerDuration)

	log.Printf("Calling sync() in goToPhase()\n")

	// TODO remove this
	// log.Printf("Processed Rotate quorum and beginning phase %v\n", phase)
	// commitDiff := r.getMinQuorumLatencyForReplica(r.configurationForPhase(r.phase+1).coordinator, r.curCoordinator()) - r.getMinQuorumLatencyForReplica(r.curCoordinator(), -1)
	// log.Printf("commitDiff between cur %v and next %v is %v\n", r.curCoordinator(), r.configurationForPhase(r.phase+1).coordinator, commitDiff)
}

func (r *Replica) createRotateMetadataMapMock() map[int32]*instlb {
	// create a metadata structure to make processing the mesages easier...
	// maps instance number to an instlb metadata struct
	instMetadataMapMock := make(map[int32]*instlb)
	// replicaInstances is []InstanceCommandIds
	if SANITY_CHECK && len(r.RotateMessagesMock) == 0 {
		log.Panic("MOCK: RotateMessagesMock was empty\n")
	}
	// maxInstReceived := int32(0)
	// for _, msg := range r.RotateMessagesMock {
	// 	if msg != nil && len(*msg) != 0 {
	// 		if (*msg)[len(*msg)-1].Instance > maxInstReceived {
	// 			maxInstReceived = (*msg)[len(*msg)-1].Instance
	// 		}
	// 	}
	// }
	// dlog.Printf("maxInstReceived %v\n", maxInstReceived)
	for rid, replicaInstances := range r.RotateMessagesMock { // I think r.RotateMessagesMock is the only difference from the REAL version should just consolidate
		if r.isStandby(rid) {
			continue
		}
		if replicaInstances == nil {
			// dlog.Printf("MOCK: createRotateMetadataMap Instances was nil\n")
			continue
		}

		// for this replicas message fill in nil values for the instances it does not know about
		// maxInstInMsg := (*replicaInstances)[len(*replicaInstances)-1].Instance
		// maxInstInMsg++
		// for ; maxInstInMsg <= maxInstReceived; maxInstInMsg++ {
		// 	// dlog.Printf("adding nil command %v\n", maxInstInMsg)
		// 	// empty commands we create to fill holes are not mocked
		// 	*replicaInstances = append(*replicaInstances, avicennaproto.InstanceCommands{maxInstInMsg, avicennaproto.NOT_RECEIVED, nil, FALSE})
		// }

		// receivedInst is InstanceCommandIds
		for _, receivedInst := range *replicaInstances {
			// instMetadata is *instlb
			if instMetadata, exist := instMetadataMapMock[receivedInst.Instance]; exist && instMetadata != nil {
				if instMetadata.status == avicennaproto.GHOSTCOMMITTED {
					continue
				}

				if instMetadata.status == avicennaproto.GHOSTACCEPTED {
					if receivedInst.Status == avicennaproto.GHOSTCOMMITTED {
						if instMetadata.phase < receivedInst.Phase {
							instMetadata.phase = receivedInst.Phase
							instMetadata.Commands = receivedInst.Commands
							instMetadata.status = avicennaproto.GHOSTCOMMITTED
						} else if instMetadata.phase == receivedInst.Phase {
							if SANITY_CHECK && len(receivedInst.Commands) != len(instMetadata.Commands) {
								log.Panicf("For inst %d, accepted %d cmds but committed %d cmds.\n", receivedInst.Instance, instMetadata.Commands, receivedInst.Commands)
							}

							instMetadata.status = avicennaproto.GHOSTCOMMITTED
							instMetadata.phase = receivedInst.Phase
							continue
						} else {
							if SANITY_CHECK {
								log.Panicf("For inst %d, accepted at phase %d but committed at phase %d.\n", receivedInst.Instance, instMetadata.phase, receivedInst.Phase)
							}
						}
					}

					if receivedInst.Status == avicennaproto.GHOSTACCEPTED {
						if instMetadata.phase < receivedInst.Phase {
							instMetadata.status = avicennaproto.GHOSTACCEPTED
							instMetadata.phase = receivedInst.Phase
							instMetadata.Commands = receivedInst.Commands
						} else if instMetadata.phase == receivedInst.Phase {
							if SANITY_CHECK && len(receivedInst.Commands) != len(instMetadata.Commands) {
								log.Panicf("For inst %d, accepted %d cmds but also accpeted %d cmds.\n", receivedInst.Instance, instMetadata.Commands, receivedInst.Commands)
							}

							continue
						} else {

						}
					}
					continue
				}

				if instMetadata.status == avicennaproto.GHOSTACCEPTED {
					if receivedInst.Status == avicennaproto.GHOSTCOMMITTED {
						instMetadata.Commands = receivedInst.Commands
						instMetadata.status = avicennaproto.GHOSTCOMMITTED
						instMetadata.phase = receivedInst.Phase
						continue
					}

					if receivedInst.Status == avicennaproto.GHOSTACCEPTED {
						instMetadata.Commands = receivedInst.Commands
						instMetadata.status = avicennaproto.GHOSTACCEPTED
						instMetadata.phase = receivedInst.Phase
						continue
					}
					continue
				}
			}

			// We haven't met any entry yet
			instMetadataMapMock[receivedInst.Instance] = &instlb{receivedInst.Instance, nil, 0, 0}
			if receivedInst.Status == avicennaproto.GHOSTCOMMITTED {
				instMetadataMapMock[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMapMock[receivedInst.Instance].status = avicennaproto.GHOSTCOMMITTED
				instMetadataMapMock[receivedInst.Instance].phase = receivedInst.Phase
				continue
			}

			if receivedInst.Status == avicennaproto.GHOSTACCEPTED {
				instMetadataMapMock[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMapMock[receivedInst.Instance].status = avicennaproto.GHOSTACCEPTED
				instMetadataMapMock[receivedInst.Instance].phase = receivedInst.Phase
				continue
			}

			if receivedInst.Status == avicennaproto.RECEIVED {
				instMetadataMapMock[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMapMock[receivedInst.Instance].status = avicennaproto.RECEIVED
				instMetadataMapMock[receivedInst.Instance].phase = receivedInst.Phase
				continue
			}

			// // create if this is the first message containing this instance of consensus
			// if instMetadata == nil {
			// 	instMetadataMapMock[receivedInst.Instance] = &instlb{receivedInst.Instance, 0, 0, 0, 0, nil, make(map[state.CommandAvi]bool), false, false}
			// 	instMetadata = instMetadataMapMock[receivedInst.Instance]
			// 	// dlog.Printf("MOCK: createRotateMetadataMapMock() creating instance metadata: %v\n", *instMetadata)
			// }

			// // dlog.Printf("MOCK: createRotateMetadataMapMock() looking at instance metadata: %v\n", *instMetadata)
			// // if receivedInst.CommandIds == nil || len(receivedInst.CommandIds) <= 0 {
			// // 	// dlog.Printf("MOCK: createRotateMetadataMapMock() Received empty commands in instance %v message: %v\n",
			// // 	// 	receivedInst.Instance, receivedInst)
			// // 	// panic("Received empty commands in Rotate!?")
			// // }

			// // this should be MOCKCOMMITTED...
			// if receivedInst.Status == avicennaproto.COMMIT {
			// 	// dlog.Printf("Received a MOCK committed instance in Rotate %v nreceived -1 %v\n", receivedInst.Status, instMetadata.nreceived)
			// 	instMetadata.committed = true
			// 	instMetadata.Commands = receivedInst.Commands
			// }

			// if r.isStandby(rid) {
			// 	log.Printf("Wierd, we don't consider logs from standbys.\n")
			// }
			// // if r.isDelegate(rid) {
			// // 	instMetadata.nDelegateVotes++
			// // } else if r.isStandby(rid) {
			// // 	instMetadata.nStandbyVotes++
			// // } else {
			// // 	instMetadata.nreceived++ // unread...?
			// // }
			// if receivedInst.Status == avicennaproto.RECEIVED {
			// 	if SANITY_CHECK {
			// 		if r.isStandby(rid) {
			// 			panic("MOCK: createRotateMetadataMapMock() received RECEIVED rotate from a standby replica")
			// 		}
			// 	}
			// 	///// Sanity Check
			// 	// todo remove sanity checks during experimentation?
			// 	if SANITY_CHECK {
			// 		if instMetadata.receivedOne {
			// 			if len(receivedInst.Commands) != len(instMetadata.Commands) {
			// 				log.Panicf("MOCK: Commands length disagreement in Received quorum %v vs %v\n",
			// 					len(receivedInst.Commands), len(instMetadata.Commands))
			// 			}
			// 			mdCmdsMap := make(map[state.Command]bool)

			// 			for _, cmd := range receivedInst.Commands {
			// 				mdCmdsMap[cmd.Cmd] = true
			// 			}
			// 			for _, cmd := range instMetadata.Commands {
			// 				if _, ok := mdCmdsMap[cmd.Cmd]; !ok {
			// 					// this fired!
			// 					panic("MOCK: MOCK: Commands in receivedQuorum with RECEIVED status do not match")
			// 				}
			// 			}
			// 		}
			// 	}
			// 	///// End Sanity Check

			// 	instMetadata.nvals++
			// 	instMetadata.Commands = receivedInst.Commands
			// 	instMetadata.receivedOne = true
			// 	for _, cmdId := range receivedInst.Commands {
			// 		delete(instMetadata.potentialCmdsToAddMap, cmdId)
			// 	}
			// 	// dlog.Printf("MOCK: createRotateMetadataMapMock() this instance received an Accept metadata now: %v\n", *instMetadata)
			// } else if !instMetadata.receivedOne {
			// 	// todo for now we propose something another replica wants?
			// 	// dlog.Printf("MOCK: createRotateMetadataMapMock() before adding to cmds map metadata now: %v\n", *instMetadata)
			// 	for _, cmdId := range receivedInst.Commands {
			// 		instMetadata.potentialCmdsToAddMap[cmdId] = true
			// 	}
			// 	// instMetadata.Commands = r.cmdsFromIds(receivedInst.CommandIds)
			// 	// dlog.Printf("MOCK: createRotateMetadataMapMock() this instance did not receive an Accept metadata now: %v\n", *instMetadata)

			// }
		}
	}
	// for instNo, instlb := range instMetadataMapMock {
	// 	totalVotes := 0
	// 	if instlb.nDelegateVotes > 0 {
	// 		totalVotes = int(instlb.nreceived + 1 + instlb.nDelegateVotes*uint8(len(r.curConfiguration().standByReplicas)))
	// 	} else {
	// 		totalVotes = int(instlb.nreceived + instlb.nStandbyVotes)
	// 	}
	// 	if totalVotes <= r.N>>1 {
	// 		// no quorum needs to either be committed or committed in my log
	// 		if r.instanceSpaceMock[instNo] != nil && r.instanceSpaceMock[instNo].status != MOCKCOMMITTED && !instlb.committed {
	// 			log.Panicf("Did not get enough rotate messages for instance %v. It was not MOCK committed and did not receive quorum total votes %v metadata: %v\n", instNo, totalVotes, instlb)
	// 		}
	// 	}
	// }
	return instMetadataMapMock
}

func (r *Replica) createRotateMetadataMap() map[int32]*instlb {
	// create a metadata structure to make processing the mesages easier...
	// maps instance number to an instlb metadata struct
	instMetadataMap := make(map[int32]*instlb)
	// replicaInstances is []InstanceCommandIds
	// maxInstReceived := int32(0)
	// for _, msg := range r.RotateMessages {
	// 	if msg != nil && len(*msg) != 0 {
	// 		if (*msg)[len(*msg)-1].Instance > maxInstReceived {
	// 			maxInstReceived = (*msg)[len(*msg)-1].Instance
	// 		}
	// 	}
	// }

	for rid, replicaInstances := range r.RotateMessages {
		if r.isStandby(rid) {
			continue
		}
		if replicaInstances == nil {
			continue
		}

		// // for this replicas message fill in nil values for the instances it does not know about
		// maxInstInMsg := (*replicaInstances)[len(*replicaInstances)-1].Instance
		// maxInstInMsg++
		// for ; maxInstInMsg <= maxInstReceived; maxInstInMsg++ {
		// 	// dlog.Printf("adding nil command %v\n", maxInstInMsg)
		// 	*replicaInstances = append(*replicaInstances, avicennaproto.InstanceCommands{maxInstInMsg, avicennaproto.NOT_RECEIVED, nil, FALSE})
		// }

		// receivedInst is InstanceCommandIds
		for _, receivedInst := range *replicaInstances {
			if instMetadata, exist := instMetadataMap[receivedInst.Instance]; exist && instMetadata != nil {
				if instMetadata.status == avicennaproto.COMMITTED {
					continue
				}

				if instMetadata.status == avicennaproto.ACCEPTED {
					if receivedInst.Status == avicennaproto.COMMITTED {
						if instMetadata.phase < receivedInst.Phase {
							instMetadata.status = avicennaproto.COMMITTED
							instMetadata.Commands = receivedInst.Commands
							instMetadata.phase = receivedInst.Phase
						} else if instMetadata.phase == receivedInst.Phase {
							if SANITY_CHECK && len(receivedInst.Commands) != len(instMetadata.Commands) {
								log.Panicf("For inst %d, accepted %d cmds but committed %d cmds.\n", receivedInst.Instance, instMetadata.Commands, receivedInst.Commands)
							}

							instMetadata.status = avicennaproto.COMMITTED
							continue
						} else {
							if SANITY_CHECK {
								log.Panicf("For inst %d, accepted at phase %d but committed at phase %d.\n", receivedInst.Instance, instMetadata.phase, receivedInst.Phase)
							}
						}
					}

					if receivedInst.Status == avicennaproto.ACCEPTED {
						if instMetadata.phase < receivedInst.Phase {
							instMetadata.status = avicennaproto.ACCEPTED
							instMetadata.Commands = receivedInst.Commands
							instMetadata.phase = receivedInst.Phase
						} else if instMetadata.phase == receivedInst.Phase {
							if SANITY_CHECK && len(receivedInst.Commands) != len(instMetadata.Commands) {
								log.Panicf("For inst %d, accepted %d cmds but also accpeted %d cmds.\n", receivedInst.Instance, instMetadata.Commands, receivedInst.Commands)
							}
							instMetadata.status = avicennaproto.ACCEPTED
						} else {

						}
					}
					continue
				}

				if instMetadata.status == avicennaproto.RECEIVED {
					if receivedInst.Status == avicennaproto.COMMITTED {
						instMetadata.Commands = receivedInst.Commands
						instMetadata.status = avicennaproto.COMMITTED
						instMetadata.phase = receivedInst.Phase
						continue
					}

					if receivedInst.Status == avicennaproto.ACCEPTED {
						instMetadata.Commands = receivedInst.Commands
						instMetadata.status = avicennaproto.ACCEPTED
						instMetadata.phase = receivedInst.Phase
						continue
					}
					continue
				}
			}

			// We haven't met any entry yet
			instMetadataMap[receivedInst.Instance] = &instlb{receivedInst.Instance, nil, 0, 0}
			if receivedInst.Status == avicennaproto.COMMITTED {
				instMetadataMap[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMap[receivedInst.Instance].status = avicennaproto.COMMITTED
				instMetadataMap[receivedInst.Instance].phase = receivedInst.Phase
				continue
			}

			if receivedInst.Status == avicennaproto.ACCEPTED {
				instMetadataMap[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMap[receivedInst.Instance].status = avicennaproto.ACCEPTED
				continue
			}

			if receivedInst.Status == avicennaproto.RECEIVED {
				instMetadataMap[receivedInst.Instance].Commands = receivedInst.Commands
				instMetadataMap[receivedInst.Instance].status = avicennaproto.RECEIVED
				instMetadataMap[receivedInst.Instance].phase = receivedInst.Phase
				continue
			}

			// // instMetadata is *instlb
			// instMetadata := instMetadataMap[receivedInst.Instance]

			// // create if this is the first message containing this instance of consensus
			// if instMetadata == nil {
			// 	instMetadataMap[receivedInst.Instance] = &instlb{receivedInst.Instance, 0, 0, 0, 0, nil, make(map[state.CommandAvi]bool), false, false}
			// 	instMetadata = instMetadataMap[receivedInst.Instance]
			// 	// dlog.Printf("createRotateMetadataMap() creating instance metadata: %v\n", *instMetadata)
			// }

			// // dlog.Printf("createRotateMetadataMap() looking at instance metadata: %v\n", *instMetadata)
			// if receivedInst.Commands == nil || len(receivedInst.Commands) <= 0 {
			// 	// dlog.Printf("createRotateMetadataMap() Received empty commands in instance %v message: %v\n",
			// 	// receivedInst.Instance, receivedInst)
			// 	// panic("Received empty commands in Rotate!?")
			// }

			// if receivedInst.Status == avicennaproto.COMMITTED {
			// 	// dlog.Printf("Received a REAL committed instance in Rotate %v nreceived - 1 %v\n", receivedInst.Status, instMetadata.nreceived)
			// 	instMetadata.committed = true
			// 	// need to add the commands I think...
			// 	instMetadata.Commands = receivedInst.Commands
			// }

			// if r.isStandby(rid) {
			// 	log.Printf("Wierd, we don't consider logs from standbys.\n")
			// }

			// // if r.isDelegate(rid) {
			// // 	instMetadata.nDelegateVotes++
			// // } else if r.isStandby(rid) {
			// // 	instMetadata.nStandbyVotes++
			// // } else {
			// // 	instMetadata.nreceived++ // unread...?
			// // }
			// if receivedInst.Status == avicennaproto.RECEIVED {
			// 	if SANITY_CHECK {
			// 		if r.isStandby(rid) {
			// 			panic("createRotateMetadataMap() received RECEIVED rotate from a standby replica")
			// 		}
			// 	}
			// 	///// Sanity Check
			// 	// todo remove sanity checks during experimentation?
			// 	if SANITY_CHECK {
			// 		if instMetadata.receivedOne {
			// 			if len(receivedInst.Commands) != len(instMetadata.Commands) {
			// 				log.Panicf("Commands length disagreement in Received quorum %v vs %v\n",
			// 					len(receivedInst.Commands), len(instMetadata.Commands))
			// 			}
			// 			mdCmdsMap := make(map[state.Command]bool)

			// 			for _, cmd := range receivedInst.Commands {
			// 				mdCmdsMap[cmd.Cmd] = true
			// 			}
			// 			for _, cmd := range instMetadata.Commands {
			// 				if _, ok := mdCmdsMap[cmd.Cmd]; !ok {
			// 					panic("Commands in receivedQuorum with RECEIVED status do not match")
			// 				}
			// 			}
			// 		}
			// 	}
			// 	///// End Sanity Check

			// 	instMetadata.nvals++
			// 	instMetadata.Commands = receivedInst.Commands
			// 	instMetadata.receivedOne = true
			// 	for _, cmdId := range receivedInst.Commands {
			// 		delete(instMetadata.potentialCmdsToAddMap, cmdId)
			// 	}
			// 	// dlog.Printf("createRotateMetadataMap() this instance received an Accept metadata now: %v\n", *instMetadata)
			// } else if !instMetadata.receivedOne {
			// 	// todo for now we propose something another replica wants?
			// 	// dlog.Printf("createRotateMetadataMap() before adding to cmds map metadata now: %v\n", *instMetadata)
			// 	for _, cmd := range receivedInst.Commands {
			// 		instMetadata.potentialCmdsToAddMap[cmd] = true
			// 	}
			// 	// instMetadata.Commands = r.cmdsFromIds(receivedInst.CommandIds)
			// 	// dlog.Printf("createRotateMetadataMap() this instance did not receive an Accept metadata now: %v\n", *instMetadata)

			// }
		}
	}
	// for instNo, instlb := range instMetadataMap {
	// 	totalVotes := 0
	// 	if instlb.nDelegateVotes > 0 {
	// 		totalVotes = int(instlb.nreceived + 1 + instlb.nDelegateVotes*uint8(len(r.curConfiguration().standByReplicas)))
	// 	} else {
	// 		totalVotes = int(instlb.nreceived + instlb.nStandbyVotes)
	// 	}
	// 	if totalVotes <= r.N>>1 {
	// 		// no quorum needs to either be committed or committed in my log
	// 		if r.instanceSpace[instNo] != nil && r.instanceSpace[instNo].status != COMMITTED && !instlb.committed {
	// 			log.Panicf("Did not get enough rotate messages for instance %v was not REAL committed and did not received quorum total votes %v metadata: %v\n", instNo, totalVotes, instlb)
	// 		}
	// 	}
	// }
	return instMetadataMap
}

func (r *Replica) processRotateQuorum() {
	// TODO I THINK WE NEED NOT THIS
	r.stopAllTimers(NULL)
	// for _, timer := range r.cmdTimers {
	// 	timer.Stop()
	// }
	dlog.Printf("===================== Processing Rotate Quorum =====================\n")
	// if I got a quorum and didn't send my rotate message then they will not have enough acceptors
	// to accept, so I should broadcast my rotate message mainly to update internal state with my knowledge
	// this isn't strictly necessary but makes implementing it easier...
	// this is necessary because I need to be able to make sure requests I have are included when broadcasting the new accept as the coordinator
	if !r.sentForPhase {
		r.bcastRotate()
	}

	// TODO TODO TODO if advcurinstancenextnil is less than max here create the instances, when they are send int long accept they will (todo check) be replied to with committed as part of the accept

	instMetadataMap := r.createRotateMetadataMap()
	instMetadataMapMock := r.createRotateMetadataMapMock()
	dlog.Printf("instMetadataMap is %v\n", instMetadataMap)
	for i, mdMap := range instMetadataMap {
		dlog.Printf("%v %v\n", i, mdMap)
	}
	dlog.Printf("instMetadataMapMock is %v\n", instMetadataMapMock)
	for i, mdMap := range instMetadataMapMock {
		dlog.Printf("%v %v\n", i, mdMap)
	}

	// iterate through the metadata map and check what each instance should be.
	for _, instMetadata := range instMetadataMap {
		inst := r.instanceSpace[instMetadata.instance]
		dlog.Printf("REAL: Examining instance %v %v\n", instMetadata.instance, inst)

		// if we don't know about it but someone does, create it
		nextNil := r.advanceCrtInstanceToNextNil()
		for instMetadata.instance > nextNil {
			dlog.Printf("REAL: instance %v phase %v wasn't created in processRotateQuorum when looking at instance %v creating %v\n",
				nextNil, r.phase, instMetadata.instance, nextNil)
			r.instanceSpace[nextNil] = createInstance(nil, NULL, 0, false, r.phase)
			nextNil = r.advanceCrtInstanceToNextNil()
		}

		if inst == nil {
			dlog.Printf("REAL: Creating instance %v in Rotate phase %v\n", instMetadata.instance, r.phase)
			r.instanceSpace[instMetadata.instance] = createInstance(nil, NULL, 0, false, r.phase)
			inst = r.instanceSpace[instMetadata.instance]
			if instMetadata.status == avicennaproto.COMMITTED {
				r.commit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}
			if instMetadata.status == avicennaproto.ACCEPTED {
				inst.status = ACCEPTED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = false
				continue
			}
			if instMetadata.status == avicennaproto.RECEIVED {
				inst.status = RECEIVED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = false
				continue
			}
			continue
		}
		// it's possible the replica received a value from another coordinator
		// and included the instance in the message because the sender did not know it was committed
		// they will learn in the commit message I sent (or another replica sent) when I set this to committed (thanks TCP)
		// TODO should every replica send always? (maybe useful for WAN)
		if inst.status == COMMITTED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.COMMITTED {
				if SANITY_CHECK && len(instMetadata.Commands) != len(inst.cmds) {
					log.Panicf("For inst %d, locally commit %d cmds, but receive commit %d cmds.\n", instMetadata.instance, inst.cmds, instMetadata.Commands)
				}
				r.bcastCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.ACCEPTED {
				log.Printf("Weird, log entry %d commit locally, but accepted after merging.\n", instMetadata.instance)
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				log.Printf("Weird, log entry %d commit locally, but received after merging.\n", instMetadata.instance)
			}
			// for _, cmd := range inst.cmds {
			// 	delete(instMetadata.potentialCmdsToAddMap, cmd) //state.CommandId{cmd.ClientId, cmd.OpId})
			// }
			// for cmd, _ := range instMetadata.potentialCmdsToAddMap {
			// 	r.addCmdIdsToQueueIfNotPresent([]state.CommandAvi{cmd}, nil)
			// 	// this code always executed
			// 	// if md, ok := r.cmdMetadata[cmdId]; !ok || (md.status != ACCEPTED && md.status != COMMITTED) {
			// 	// 	dlog.Printf("REAL: Adding %v to cmdQ in COMMITTED rotate\n", cmdId)
			// 	// 	r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
			// 	// 	// r.cmdQ[cmdId] = trueƒ
			// 	// }
			// }
			continue
		}
		if inst.status == ACCEPTED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.COMMITTED {
				if SANITY_CHECK && len(instMetadata.Commands) != len(inst.cmds) {
					log.Panicf("For inst %d, locally accepted %d cmds, but receive commit %d cmds.\n", instMetadata.instance, inst.cmds, instMetadata.Commands)
				}
				r.commit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.ACCEPTED {
				continue
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				log.Printf("Weird, log entry %d accepted locally, but received after merging.\n", instMetadata.instance)
			}
			continue
		}
		if inst.status == RECEIVED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.COMMITTED {
				r.commit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.ACCEPTED {
				inst.status = ACCEPTED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = false
				continue
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				continue
			}
			continue
		}

		// if instMetadata.committed {
		// 	r.commit(instMetadata.instance, &instMetadata.Commands, nil)
		// }

		// if this replica received a quorum of vals it can commit
		// if int(instMetadata.nvals) > 1 || instMetadata.committed {
		// 	// if we are committing more than one instance at a time we should batch them together?
		// 	log.Printf("REAL: Rotate committing %v, commands %v\n", instMetadata.instance, instMetadata.Commands)
		// 	r.commit(instMetadata.instance, &instMetadata.Commands, nil, inst.doMock)
		// 	r.bcastCommit(instMetadata.instance, instMetadata.Commands)
		// 	// r.broadcastCommitToClientChan <- InstWithNum{Instance: *inst, instNo: instMetadata.instance}
		// 	// r.bcastRealCommitToClients(instMetadata.instance)
		// } else {
		// 	// received at least one value: this replica MUST propose it
		// 	// TODO if standby but accepted a value in a previous phase,
		// 	// it does not enter this case because it doesn't send RECEIVED for
		// 	// its own value because it is the standby, thus it STAYS ACCEPTED
		// 	// in the instance when it is no longer standby
		// 	if SANITY_CHECK {
		// 		if !instMetadata.receivedOne && r.isStandby(r.Id) && inst.status == ACCEPTED {
		// 			panic("REAL: I am a standby that accepted some instance")
		// 		}
		// 	}
		// 	if instMetadata.receivedOne {
		// 		// need to propose the correct val, necessary for correctness
		// 		dlog.Printf("REAL: Setting inst.cmds to %v for instno %v inst %v\n", instMetadata.Commands, instMetadata.instance, inst)
		// 		// if I accepted a value in this phase it better be the same set of cmds
		// 		// SANITY CHECK
		// 		if SANITY_CHECK {
		// 			if inst.status == ACCEPTED {
		// 				// tmp := make(map[state.CommandId]bool)
		// 				tmp := make(map[state.CommandId]int)
		// 				total := 0
		// 				for _, cmd := range instMetadata.Commands {
		// 					tmp[state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}]++ // = true
		// 					total++
		// 				}
		// 				if total != len(inst.cmds) {
		// 					log.Panicf("Real: Rotate len mismatch \n\t inst cmds %v \n\t received cmds %v\n", inst.cmds, instMetadata.Commands)
		// 				}
		// 				for _, cmd := range inst.cmds {
		// 					cmdId := state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}
		// 					if SANITY_CHECK {
		// 						if _, ok := tmp[cmdId]; !ok {
		// 							log.Panicf("REAL: Accepted command %v not in RECEIVED instance \n\tinst cmds %v \n\treceived commands %v\n\ttmp %v\n",
		// 								cmdId, inst.cmds, instMetadata.Commands, tmp)
		// 						} else {
		// 							tmp[cmdId]--
		// 						}
		// 					}
		// 					// not sure why this wasn't firing when rotating in handlePropose? it should cause it to fire whenever a duplicate was in accept
		// 					// delete(tmp, cmdId)
		// 				}
		// 				for _, count := range tmp {
		// 					if count != 0 {
		// 						log.Panicf("REAL: Rotate: commands mismatch: \n\t inst %v\n\t received %v\n\t tmp %v\n", inst.cmds, instMetadata.Commands, tmp)
		// 					}
		// 				}
		// 				// if len(tmp) > 0 {
		// 				// 	panic("REAL: Accepted command does not have command in RECEIVED instance")
		// 				// }
		// 			}
		// 		}
		// 		// TODO I don't think we should add to the queue in rotate
		// 		if r.isCoordinatorForPhase(r.Id, r.phase+1) {
		// 			r.addToQueueIfNotPresent(inst.cmds, instMetadata.Commands) // r.idsFromCmds(instMetadata.Commands))
		// 			// don't add other replicas commands to my queue
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// 	if md, ok := r.cmdMetadata[cmdId]; !ok || (md.status != ACCEPTED && md.status != COMMITTED) {
		// 			// 		dlog.Printf("Adding %v to cmdQ in received from other rotate\n", cmdId)
		// 			// 		r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
		// 			// 	}
		// 			// }
		// 		}
		// 		inst.cmds = instMetadata.Commands
		// 		inst.status = RECEIVEDFROMOTHER
		// 		dlog.Printf("REAL: Rotate setting %v but no quorum of vals phase %v\n", instMetadata.instance, r.phase)
		// 		// any cmdId leftover in the map should be added to the cmdQ to be accepted later
		// 		dlog.Printf("REAL: processRotateQuorum() adding %v to cmdQ\n", instMetadata.potentialCmdsToAddMap)
		// 		// TODO this can be function call

		// 	} else { // received no values in this phase
		// 		// TODO I think I should remove this
		// 		if inst.status != RECEIVEDFROMOTHER && inst.status != ACCEPTED {
		// 			// TODO don't do this?
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// dlog.Printf("Adding %v to cmdQ\n", cmdId)
		// 			// r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
		// 			// r.cmdQ[cmdId] = true
		// 			// }
		// 			cmds := make([]state.CommandAvi, 0)
		// 			for cmd := range instMetadata.potentialCmdsToAddMap {
		// 				cmds = append(cmds, cmd)
		// 			}
		// 			// cmdIds := r.getAndDeleteQueue()
		// 			inst.cmds = cmds
		// 			dlog.Printf("Rotate no vals phase %v inst %v\n", r.phase, inst)
		// 			inst.status = NULL
		// 		} else {
		// 			if SANITY_CHECK && inst.status != RECEIVEDFROMOTHER {
		// 				panic("REAL: Accepted something not marked received...")
		// 			}
		// 			// todo functions
		// 			// for _, cmd := range inst.cmds {
		// 			// 	delete(instMetadata.potentialCmdsToAddMap, state.CommandId{cmd.ClientId, cmd.OpId})
		// 			// }
		// 			dlog.Printf("REAL: processRotateQuorum() adding %v to cmdQ in last else\n", instMetadata.potentialCmdsToAddMap)
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// 	if md := r.cmdMetadata[cmdId]; md != nil || (md.status != ACCEPTED && md.status != COMMITTED) {
		// 			// 		dlog.Printf("Adding %v to cmdQ in other rotate\n", cmdId)
		// 			// 		r.cmdQ[cmdId] = true
		// 			// 	}
		// 			// }
		// 		}
		// 	}
		// }
		// if SANITY_CHECK {
		// 	if inst.status != RECEIVEDFROMOTHER && inst.status != NULL && inst.status != COMMITTED {
		// 		p := fmt.Sprintf("REAL: Instance %v incorrect status %v after processing rotate\n", instMetadata.instance, inst.status)
		// 		panic(p)
		// 	}
		// }
	}

	// iterate through the metadata map and check what each instance should be.
	for _, instMetadata := range instMetadataMapMock {
		inst := r.instanceSpaceMock[instMetadata.instance]
		dlog.Printf("MOCK: Examining instance %v %v\n", instMetadata.instance, inst)

		// if we don't know about it but someone does, create it
		nextNil := r.advanceMockCrtInstanceToNextNil()
		for instMetadata.instance > nextNil {
			dlog.Printf("MOCK: mock instance %v phase %v wasn't created in processRotateQuorum when looking at instance %v creating %v\n",
				nextNil, r.phase, instMetadata.instance, nextNil)
			r.instanceSpaceMock[nextNil] = createInstance(nil, NULL, 0, false, r.phase)
			nextNil = r.advanceMockCrtInstanceToNextNil()
		}

		if inst == nil {
			dlog.Printf("REAL: Creating instance %v in Rotate phase %v\n", instMetadata.instance, r.phase)
			r.instanceSpace[instMetadata.instance] = createInstance(nil, NULL, 0, false, r.phase)
			inst = r.instanceSpace[instMetadata.instance]
			if instMetadata.status == avicennaproto.GHOSTCOMMITTED {
				r.mockCommit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastMockCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}
			if instMetadata.status == avicennaproto.GHOSTACCEPTED {
				inst.status = GHOSTOVERCOMMITTED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = true
				continue
			}
			if instMetadata.status == avicennaproto.RECEIVED {
				inst.status = RECEIVED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = true
				continue
			}
			continue
		}
		// it's possible the replica received a value from another coordinator
		// and included the instance in the message because the sender did not know it was committed
		// they will learn in the commit message I sent (or another replica sent) when I set this to committed (thanks TCP)
		// TODO should every replica send always? (maybe useful for WAN)
		if inst.status == GHOSTOVERCOMMITTED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.GHOSTCOMMITTED {
				if SANITY_CHECK && len(instMetadata.Commands) != len(inst.cmds) {
					log.Panicf("For inst %d, locally commit %d cmds, but receive commit %d cmds.\n", instMetadata.instance, inst.cmds, instMetadata.Commands)
				}
				r.bcastMockCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.GHOSTACCEPTED {
				log.Printf("Weird, log entry %d commit locally, but accepted after merging.\n", instMetadata.instance)
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				log.Printf("Weird, log entry %d commit locally, but received after merging.\n", instMetadata.instance)
			}
			// for _, cmd := range inst.cmds {
			// 	delete(instMetadata.potentialCmdsToAddMap, cmd) //state.CommandId{cmd.ClientId, cmd.OpId})
			// }
			// for cmd, _ := range instMetadata.potentialCmdsToAddMap {
			// 	r.addCmdIdsToQueueIfNotPresent([]state.CommandAvi{cmd}, nil)
			// 	// this code always executed
			// 	// if md, ok := r.cmdMetadata[cmdId]; !ok || (md.status != ACCEPTED && md.status != COMMITTED) {
			// 	// 	dlog.Printf("REAL: Adding %v to cmdQ in COMMITTED rotate\n", cmdId)
			// 	// 	r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
			// 	// 	// r.cmdQ[cmdId] = trueƒ
			// 	// }
			// }
			continue
		}
		if inst.status == MOCKACCEPTED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.GHOSTCOMMITTED {
				if SANITY_CHECK && len(instMetadata.Commands) != len(inst.cmds) {
					log.Panicf("For inst %d, locally accepted %d cmds, but receive commit %d cmds.\n", instMetadata.instance, inst.cmds, instMetadata.Commands)
				}
				r.mockCommit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastMockCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.GHOSTACCEPTED {
				continue
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				log.Printf("Weird, log entry %d accepted locally, but received after merging.\n", instMetadata.instance)
			}
			continue
		}
		if inst.status == RECEIVED {
			dlog.Printf("REAL: Instance %v was already committed\n", instMetadata.instance)
			if instMetadata.status == avicennaproto.GHOSTCOMMITTED {
				r.mockCommit(instMetadata.instance, &instMetadata.Commands, nil, false)
				r.bcastMockCommit(instMetadata.instance, instMetadata.Commands)
				continue
			}

			if instMetadata.status == avicennaproto.GHOSTACCEPTED {
				inst.status = ACCEPTED
				inst.cmds = instMetadata.Commands
				inst.accepts = 0
				inst.doGhost = false
				continue
			}

			if instMetadata.status == avicennaproto.RECEIVED {
				continue
			}
			continue
		}
		// it's possible the replica received a value from another coordinator
		// and included the instance in the message because the sender did not know it was committed
		// they will learn in the commit message I sent (or another replica sent) when I set this to committed (thanks TCP)
		// TODO should every replica send always? (maybe useful for WAN)
		// if inst.status == MOCKCOMMITTED {
		// 	dlog.Printf("MOCK: MockInstance %v was already committed\n", instMetadata.instance)
		// 	for _, cmd := range inst.cmds {
		// 		delete(instMetadata.potentialCmdsToAddMap, cmd) //state.CommandId{cmd.ClientId, cmd.OpId})
		// 	}
		// 	for cmd, _ := range instMetadata.potentialCmdsToAddMap {
		// 		r.addCmdIdsToQueueIfNotPresentMock([]state.CommandAvi{cmd}, nil)
		// 		// if md, ok := r.cmdMetadata[cmdId]; !ok || (md.status != MOCKACCEPTED && md.status != MOCKCOMMITTED) {
		// 		// 	dlog.Printf("MOCK: Adding %v to cmdQ in MOCKCOMMITTED rotate\n", cmdId)
		// 		// 	r.addCmdIdsToQueueIfNotPresentMock([]state.CommandId{cmdId}, nil)
		// 		// 	// r.cmdQ[cmdId] = true
		// 		// }
		// 	}
		// 	continue
		// }

		// // if this replica received a quorum of vals it can commit
		// if int(instMetadata.nvals) > 1 || instMetadata.committed {
		// 	// if we are committing more than one instance at a time we should batch them together?
		// 	dlog.Printf("MOCK: Rotate mock committing %v from quorum (%v) of vals phase %v was commit notification? %v\n",
		// 		instMetadata.instance, instMetadata.nvals, r.phase, instMetadata.committed)
		// 	r.mockCommit(instMetadata.instance, &instMetadata.Commands, nil, false)
		// 	r.bcastMockCommit(instMetadata.instance, instMetadata.Commands)
		// 	// r.broadcastCommitToClientChan <- InstWithNum{Instance: *inst, instNo: instMetadata.instance}
		// 	// r.bcastMockCommitToClients(instMetadata.instance)
		// } else {
		// 	// received at least one value: this replica MUST propose it
		// 	// TODO if standby but accepted a value in a previous phase,
		// 	// it does not enter this case because it doesn't send RECEIVED for
		// 	// its own value because it is the standby, thus it STAYS ACCEPTED
		// 	// in the instance when it is no longer standby
		// 	if SANITY_CHECK {
		// 		if !instMetadata.receivedOne && r.isStandby(r.Id) && inst.status == MOCKACCEPTED {
		// 			panic("MOCK: I am a standby that accepted some instance")
		// 		}
		// 	}
		// 	if instMetadata.receivedOne {
		// 		// need to propose the correct val, necessary for correctness
		// 		dlog.Printf("MOCK: Setting inst.cmds to %v for instno %v inst %v\n", instMetadata.Commands, instMetadata.instance, inst)
		// 		// if I accepted a value in this phase it better be the same set of cmds
		// 		// SANITY CHECK
		// 		if SANITY_CHECK {
		// 			if inst.status == MOCKACCEPTED {
		// 				// tmp := make(map[state.CommandId]bool)
		// 				tmp := make(map[state.CommandId]int)
		// 				total := 0
		// 				for _, cmd := range instMetadata.Commands {
		// 					tmp[state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}]++ // = true
		// 					total++
		// 				}
		// 				if total != len(inst.cmds) {
		// 					log.Panicf("Real: Rotate len mismatch \n\t inst cmds %v \n\t received cmds %v\n", inst.cmds, instMetadata.Commands)
		// 				}
		// 				for _, cmd := range inst.cmds {
		// 					cmdId := state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}
		// 					if SANITY_CHECK {
		// 						if _, ok := tmp[cmdId]; !ok {
		// 							log.Panicf("MOCK: Accepted command %v not in RECEIVED instance \n\tinst cmds %v \n\treceived commands %v\n\ttmp %v\n",
		// 								cmdId, inst.cmds, instMetadata.Commands, tmp)
		// 						} else {
		// 							tmp[cmdId]--
		// 						}
		// 					}
		// 					// TODO THIS LINE CAUSES PANIC TO FIRE WHEN THERE ARE DUPS
		// 					// delete(tmp, cmdId)
		// 				}
		// 				for _, count := range tmp {
		// 					if count != 0 {
		// 						log.Panicf("REAL: Rotate: commands mismatch: \n\t inst %v\n\t received %v\n\t tmp %v\n", inst.cmds, instMetadata.Commands, tmp)
		// 					}
		// 				}
		// 				// if len(tmp) > 0 {
		// 				// 	panic("MOCK: Accepted command does not have command in RECEIVED instance")
		// 				// }
		// 			}
		// 		}
		// 		// TODO I don't think we should add to the queue in rotate
		// 		if r.isMockCoordinatorForPhase(r.Id, r.phase+1) {
		// 			r.addToQueueIfNotPresentMock(inst.cmds, instMetadata.Commands) // r.idsFromCmds(instMetadata.Commands))
		// 			// don't add other replicas commands to my queue
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// 	if md, ok := r.cmdMetadata[cmdId]; !ok || (md.status != ACCEPTED && md.status != COMMITTED) {
		// 			// 		dlog.Printf("Adding %v to cmdQ in received from other rotate\n", cmdId)
		// 			// 		r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
		// 			// 	}
		// 			// }
		// 		}
		// 		inst.cmds = instMetadata.Commands
		// 		inst.status = RECEIVEDFROMOTHER
		// 		dlog.Printf("MOCK: Rotate setting %v but no quorum of vals phase %v\n", instMetadata.instance, r.phase)
		// 		// any cmdId leftover in the map should be added to the cmdQ to be accepted later
		// 		dlog.Printf("MOCK: processRotateQuorum() adding %v to cmdQ\n", instMetadata.potentialCmdsToAddMap)
		// 		// TODO this can be function call

		// 	} else { // received no values in this phase
		// 		// TODO I think I should remove this
		// 		if inst.status != RECEIVEDFROMOTHER && inst.status != MOCKACCEPTED {
		// 			// TODO don't do this?
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// dlog.Printf("Adding %v to cmdQ\n", cmdId)
		// 			// r.addCmdIdsToQueueIfNotPresent([]state.CommandId{cmdId}, nil)
		// 			// r.cmdQ[cmdId] = true
		// 			// }
		// 			cmdIds := make([]state.CommandAvi, 0)
		// 			for cmdId := range instMetadata.potentialCmdsToAddMap {
		// 				cmdIds = append(cmdIds, cmdId)
		// 			}
		// 			// cmdIds := r.getAndDeleteQueue()
		// 			inst.cmds = cmdIds
		// 			dlog.Printf("MOCK: Rotate no vals phase %v inst %v\n", r.phase, inst)
		// 			inst.status = NULL
		// 		} else {
		// 			if SANITY_CHECK && inst.status != RECEIVEDFROMOTHER {
		// 				panic("MOCK: Accepted something not marked received...")
		// 			}
		// 			// todo functions
		// 			// for _, cmd := range inst.cmds {
		// 			// 	delete(instMetadata.potentialCmdsToAddMap, state.CommandId{cmd.ClientId, cmd.OpId})
		// 			// }
		// 			dlog.Printf("MOCK: processRotateQuorum() adding %v to cmdQ in last else\n", instMetadata.potentialCmdsToAddMap)
		// 			// for cmdId, _ := range instMetadata.potentialCmdsToAddMap {
		// 			// 	if md := r.cmdMetadata[cmdId]; md != nil || (md.status != ACCEPTED && md.status != COMMITTED) {
		// 			// 		dlog.Printf("Adding %v to cmdQ in other rotate\n", cmdId)
		// 			// 		r.cmdQ[cmdId] = true
		// 			// 	}
		// 			// }
		// 		}
		// 	}
		// }
		// if SANITY_CHECK {
		// 	if inst.status != RECEIVEDFROMOTHER && inst.status != NULL && inst.status != MOCKCOMMITTED {
		// 		p := fmt.Sprintf("MOCK: Instance %v incorrect status %v after processing rotate\n", instMetadata.instance, inst.status)
		// 		panic(p)
		// 	}
		// }
	}
}

func (r *Replica) handleRotate(rotate *avicennaproto.Rotate) {
	dlog.Printf("phase %v got rotate message\n", rotate.Phase)
	if r.phase == rotate.Phase {
		log.Printf("Receive rotate message from replica %d\n", rotate.ReplicaId)
		if !r.sentForPhase {
			r.bcastRotate()
		}

		if r.isStandby(rotate.ReplicaId) {
			return
		}
		// // Added because of a complex case
		// // TODO add description
		// // cch: I think this is only because of the old detector that could be wrong
		// // 		now we rotate if even one replica thinks we should.
		// if int(r.nRotateMessages+1) >= r.N>>1 && !r.sentForPhase {
		// 	dlog.Printf("phase %v received %v N>>1 %v rotate messages bcast\n", r.phase, r.nRotateMessages, r.N>>1)
		// 	r.bcastRotate()
		// } else {
		// 	dlog.Printf("phase %v received %v N>>1 %v rotate messages not bcasting\n", r.phase, r.nRotateMessages, r.N>>1)
		// }

		// REAL
		if len(rotate.Instances) > 0 {
			instCommands := rotate.Instances[0]
			// sending replica knows about commits I'm behind on
			if instCommands.Instance > r.committedUpTo+1 {
				dlog.Printf("Received a Rotate message that had"+
					"commits I am missing committedUpTo: %v first in received %v\n",
					r.committedUpTo, instCommands.Instance)
			} else if instCommands.Instance <= r.committedUpTo {
				dlog.Printf("Rotate is behind on committed instances "+
					"have %v got %v\n", r.committedUpTo, instCommands.Instance)
			}
		}

		// MOCK
		if len(rotate.MockInstances) > 0 {
			instCommands := rotate.MockInstances[0]
			// sending replica knows about commits I'm behind on
			if instCommands.Instance > r.mockCommittedUpTo+1 {
				dlog.Printf("Received a Rotate message that had"+
					"mock commits I am missing mockCommittedUpTo: %v first in received %v\n",
					r.committedUpTo, instCommands.Instance)
			} else if instCommands.Instance <= r.mockCommittedUpTo {
				dlog.Printf("Rotate is behind on mock committed instances "+
					"have %v got %v\n", r.mockCommittedUpTo, instCommands.Instance)
			}
		}

		if r.trackRotate(rotate) {
			// clear the TwoHeaps because I might not be the one to have detected it
			// this should be a function
			r.objectiveFuncMD.(*TwoHeaps).clear()

			log.Printf("Rotate got quorum phase %v recMsgs %v going to move to phase %v\n", r.phase, r.nRotateMessages, r.phase+1)
			dlog.Printf("Current configuration %v next configuration %v\n", r.curConfiguration(), r.configurations[(r.phase+1)%int32(len(r.configurations))])
			// I took part in my own quorum but I might not have sent it yet if they both sent it to me

			r.processRotateQuorum() // processes both REAL and MOCK
			r.goToPhase(r.phase + 1)

			if r.isCoordinator(r.Id) {
				dlog.Printf("I just became the coordinator in phase %v\n", r.phase)
				// start the next phase
				// TODO make this send everything we have to
				// log.Printf("Calling sync() in handleRotate()\n")
				r.sync()
				r.bcastLongAccept()
				// r.bcastAccept(-1, nil) // TODO changed from bcastAccept
				// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
			}
			if r.isCurMockCoordinator() {
				dlog.Printf("I just became the mock coordinator in phase %v\n", r.phase)
				// start the next phase
				// TODO make this send everything we have to
				log.Printf("Calling sync() in handleRotate()\n")
				r.sync()
				r.bcastLongMockAccept() // TODO
				// r.bcastAccept(-1, nil) // TODO changed from bcastAccept
				// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
			}
		} else {
			return
		}
	} else if rotate.Phase < r.phase {
		// in the past, ignore (reliable message delivery will eventually update the other replica)
		dlog.Printf("Received a Rotate with phase in the past %v have %v\n", rotate.Phase, r.phase)
	} else {
		// we are behind.
		// null everything and send a ReceivedMessage for received.phase?
		// or can I send the same received message!?
		//   todo this would have a higher chance of committing
		//   todo actually if receiving it here triggers me sending it should I just send the same vals/null?
		// panic("Not handling a higher phased Rotate just yet")
		if len(rotate.Instances) > 0 {
			if rotate.Instances[0].Instance <= r.committedUpTo {
				r.goToPhase(rotate.Phase)
				if r.trackRotate(rotate) {
					dlog.Printf("HIGHER NUMBERED PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
					r.processRotateQuorum()
					// TODO I think in this case this needs the following code:
					// r.goToPhase(r.phase + 1)

					// if r.isCoordinator(r.Id) {
					// 	dlog.Printf("I just became the coordinator in phase %v\n", r.phase)
					// 	// start the next phase
					// 	// TODO make this send everything we have to
					// 	r.bcastAccept(-1, nil) // TODO changed from bcastAccept
					// 	// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
					// }
				}
			} else {
				// there are commits that I am missing
				// panic("TODO received a Rotate for a replica" +
				// 	" in a later phase whose first instance is later than my committedUpTO\n")
				dlog.Printf("MISSING COMMITS IN ROTATE FOR HIGHER PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
				r.rotateChan <- genericsmr.SerializableWithRecvTime{rotate, time.Time{}}
			}
		} else if len(rotate.MockInstances) > 0 { // cch todo added this for mock, not sure about this, unsure what the checks are meant to do
			if rotate.MockInstances[0].Instance <= r.mockCommittedUpTo {
				r.goToPhase(rotate.Phase)
				if r.trackRotate(rotate) {
					dlog.Printf("HIGHER NUMBERED PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
					r.processRotateQuorum()
					// TODO I think in this case this needs the following code:
					// r.goToPhase(r.phase + 1)

					// if r.isCoordinator(r.Id) {
					// 	dlog.Printf("I just became the coordinator in phase %v\n", r.phase)
					// 	// start the next phase
					// 	// TODO make this send everything we have to
					// 	r.bcastAccept(-1, nil) // TODO changed from bcastAccept
					// 	// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
					// }
				}
			} else {
				// there are commits that I am missing
				// panic("TODO received a Rotate for a replica" +
				// 	" in a later phase whose first instance is later than my committedUpTO\n")
				log.Printf("MISSING COMMITS IN ROTATE FOR HIGHER PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
				r.rotateChan <- genericsmr.SerializableWithRecvTime{rotate, time.Time{}}
			}
		} else { // TODO there is a bug here where if it
			//   received a 0 len high number rotate it doesn't do anything and livelocks
			//   and some issue with network
			r.rotateChan <- genericsmr.SerializableWithRecvTime{rotate, time.Time{}}
		}

	}
}

func (r *Replica) handleRotateOLD(rotate *avicennaproto.Rotate) {
	dlog.Printf("phase %v got rotate message\n", rotate.Phase)
	if r.phase == rotate.Phase {
		if !r.sentForPhase {
			r.bcastRotate()
		}
		// // Added because of a complex case
		// // TODO add description
		// // cch: I think this is only because of the old detector that could be wrong
		// // 		now we rotate if even one replica thinks we should.
		// if int(r.nRotateMessages+1) >= r.N>>1 && !r.sentForPhase {
		// 	dlog.Printf("phase %v received %v N>>1 %v rotate messages bcast\n", r.phase, r.nRotateMessages, r.N>>1)
		// 	r.bcastRotate()
		// } else {
		// 	dlog.Printf("phase %v received %v N>>1 %v rotate messages not bcasting\n", r.phase, r.nRotateMessages, r.N>>1)
		// }

		if len(rotate.Instances) > 0 {
			instCommands := rotate.Instances[0]
			// sending replica knows about commits I'm behind on
			if instCommands.Instance > r.committedUpTo+1 {
				dlog.Printf("Received a Rotate message that had"+
					"commits I am missing committedUpTo: %v first in received %v\n",
					r.committedUpTo, instCommands.Instance)
			} else if instCommands.Instance <= r.committedUpTo {
				dlog.Printf("Rotate is behind on committed instances "+
					"have %v got %v\n", r.committedUpTo, instCommands.Instance)
			}
		}

		if r.trackRotate(rotate) {
			dlog.Printf("Rotate got quorum phase %v recMsgs %v going to move to phase %v\n", r.phase, r.nRotateMessages, r.phase+1)
			dlog.Printf("Current configuration %v next configuration %v\n", r.curConfiguration(), r.configurations[(r.phase+1)%int32(len(r.configurations))])
			// I took part in my own quorum but I might not have sent it yet if they both sent it to me

			r.processRotateQuorum()
			r.goToPhase(r.phase + 1)

			if r.isCoordinator(r.Id) {
				dlog.Printf("I just became the coordinator in phase %v\n", r.phase)
				// start the next phase
				// TODO make this send everything we have to
				r.bcastLongAccept()
				// r.bcastAccept(-1, nil) // TODO changed from bcastAccept
				// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
			}
		} else {
			return
		}
	} else if rotate.Phase < r.phase {
		// in the past, ignore (reliable message delivery will eventually update the other replica)
		dlog.Printf("Received a Rotate with phase in the past %v have %v\n", rotate.Phase, r.phase)
	} else {
		// we are behind.
		// null everything and send a ReceivedMessage for received.phase?
		// or can I send the same received message!?
		//   todo this would have a higher chance of committing
		//   todo actually if receiving it here triggers me sending it should I just send the same vals/null?
		// panic("Not handling a higher phased Rotate just yet")
		if len(rotate.Instances) > 0 {
			if rotate.Instances[0].Instance <= r.committedUpTo {
				r.goToPhase(rotate.Phase)
				if r.trackRotate(rotate) {
					dlog.Printf("HIGHER NUMBERED PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
					r.processRotateQuorum()
					// TODO I think in this case this needs the following code:
					// r.goToPhase(r.phase + 1)

					// if r.isCoordinator(r.Id) {
					// 	dlog.Printf("I just became the coordinator in phase %v\n", r.phase)
					// 	// start the next phase
					// 	// TODO make this send everything we have to
					// 	r.bcastAccept(-1, nil) // TODO changed from bcastAccept
					// 	// r.bcastAccept(r.crtInstance-1, r.idsFromCmds(r.instanceSpace[r.crtInstance-1].cmds)) // TODO changed from bcastAccept
					// }
				}
			} else {
				// there are commits that I am missing
				// panic("TODO received a Rotate for a replica" +
				// 	" in a later phase whose first instance is later than my committedUpTO\n")
				dlog.Printf("MISSING COMMITS IN ROTATE FOR HIGHER PHASE Rotate got quorum phase %v recMsgs %v\n", r.phase, r.nRotateMessages)
				r.rotateChan <- genericsmr.SerializableWithRecvTime{rotate, time.Time{}}
			}
		} else { // TODO there is a bug here where if it
			//   received a 0 len high number rotate it doesn't do anything and livelocks
			//   and some issue with network

			r.rotateChan <- genericsmr.SerializableWithRecvTime{rotate, time.Time{}}
		}

	}
}

func printCommands(cmds *[]state.Command) {
	for _, cmd := range *cmds {
		dlog.Printf("\t%v\t%v\n", cmd.ClientId, cmd.OpId)
	}
}

func (r *Replica) executeCommandsMock() {
	// slowdownTimers := &slowdowntimers.SlowdownTimers{}

	i := int32(0)
	// if r.IsSlowdownReplica {
	// 	if INJECT_TRANSIENT_SLOWDOWN {
	// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	} else if INJECT_LONGLIVED_SLOWDOWN {
	// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	}
	// }
	for !r.Shutdown {
		// if receivedAPropose {//&& !slowdownTimers.Initialized {
		// 	slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
		// }
		executed := false

		if r.IsSlowdownReplica {
			// if INJECT_TRANSIENT_SLOWDOWN {
			// 	slowdownTimers.CheckAndDoSlowdown()
			// }
			// else if INJECT_LONGLIVED_SLOWDOWN {
			// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
			// }
		}

		for i <= r.mockCommittedUpTo {
			if r.instanceSpaceMock[i].cmds != nil {
				inst := r.instanceSpaceMock[i]

				dlog.Printf("About to execute instance %v with %v cmds\n",
					i, len(inst.cmds))

				if SANITY_CHECK {
					if len(inst.cmds) == 0 {
						// log.Printf("0 Commands in execution instance %v\n", i)
						// panic("0 cmds in execution")
					}
				}
				// each instance has a batch of cmds to exec
				for j := 0; j < len(inst.cmds); j++ {
					cmd := inst.cmds[j]
					cmdExecMapKey := getKeyForExecMap(cmd.Cmd.ClientId, cmd.Cmd.OpId)
					if r.execMapMock[cmdExecMapKey] {
						dlog.Printf("This replica is executing the same request twice! %v %v\n", cmd.Cmd.ClientId, cmd.Cmd.OpId)
						// let's not count dups for Mock executions
						// dups++
						continue
					}
					r.execMapMock[cmdExecMapKey] = true
					// Mock execution doesn't execute
					// val := cmd.Execute(r.State)

					// WHEN REPLYING TO CLIENTS
					// val := state.Value(0)

					// TODO for correctness tests atm
					// r.recordCommands(inst.cmds)

					// we don't MockReply anymore
					// if true || r.isCurMockCoordinator() || r.isCoordinator(r.Id) {
					// 	if writer, ok := r.clientWriters[cmd.ClientId]; ok {
					// 		propreply := &genericsmrproto.ProposeReplyTS{
					// 			OK:        avicennaproto.CLIENTMOCKREPLY,
					// 			CommandId: cmd.OpId,
					// 			Value:     val,
					// 			Timestamp: int64(i)}

					// 		dlog.Printf("MockReplying to a client for %v %v in instance %v\n", cmd.ClientId, cmd.OpId, i)

					// 		if err := r.ReplyProposeTS(propreply, writer); err != nil {
					// 			pstring := fmt.Sprintf("Error replying to client: %v", err)
					// 			panic(pstring)
					// 		}
					// 	} else {
					// 		dlog.Printf("Replica has no writer to client %v\n", cmd.ClientId)
					// 		panic("No client writer")
					// 	}
					// }
				}
				i++
				dlog.Printf("Execution reached and is now waiting for %v\n", i)
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

func (r *Replica) executeCommands() {
	// slowdownTimers := &slowdowntimers.SlowdownTimers{}

	i := int32(0)
	// if r.IsSlowdownReplica {
	// 	if INJECT_TRANSIENT_SLOWDOWN {
	// 		// slowdownTimers.InitializeTimers(r.Id, r.TimesToSlowdown)
	// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	} else if INJECT_LONGLIVED_SLOWDOWN {
	// 		slowdownTimers.InitializeTimers(r.Id, r.TimeToSlowdown, r.SlowdownDuration)
	// 	}
	// }
	for !r.Shutdown {
		// if receivedAPropose && !slowdownTimers.initialized {
		// 	slowdownTimers.initializeTimers()
		// }
		executed := false
		// if r.IsSlowdownReplica {
		// 	// if INJECT_TRANSIENT_SLOWDOWN {
		// 	// 	slowdownTimers.CheckAndDoSlowdown()
		// 	// }
		// 	//  else
		// 	//  if INJECT_LONGLIVED_SLOWDOWN {
		// 	// 	slowdownTimers.CheckAndDoLongLivedSlowdown()
		// 	// }
		// }

		for i <= r.committedUpTo {
			if r.instanceSpace[i].cmds != nil {
				inst := r.instanceSpace[i]

				// if SANITY_CHECK {
				// 	// if len(inst.cmds) == 0 {
				// 	// 	// log.Printf("0 Commands in execution instance %v\n", i)
				// 	// 	// panic("0 cmds in execution")
				// 	// }
				// }
				// each instance has a batch of cmds to exec
				for j := 0; j < len(inst.cmds); j++ {
					cmd := inst.cmds[j]
					cmdExecMapKey := getKeyForExecMap(cmd.Cmd.ClientId, cmd.Cmd.OpId)
					if r.execMap[cmdExecMapKey] {
						dlog.Printf("This replica is executing the same request twice! %v %v\n", cmd.Cmd.ClientId, cmd.Cmd.OpId)
						dups++
						continue
					}
					r.execMap[cmdExecMapKey] = true
					// before := time.Now()
					val := cmd.Cmd.Execute(r.State)

					// r.sync() // used for debugging and testing only !!!

					executeTime := time.Since(inst.commitTime) // likely 0...
					executeTimeReply := int64(-1)

					if r.isCurMockCoordinator() && cmd.DoMock {
						mockExecTimeNew := genericsmrproto.MockExecTime_{ExecTime: int64(executeTime / time.Microsecond), DoMock: true, CommandId: state.CommandId{ClientId: cmd.Cmd.ClientId, OpId: cmd.Cmd.OpId}}
						executeTimeReply = int64(executeTime) // only the ghost leader records execution time
						r.mockExecTime <- mockExecTimeNew
						if mockExecTimeNew.ExecTime < 0 {
							log.Printf("Weird, the ghost execution is less than 0! %v, %d\n", executeTime, mockExecTimeNew.ExecTime)
						}
						// log.Printf("I'm ghost leader, execution time for command {ClientId %d, CommandId %d} is %v\n", cmd.ClientId, cmd.OpId, executeTime)
					}

					// if r.isCoordinator(r.Id) {
					// 	// log.Printf("I'm real leader, execution time for command {ClientId %d, CommandId %d} is %v\n", cmd.ClientId, cmd.OpId, executeTime)
					// }

					// TODO for correctness tests atm
					// if i < 100000 {
					// 	r.recordCommands(inst.cmds)
					// }

					if true {
						if writer, ok := r.clientWriters[cmd.Cmd.ClientId]; ok {
							propreply := &genericsmrproto.ProposeReplyTSMock{
								OK:           TRUE,
								CommandId:    cmd.Cmd.OpId,
								Value:        val,
								Timestamp:    int64(executeTimeReply),
								MockInstruct: bool(r.surgMock)}
							// Timestamp: int64(i)}
							if err := r.ReplyProposeTSMock(cmd.Cmd.ClientId, propreply, writer); err != nil {
								log.Panicf("Error replying to client: %v", err)
								// pstring := fmt.Sprintf("Error replying to client: %v", err)
								// panic(pstring)
							}
							r.stats.nMsgsSent++
						} else {
							dlog.Printf("Replica has no writer to client %v\n", cmd.Cmd.ClientId)
							panic("No client writer")
						}
					}
				}
				i++
				dlog.Printf("Execution reached and is now waiting for %v\n", i)
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
