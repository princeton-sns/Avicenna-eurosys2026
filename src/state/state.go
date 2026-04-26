package state

import (
	"sync"
	//"fmt"
	//"code.google.com/p/leveldb-go/leveldb"
	//"encoding/binary"
)

type Operation uint8

//type OperationId int32

const (
	NONE Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
)

type Value int64

const NIL Value = 0

const Size = 2

var Buf [Size]byte

type Key int64

type Command struct {
	ClientId uint32
	OpId     int32
	Op       Operation
	K        Key
	V        Value
}

type CommandAvi struct {
	Cmd    Command
	DoMock bool
}

type CommandId struct {
	ClientId uint32
	OpId     int32
}

type State struct {
	mutex *sync.Mutex
	Store map[Key]Value
	//DB *leveldb.DB
}

func InitState() *State {
	/*
	   d, err := leveldb.Open("/Users/iulian/git/epaxos-batching/dpaxos/bin/db", nil)

	   if err != nil {
	       fmt.Printf("Leveldb open failed: %v\n", err)
	   }

	   return &State{d}
	*/

	//return &State{new(sync.Mutex), make(map[Key]Value)}
	return &State{new(sync.Mutex), make(map[Key]Value, 100000)}
	//return &State{new(sync.Mutex), make(map[Key]Value, 10000000)}
	//return &State{new(sync.Mutex), make(map[Key]Value, 100000000)}
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(command *Command) bool {
	return command.Op == GET
}

func (c *Command) Execute(st *State) Value {
	//fmt.Printf("Executing (%d, %d)\n", c.K, c.V)

	//var key, value [8]byte
	// size := 1 * 1024 // 1kb
	// size := 10 * 1024 // 10kb
	// size := 100 * 1024 // 100KB
	// size := 1 * 1024 * 1024 // 1mb
	// size := 10 * 1024 * 1024 // 10mb
	// Buf := make([]byte, size)
	// Buf[0] = 0
	//    st.mutex.Lock()
	//    defer st.mutex.Unlock()

	switch c.Op {
	case PUT:
		/*
		   binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		   binary.LittleEndian.PutUint64(value[:], uint64(c.V))
		   st.DB.Set(key[:], value[:], nil)
		*/

		// st.Store[c.K] = c.V
		// Buf := make([]byte, size)
		Buf[0] = 0
		// log.Printf("Executing the write.\n")
		// for i := 0; i < len(Buf); i++ {
		// 	Buf[i] = byte(i)
		// }
		// buf[0] = 0
		// two := []byte{0xAB, 0xCD}
		// dlog.Println(two)

		// sixteen := []byte{
		// 	0x00, 0x01, 0x02, 0x03,
		// 	0x04, 0x05, 0x06, 0x07,
		// 	0x08, 0x09, 0x0A, 0x0B,
		// 	0x0C, 0x0D, 0x0E, 0x0F,
		// }
		// dlog.Println(sixteen)
		return c.V

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}
	}

	return NIL
}
