package avicennaproto

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"genericsmrproto"
	"io"
	"state"
	"sync"
)

func (t *Accept) New() fastrpc.Serializable {
	return new(Accept)
}

func (t *AcceptExecTime) New() fastrpc.Serializable {
	return new(AcceptExecTime)
}

func (t *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}

func (t *Rotate) New() fastrpc.Serializable {
	return new(Rotate)
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}

func (t *CommitShort) New() fastrpc.Serializable {
	return new(CommitShort)
}
func (t *InstanceCommands) New() fastrpc.Serializable {
	return new(InstanceCommands)
}

func (t *Ping) New() fastrpc.Serializable {
	return new(Ping)
}

func (t *PingReply) New() fastrpc.Serializable {
	return new(PingReply)
}

func (t *RttTable) New() fastrpc.Serializable {
	return new(RttTable)
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *ClientLatency) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ClientLatencyCache struct {
	mu    sync.Mutex
	cache []*ClientLatency
}

func NewClientLatencyCache() *ClientLatencyCache {
	c := &ClientLatencyCache{}
	c.cache = make([]*ClientLatency, 0)
	return c
}

func (p *ClientLatencyCache) Get() *ClientLatency {
	var t *ClientLatency
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ClientLatency{}
	}
	return t
}
func (p *ClientLatencyCache) Put(t *ClientLatency) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ClientLatency) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	t.CmdId.Marshal(wire)
	bs = b[:8]
	tmp64 := t.Latency
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *ClientLatency) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	t.CmdId.Unmarshal(wire)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Latency = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *InstanceCommands) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type InstanceCommandsCache struct {
	mu    sync.Mutex
	cache []*InstanceCommands
}

func NewInstanceCommandsCache() *InstanceCommandsCache {
	c := &InstanceCommandsCache{}
	c.cache = make([]*InstanceCommands, 0)
	return c
}

func (p *InstanceCommandsCache) Get() *InstanceCommands {
	var t *InstanceCommands
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &InstanceCommands{}
	}
	return t
}
func (p *InstanceCommandsCache) Put(t *InstanceCommands) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *InstanceCommands) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:5]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.Status)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Commands))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Marshal(wire)
	}

	// Phase (int32, little-endian)  <-- NEW
	bs = b[:4]
	tmp32 = t.Phase
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	bs = b[:1]
	bs[0] = byte(t.DoGhost)
	wire.Write(bs)
}

func (t *InstanceCommands) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Status = uint8(bs[4])
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Commands = make([]state.CommandAvi, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Unmarshal(wire)
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.Phase = int32(uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24))
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.DoGhost = uint8(bs[0])
	return nil
}

func (t *PingReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 4, true
}

type PingReplyCache struct {
	mu    sync.Mutex
	cache []*PingReply
}

func NewPingReplyCache() *PingReplyCache {
	c := &PingReplyCache{}
	c.cache = make([]*PingReply, 0)
	return c
}

func (p *PingReplyCache) Get() *PingReply {
	var t *PingReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PingReply{}
	}
	return t
}
func (p *PingReplyCache) Put(t *PingReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PingReply) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *PingReply) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	return nil
}

func (t *RttTable) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type RttTableCache struct {
	mu    sync.Mutex
	cache []*RttTable
}

func NewRttTableCache() *RttTableCache {
	c := &RttTableCache{}
	c.cache = make([]*RttTable, 0)
	return c
}

func (p *RttTableCache) Get() *RttTable {
	var t *RttTable
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RttTable{}
	}
	return t
}
func (p *RttTableCache) Put(t *RttTable) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *RttTable) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:]
	alen1 := int64(len(t.RttTable))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:8]
		tmp64 := t.RttTable[i]
		bs[0] = byte(tmp64)
		bs[1] = byte(tmp64 >> 8)
		bs[2] = byte(tmp64 >> 16)
		bs[3] = byte(tmp64 >> 24)
		bs[4] = byte(tmp64 >> 32)
		bs[5] = byte(tmp64 >> 40)
		bs[6] = byte(tmp64 >> 48)
		bs[7] = byte(tmp64 >> 56)
		wire.Write(bs)
	}
}

func (t *RttTable) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.RttTable = make([]int64, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:8]
		if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
			return err
		}
		t.RttTable[i] = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	}
	return nil
}

func (t *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type AcceptReplyCache struct {
	mu    sync.Mutex
	cache []*AcceptReply
}

func NewAcceptReplyCache() *AcceptReplyCache {
	c := &AcceptReplyCache{}
	c.cache = make([]*AcceptReply, 0)
	return c
}

func (p *AcceptReplyCache) Get() *AcceptReply {
	var t *AcceptReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptReply{}
	}
	return t
}
func (p *AcceptReplyCache) Put(t *AcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptReply) Marshal(wire io.Writer) {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.Phase
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	bs[8] = byte(t.OK)
	wire.Write(bs)
}

func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Phase = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OK = uint8(bs[8])
	return nil
}

func (t *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitCache struct {
	mu    sync.Mutex
	cache []*Commit
}

func NewCommitCache() *CommitCache {
	c := &CommitCache{}
	c.cache = make([]*Commit, 0)
	return c
}

func (p *CommitCache) Get() *Commit {
	var t *Commit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Commit{}
	}
	return t
}
func (p *CommitCache) Put(t *Commit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Commit) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Phase
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	bs[8] = byte(t.Mock)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Instances))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Marshal(wire)
	}
	bs = b[:4]
	tmp32 = t.GlobalMaxCommit
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *Commit) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Phase = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Mock = uint8(bs[8])
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Instances = make([]InstanceCommands, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Unmarshal(wire)
	}
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.GlobalMaxCommit = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	return nil
}

func (t *ClientRttTable) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ClientRttTableCache struct {
	mu    sync.Mutex
	cache []*ClientRttTable
}

func NewClientRttTableCache() *ClientRttTableCache {
	c := &ClientRttTableCache{}
	c.cache = make([]*ClientRttTable, 0)
	return c
}

func (p *ClientRttTableCache) Get() *ClientRttTable {
	var t *ClientRttTable
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ClientRttTable{}
	}
	return t
}
func (p *ClientRttTableCache) Put(t *ClientRttTable) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ClientRttTable) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Rtts))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:8]
		tmp64 := t.Rtts[i]
		bs[0] = byte(tmp64)
		bs[1] = byte(tmp64 >> 8)
		bs[2] = byte(tmp64 >> 16)
		bs[3] = byte(tmp64 >> 24)
		bs[4] = byte(tmp64 >> 32)
		bs[5] = byte(tmp64 >> 40)
		bs[6] = byte(tmp64 >> 48)
		bs[7] = byte(tmp64 >> 56)
		wire.Write(bs)
	}
}

func (t *ClientRttTable) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Rtts = make([]int64, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:8]
		if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
			return err
		}
		t.Rtts[i] = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	}
	return nil
}

func (t *InstanceCommandIdsOLD) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type InstanceCommandIdsOLDCache struct {
	mu    sync.Mutex
	cache []*InstanceCommandIdsOLD
}

func NewInstanceCommandIdsOLDCache() *InstanceCommandIdsOLDCache {
	c := &InstanceCommandIdsOLDCache{}
	c.cache = make([]*InstanceCommandIdsOLD, 0)
	return c
}

func (p *InstanceCommandIdsOLDCache) Get() *InstanceCommandIdsOLD {
	var t *InstanceCommandIdsOLD
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &InstanceCommandIdsOLD{}
	}
	return t
}
func (p *InstanceCommandIdsOLDCache) Put(t *InstanceCommandIdsOLD) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *InstanceCommandIdsOLD) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:5]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.Status)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.CommandIds))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.CommandIds[i].Marshal(wire)
	}
	bs = b[:1]
	bs[0] = byte(t.DoMock)
	wire.Write(bs)
}

func (t *InstanceCommandIdsOLD) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Status = uint8(bs[4])
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.CommandIds = make([]state.CommandId, alen1)
	for i := int64(0); i < alen1; i++ {
		t.CommandIds[i].Unmarshal(wire)
	}
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.DoMock = uint8(bs[0])
	return nil
}

func (t *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptCache struct {
	mu    sync.Mutex
	cache []*Accept
}

func NewAcceptCache() *AcceptCache {
	c := &AcceptCache{}
	c.cache = make([]*Accept, 0)
	return c
}

func (p *AcceptCache) Get() *Accept {
	var t *Accept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Accept{}
	}
	return t
}
func (p *AcceptCache) Put(t *Accept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Accept) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Phase
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Instances))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Marshal(wire)
	}
}

func (t *Accept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Phase = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Instances = make([]InstanceCommands, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Unmarshal(wire)
	}
	return nil
}

func (t *AcceptExecTime) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptExecTimeCache struct {
	mu    sync.Mutex
	cache []*AcceptExecTime
}

func NewAcceptExecTimeCache() *AcceptExecTimeCache {
	c := &AcceptExecTimeCache{}
	c.cache = make([]*AcceptExecTime, 0)
	return c
}

func (p *AcceptExecTimeCache) Get() *AcceptExecTime {
	var t *AcceptExecTime
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptExecTime{}
	}
	return t
}
func (p *AcceptExecTimeCache) Put(t *AcceptExecTime) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *AcceptExecTime) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte

	// marshal ReplicaId, int32
	bs = b[:8]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)

	// marshal Phase, int32
	tmp32 = t.Phase
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)

	// marshal Instances, []InstanceCommands
	bs = b[:]
	alen1 := int64(len(t.Instances))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Marshal(wire)
	}

	// marshal MockExecTimes, []genericsmrproto.MockExecTime_
	length := int32(len(t.MockExecTimes)) // the length of MockExecTimes[], used for unmarshal
	bs = b[:4]
	bs[0] = byte(length)
	bs[1] = byte(length >> 8)
	bs[2] = byte(length >> 16)
	bs[3] = byte(length >> 24)
	wire.Write(bs)

	for _, tmpExecTime := range t.MockExecTimes {
		tmpExecTime.Marshal(wire)
	}
}

func (t *AcceptExecTime) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte

	// unmarshal ReplicaId, int32
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	// unmarshal Phase, int32
	t.Phase = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))

	// unmarshal Instances, []InstanceCommands
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Instances = make([]InstanceCommands, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Unmarshal(wire)
	}

	// unmarshal MockExecTimes, []genericsmrproto.MockExecTime_
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	length := int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	t.MockExecTimes = make([]genericsmrproto.MockExecTime_, length)
	for i := int32(0); i < length; i++ {
		t.MockExecTimes[i].Unmarshal(wire)
	}
	return nil
}

func (t *Ping) BinarySize() (nbytes int, sizeKnown bool) {
	return 4, true
}

type PingCache struct {
	mu    sync.Mutex
	cache []*Ping
}

func NewPingCache() *PingCache {
	c := &PingCache{}
	c.cache = make([]*Ping, 0)
	return c
}

func (p *PingCache) Get() *Ping {
	var t *Ping
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Ping{}
	}
	return t
}
func (p *PingCache) Put(t *Ping) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Ping) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *Ping) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	return nil
}

func (t *Rotate) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type RotateCache struct {
	mu    sync.Mutex
	cache []*Rotate
}

func NewRotateCache() *RotateCache {
	c := &RotateCache{}
	c.cache = make([]*Rotate, 0)
	return c
}

func (p *RotateCache) Get() *Rotate {
	var t *Rotate
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Rotate{}
	}
	return t
}
func (p *RotateCache) Put(t *Rotate) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Rotate) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Phase
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Instances))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.MockInstances))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.MockInstances[i].Marshal(wire)
	}
}

func (t *Rotate) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Phase = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Instances = make([]InstanceCommands, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Instances[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.MockInstances = make([]InstanceCommands, alen2)
	for i := int64(0); i < alen2; i++ {
		t.MockInstances[i].Unmarshal(wire)
	}
	return nil
}

func (t *CommitShort) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

type CommitShortCache struct {
	mu    sync.Mutex
	cache []*CommitShort
}

func NewCommitShortCache() *CommitShortCache {
	c := &CommitShortCache{}
	c.cache = make([]*CommitShort, 0)
	return c
}

func (p *CommitShortCache) Get() *CommitShort {
	var t *CommitShort
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommitShort{}
	}
	return t
}
func (p *CommitShortCache) Put(t *CommitShort) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommitShort) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.Id
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Count
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *CommitShort) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Id = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Count = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	return nil
}
