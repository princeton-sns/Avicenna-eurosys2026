package genericsmrproto

import (
	"bufio"
	"encoding/binary"
	"io"
	"state"
	"sync"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *ReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ReadReplyCache struct {
	mu    sync.Mutex
	cache []*ReadReply
}

func NewReadReplyCache() *ReadReplyCache {
	c := &ReadReplyCache{}
	c.cache = make([]*ReadReply, 0)
	return c
}

func (p *ReadReplyCache) Get() *ReadReply {
	var t *ReadReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ReadReply{}
	}
	return t
}
func (p *ReadReplyCache) Put(t *ReadReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ReadReply) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ReadReply) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *ProposeAndRead) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeAndReadCache struct {
	mu    sync.Mutex
	cache []*ProposeAndRead
}

func NewProposeAndReadCache() *ProposeAndReadCache {
	c := &ProposeAndReadCache{}
	c.cache = make([]*ProposeAndRead, 0)
	return c
}

func (p *ProposeAndReadCache) Get() *ProposeAndRead {
	var t *ProposeAndRead
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeAndRead{}
	}
	return t
}
func (p *ProposeAndReadCache) Put(t *ProposeAndRead) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeAndRead) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	t.Key.Marshal(wire)
}

func (t *ProposeAndRead) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Command.Unmarshal(wire)
	t.Key.Unmarshal(wire)
	return nil
}

func (t *Propose) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeCache struct {
	mu    sync.Mutex
	cache []*Propose
}

func NewProposeCache() *ProposeCache {
	c := &ProposeCache{}
	c.cache = make([]*Propose, 0)
	return c
}

func (p *ProposeCache) Get() *Propose {
	var t *Propose
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Propose{}
	}
	return t
}
func (p *ProposeCache) Put(t *Propose) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Propose) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *Propose) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Command.Unmarshal(wire)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *ProposeReplyTS) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeReplyTSCache struct {
	mu    sync.Mutex
	cache []*ProposeReplyTS
}

func NewProposeReplyTSCache() *ProposeReplyTSCache {
	c := &ProposeReplyTSCache{}
	c.cache = make([]*ProposeReplyTS, 0)
	return c
}

func (p *ProposeReplyTSCache) Get() *ProposeReplyTS {
	var t *ProposeReplyTS
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeReplyTS{}
	}
	return t
}
func (p *ProposeReplyTSCache) Put(t *ProposeReplyTS) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeReplyTS) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *ProposeReplyTS) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *ProposeReplyTSMock) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeReplyTSMockCache struct {
	mu    sync.Mutex
	cache []*ProposeReplyTSMock
}

func NewProposeReplyTSMockCache() *ProposeReplyTSMockCache {
	c := &ProposeReplyTSMockCache{}
	c.cache = make([]*ProposeReplyTSMock, 0)
	return c
}

func (p *ProposeReplyTSMockCache) Get() *ProposeReplyTSMock {
	var t *ProposeReplyTSMock
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeReplyTSMock{}
	}
	return t
}
func (p *ProposeReplyTSMockCache) Put(t *ProposeReplyTSMock) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeReplyTSMock) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte

	// marshal OK, uint8
	bs = b[:1]
	bs[0] = byte(t.OK)
	wire.Write(bs)

	// marshal CommandId, int32
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// marshal Value, state.Value
	t.Value.Marshal(wire)

	// marshal Timestamp, int64
	bs = b[:8]
	tmp64 := t.Timestamp
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)

	// marshal MockInstruct, bool
	bs = b[:1]
	if t.MockInstruct {
		bs[0] = 1
	} else {
		bs[0] = 0
	}
	wire.Write(bs)
}

func (t *ProposeReplyTSMock) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte

	// read OK, uint8
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.OK = uint8(bs[0])

	// read CommandId, int32
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	// read Value, state.Value
	t.Value.Unmarshal(wire)

	// read Timestamp, int64
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))

	// read MockInstruct, bool
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.MockInstruct = bs[0] != 0

	return nil
}

func (t *RealCommitted) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type RealCommittedCache struct {
	mu    sync.Mutex
	cache []*RealCommitted
}

func NewRealCommittedCache() *RealCommittedCache {
	c := &RealCommittedCache{}
	c.cache = make([]*RealCommitted, 0)
	return c
}

func (p *RealCommittedCache) Get() *RealCommitted {
	var t *RealCommitted
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RealCommitted{}
	}
	return t
}
func (p *RealCommittedCache) Put(t *RealCommitted) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *RealCommitted) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.OpId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Commands))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Marshal(wire)
	}
}

func (t *RealCommitted) Unmarshal(rr io.Reader) error {
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
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OpId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Commands = make([]state.CommandAvi, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Unmarshal(wire)
	}
	return nil
}

func (t *RegisterClientIdArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 4, true
}

type RegisterClientIdArgsCache struct {
	mu    sync.Mutex
	cache []*RegisterClientIdArgs
}

func NewRegisterClientIdArgsCache() *RegisterClientIdArgsCache {
	c := &RegisterClientIdArgsCache{}
	c.cache = make([]*RegisterClientIdArgs, 0)
	return c
}

func (p *RegisterClientIdArgsCache) Get() *RegisterClientIdArgs {
	var t *RegisterClientIdArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RegisterClientIdArgs{}
	}
	return t
}
func (p *RegisterClientIdArgsCache) Put(t *RegisterClientIdArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *RegisterClientIdArgs) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *RegisterClientIdArgs) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	return nil
}

func (t *GetViewReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 13, true
}

type GetViewReplyCache struct {
	mu    sync.Mutex
	cache []*GetViewReply
}

func NewGetViewReplyCache() *GetViewReplyCache {
	c := &GetViewReplyCache{}
	c.cache = make([]*GetViewReply, 0)
	return c
}

func (p *GetViewReplyCache) Get() *GetViewReply {
	var t *GetViewReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &GetViewReply{}
	}
	return t
}
func (p *GetViewReplyCache) Put(t *GetViewReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *GetViewReply) Marshal(wire io.Writer) {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	bs[0] = byte(t.OK)
	tmp32 := t.ViewId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	tmp32 = t.PilotId
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	tmp32 = t.ReplicaId
	bs[9] = byte(tmp32)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32 >> 16)
	bs[12] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *GetViewReply) Unmarshal(wire io.Reader) error {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	if _, err := io.ReadAtLeast(wire, bs, 13); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.ViewId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.PilotId = int32((uint32(bs[5]) | (uint32(bs[6]) << 8) | (uint32(bs[7]) << 16) | (uint32(bs[8]) << 24)))
	t.ReplicaId = int32((uint32(bs[9]) | (uint32(bs[10]) << 8) | (uint32(bs[11]) << 16) | (uint32(bs[12]) << 24)))
	return nil
}

func (t *MockCommitted) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MockCommittedCache struct {
	mu    sync.Mutex
	cache []*MockCommitted
}

func NewMockCommittedCache() *MockCommittedCache {
	c := &MockCommittedCache{}
	c.cache = make([]*MockCommitted, 0)
	return c
}

func (p *MockCommittedCache) Get() *MockCommitted {
	var t *MockCommitted
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MockCommitted{}
	}
	return t
}
func (p *MockCommittedCache) Put(t *MockCommitted) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MockCommitted) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.OpId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Commands))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Marshal(wire)
	}
}

func (t *MockCommitted) Unmarshal(rr io.Reader) error {
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
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OpId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Commands = make([]state.CommandAvi, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Unmarshal(wire)
	}
	return nil
}

func (t *Beacon) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type BeaconCache struct {
	mu    sync.Mutex
	cache []*Beacon
}

func NewBeaconCache() *BeaconCache {
	c := &BeaconCache{}
	c.cache = make([]*Beacon, 0)
	return c
}

func (p *BeaconCache) Get() *Beacon {
	var t *Beacon
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Beacon{}
	}
	return t
}
func (p *BeaconCache) Put(t *Beacon) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Beacon) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *Beacon) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = uint64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *BeTheLeaderReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

type BeTheLeaderReplyCache struct {
	mu    sync.Mutex
	cache []*BeTheLeaderReply
}

func NewBeTheLeaderReplyCache() *BeTheLeaderReplyCache {
	c := &BeTheLeaderReplyCache{}
	c.cache = make([]*BeTheLeaderReply, 0)
	return c
}

func (p *BeTheLeaderReplyCache) Get() *BeTheLeaderReply {
	var t *BeTheLeaderReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeTheLeaderReply{}
	}
	return t
}
func (p *BeTheLeaderReplyCache) Put(t *BeTheLeaderReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeTheLeaderReply) Marshal(wire io.Writer) {
}

func (t *BeTheLeaderReply) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *CommittedFromClient) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommittedFromClientCache struct {
	mu    sync.Mutex
	cache []*CommittedFromClient
}

func NewCommittedFromClientCache() *CommittedFromClientCache {
	c := &CommittedFromClientCache{}
	c.cache = make([]*CommittedFromClient, 0)
	return c
}

func (p *CommittedFromClientCache) Get() *CommittedFromClient {
	var t *CommittedFromClient
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommittedFromClient{}
	}
	return t
}
func (p *CommittedFromClientCache) Put(t *CommittedFromClient) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommittedFromClient) Marshal(wire io.Writer) {
	var b [20]byte
	var bs []byte
	bs = b[:20]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	utmp32 := t.ClientId
	bs[4] = byte(utmp32)
	bs[5] = byte(utmp32 >> 8)
	bs[6] = byte(utmp32 >> 16)
	bs[7] = byte(utmp32 >> 24)
	tmp32 = t.OpId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp64 := t.Timestamp
	bs[12] = byte(tmp64)
	bs[13] = byte(tmp64 >> 8)
	bs[14] = byte(tmp64 >> 16)
	bs[15] = byte(tmp64 >> 24)
	bs[16] = byte(tmp64 >> 32)
	bs[17] = byte(tmp64 >> 40)
	bs[18] = byte(tmp64 >> 48)
	bs[19] = byte(tmp64 >> 56)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Commands))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Marshal(wire)
	}
}

func (t *CommittedFromClient) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [20]byte
	var bs []byte
	bs = b[:20]
	if _, err := io.ReadAtLeast(wire, bs, 20); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ClientId = uint32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OpId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Timestamp = int64((uint64(bs[12]) | (uint64(bs[13]) << 8) | (uint64(bs[14]) << 16) | (uint64(bs[15]) << 24) | (uint64(bs[16]) << 32) | (uint64(bs[17]) << 40) | (uint64(bs[18]) << 48) | (uint64(bs[19]) << 56)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Commands = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Commands[i].Unmarshal(wire)
	}
	return nil
}

func (t *CommitLatencyFeedback) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitLatencyFeedbackCache struct {
	mu    sync.Mutex
	cache []*CommitLatencyFeedback
}

func NewCommitLatencyFeedbackCache() *CommitLatencyFeedbackCache {
	c := &CommitLatencyFeedbackCache{}
	c.cache = make([]*CommitLatencyFeedback, 0)
	return c
}

func (p *CommitLatencyFeedbackCache) Get() *CommitLatencyFeedback {
	var t *CommitLatencyFeedback
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommitLatencyFeedback{}
	}
	return t
}
func (p *CommitLatencyFeedbackCache) Put(t *CommitLatencyFeedback) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommitLatencyFeedback) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte

	t.CommandId.Marshal(wire)

	// marshal t.RealInstance (int 32)
	bs = b[:4]
	tmp32 := t.RealInstance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// marshal t.GhostInstance (int 32)
	bs = b[:4]
	tmp32 = t.GhostInstance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// marshal t.RealCommitLatency (int 64)
	bs = b[:8]
	tmp64 := t.RealCommitLatency
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)

	// marshal t.RealCommitLatency (int 64)
	bs = b[:8]
	tmp64 = t.GhostCommitLatency
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)

	bs = b[:8]
	alen1 := int64(len(t.RealInstCmds))
	bs[0] = byte(alen1)
	bs[1] = byte(alen1 >> 8)
	bs[2] = byte(alen1 >> 16)
	bs[3] = byte(alen1 >> 24)
	bs[4] = byte(alen1 >> 32)
	bs[5] = byte(alen1 >> 40)
	bs[6] = byte(alen1 >> 48)
	bs[7] = byte(alen1 >> 56)
	wire.Write(bs)
	for i := int64(0); i < alen1; i++ {
		t.RealInstCmds[i].Marshal(wire)
	}

	bs = b[:8]
	alen2 := int64(len(t.GhostInstCmds))
	bs[0] = byte(alen2)
	bs[1] = byte(alen2 >> 8)
	bs[2] = byte(alen2 >> 16)
	bs[3] = byte(alen2 >> 24)
	bs[4] = byte(alen2 >> 32)
	bs[5] = byte(alen2 >> 40)
	bs[6] = byte(alen2 >> 48)
	bs[7] = byte(alen2 >> 56)
	wire.Write(bs)
	for i := int64(0); i < alen2; i++ {
		t.GhostInstCmds[i].Marshal(wire)
	}
}

func (t *CommitLatencyFeedback) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [8]byte
	var bs []byte

	t.CommandId.Unmarshal(wire)

	// unmarshal t.RealInstance (int 32)
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.RealInstance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	// unmarshal t.GhostInstance (int 32)
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.GhostInstance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	// unmarshal RealCommitLatency  (int 64)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.RealCommitLatency = int64((uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))

	// unmarshal GhostCommitLatency  (int 64)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.GhostCommitLatency = int64((uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))

	// unmarshal realInstCmds
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	alen1 := int64((uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))
	t.RealInstCmds = make([]state.CommandAvi, alen1)
	for i := int64(0); i < alen1; i++ {
		t.RealInstCmds[i].Unmarshal(wire)
	}

	// unmarshal ghostInstCmds
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	alen2 := int64((uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2] << 16)) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))
	t.GhostInstCmds = make([]state.CommandAvi, alen2)
	for i := int64(0); i < alen2; i++ {
		t.GhostInstCmds[i].Unmarshal(wire)
	}
	return nil
}

func (t *Read) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ReadCache struct {
	mu    sync.Mutex
	cache []*Read
}

func NewReadCache() *ReadCache {
	c := &ReadCache{}
	c.cache = make([]*Read, 0)
	return c
}

func (p *ReadCache) Get() *Read {
	var t *Read
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Read{}
	}
	return t
}
func (p *ReadCache) Put(t *Read) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Read) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Key.Marshal(wire)
}

func (t *Read) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Key.Unmarshal(wire)
	return nil
}

func (t *BeaconReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type BeaconReplyCache struct {
	mu    sync.Mutex
	cache []*BeaconReply
}

func NewBeaconReplyCache() *BeaconReplyCache {
	c := &BeaconReplyCache{}
	c.cache = make([]*BeaconReply, 0)
	return c
}

func (p *BeaconReplyCache) Get() *BeaconReply {
	var t *BeaconReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeaconReply{}
	}
	return t
}
func (p *BeaconReplyCache) Put(t *BeaconReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeaconReply) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *BeaconReply) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = uint64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *PingReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
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
}

func (t *PingReply) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *BeTheLeaderArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

type BeTheLeaderArgsCache struct {
	mu    sync.Mutex
	cache []*BeTheLeaderArgs
}

func NewBeTheLeaderArgsCache() *BeTheLeaderArgsCache {
	c := &BeTheLeaderArgsCache{}
	c.cache = make([]*BeTheLeaderArgs, 0)
	return c
}

func (p *BeTheLeaderArgsCache) Get() *BeTheLeaderArgs {
	var t *BeTheLeaderArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeTheLeaderArgs{}
	}
	return t
}
func (p *BeTheLeaderArgsCache) Put(t *BeTheLeaderArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeTheLeaderArgs) Marshal(wire io.Writer) {
}

func (t *BeTheLeaderArgs) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *ProposeWithExecTime) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeWithExecTimeCache struct {
	mu    sync.Mutex
	cache []*ProposeWithExecTime
}

func NewProposeWithExecTimeCache() *ProposeWithExecTimeCache {
	c := &ProposeWithExecTimeCache{}
	c.cache = make([]*ProposeWithExecTime, 0)
	return c
}

func (p *ProposeWithExecTimeCache) Get() *ProposeWithExecTime {
	var t *ProposeWithExecTime
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeWithExecTime{}
	}
	return t
}
func (p *ProposeWithExecTimeCache) Put(t *ProposeWithExecTime) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeWithExecTime) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte

	// Marshal CommandId (int32 = 4 bytes)
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// Marshal Command
	t.Command.Marshal(wire)

	// Marshal EndToEndLatency.Latency (int64 = 8 bytes)
	bs = b[:8]
	tmp64 := t.EndToEndLatency.Latency
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)

	// Marshal EndToEndLatency.CommandId (int32 = 4 bytes)
	bs = b[:4]
	tmp32 = t.EndToEndLatency.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ProposeWithExecTime) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte

	// Unmarshal CommandId (int32 = 4 bytes)
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	// Unmarshal Command
	t.Command.Unmarshal(wire)

	// Unmarshal EndToEndLatency.Latency (int64 = 8 bytes)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.EndToEndLatency.Latency = int64(
		(uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) |
			(uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))

	// Unmarshal EndToEndLatency.CommandId (int32 = 4 bytes)
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.EndToEndLatency.CommandId = int32((uint32(bs[0])) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24))

	return nil
}

func (t *MockExecTime_) BinarySize() (nbytes int, sizeKnown bool) {
	return 5, true
}

type MockExecTime_Cache struct {
	mu    sync.Mutex
	cache []*MockExecTime_
}

func NewMockExecTime_Cache() *MockExecTime_Cache {
	c := &MockExecTime_Cache{}
	c.cache = make([]*MockExecTime_, 0)
	return c
}

func (p *MockExecTime_Cache) Get() *MockExecTime_ {
	var t *MockExecTime_
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MockExecTime_{}
	}
	return t
}
func (p *MockExecTime_Cache) Put(t *MockExecTime_) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MockExecTime_) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte

	// Marshal MockExecTime.ExecTime (int64)
	bs = b[:8]
	tmp64 := t.ExecTime
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)

	// Marshal MockExecTime.DoMock (bool)
	bs = b[:1]
	if t.DoMock {
		bs[0] = 1
	} else {
		bs[0] = 0
	}
	wire.Write(bs)

	// Marshal MockExecTime.CommandId (int32)
	t.CommandId.Marshal(wire)
}

func (t *MockExecTime_) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte

	// Unmarshal MockExecTime.ExecTime (int64 = 8 bytes)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ExecTime = int64(
		(uint64(bs[0])) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) |
			(uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56))

	// Unmarshal MockExecTime.DoMock (bool)
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.DoMock = (bs[0] != 0)

	// Unmarshal MockExecTime.CommandId (int32 = 4 bytes)
	t.CommandId.Unmarshal(wire)
	return nil
}

func (t *ProposeReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 5, true
}

type ProposeReplyCache struct {
	mu    sync.Mutex
	cache []*ProposeReply
}

func NewProposeReplyCache() *ProposeReplyCache {
	c := &ProposeReplyCache{}
	c.cache = make([]*ProposeReply, 0)
	return c
}

func (p *ProposeReplyCache) Get() *ProposeReply {
	var t *ProposeReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeReply{}
	}
	return t
}
func (p *ProposeReplyCache) Put(t *ProposeReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ProposeReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	return nil
}

func (t *RealCommitAtLeast) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type RealCommitAtLeastCache struct {
	mu    sync.Mutex
	cache []*RealCommitAtLeast
}

func NewRealCommitAtLeastCache() *RealCommitAtLeastCache {
	c := &RealCommitAtLeastCache{}
	c.cache = make([]*RealCommitAtLeast, 0)
	return c
}

func (p *RealCommitAtLeastCache) Get() *RealCommitAtLeast {
	var t *RealCommitAtLeast
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RealCommitAtLeast{}
	}
	return t
}
func (p *RealCommitAtLeastCache) Put(t *RealCommitAtLeast) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *RealCommitAtLeast) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	utmp32 := t.CommandId
	bs[4] = byte(utmp32)
	bs[5] = byte(utmp32 >> 8)
	bs[6] = byte(utmp32 >> 16)
	bs[7] = byte(utmp32 >> 24)
	tmp64 := t.Timestamp
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *RealCommitAtLeast) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CommandId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Timestamp = int64((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *RegisterClientIdReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 1, true
}

type RegisterClientIdReplyCache struct {
	mu    sync.Mutex
	cache []*RegisterClientIdReply
}

func NewRegisterClientIdReplyCache() *RegisterClientIdReplyCache {
	c := &RegisterClientIdReplyCache{}
	c.cache = make([]*RegisterClientIdReply, 0)
	return c
}

func (p *RegisterClientIdReplyCache) Get() *RegisterClientIdReply {
	var t *RegisterClientIdReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RegisterClientIdReply{}
	}
	return t
}
func (p *RegisterClientIdReplyCache) Put(t *RegisterClientIdReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *RegisterClientIdReply) Marshal(wire io.Writer) {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	bs[0] = byte(t.OK)
	wire.Write(bs)
}

func (t *RegisterClientIdReply) Unmarshal(wire io.Reader) error {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	return nil
}

func (t *GetView) BinarySize() (nbytes int, sizeKnown bool) {
	return 4, true
}

type GetViewCache struct {
	mu    sync.Mutex
	cache []*GetView
}

func NewGetViewCache() *GetViewCache {
	c := &GetViewCache{}
	c.cache = make([]*GetView, 0)
	return c
}

func (p *GetViewCache) Get() *GetView {
	var t *GetView
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &GetView{}
	}
	return t
}
func (p *GetViewCache) Put(t *GetView) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *GetView) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *GetView) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
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
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
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

func (t *ProposeAndReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeAndReadReplyCache struct {
	mu    sync.Mutex
	cache []*ProposeAndReadReply
}

func NewProposeAndReadReplyCache() *ProposeAndReadReplyCache {
	c := &ProposeAndReadReplyCache{}
	c.cache = make([]*ProposeAndReadReply, 0)
	return c
}

func (p *ProposeAndReadReplyCache) Get() *ProposeAndReadReply {
	var t *ProposeAndReadReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeAndReadReply{}
	}
	return t
}
func (p *ProposeAndReadReplyCache) Put(t *ProposeAndReadReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeAndReadReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ProposeAndReadReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *PingArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 1, true
}

type PingArgsCache struct {
	mu    sync.Mutex
	cache []*PingArgs
}

func NewPingArgsCache() *PingArgsCache {
	c := &PingArgsCache{}
	c.cache = make([]*PingArgs, 0)
	return c
}

func (p *PingArgsCache) Get() *PingArgs {
	var t *PingArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PingArgs{}
	}
	return t
}
func (p *PingArgsCache) Put(t *PingArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PingArgs) Marshal(wire io.Writer) {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	bs[0] = byte(t.ActAsLeader)
	wire.Write(bs)
}

func (t *PingArgs) Unmarshal(wire io.Reader) error {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.ActAsLeader = uint8(bs[0])
	return nil
}
