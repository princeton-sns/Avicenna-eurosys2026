package state

import (
	"encoding/binary"
	"io"
	"sync"
)

func (t *Command) Marshal(w io.Writer) {
	var b [8]byte

	// ClientId
	bs := b[:4]
	utmp32 := t.ClientId
	bs[0] = byte(utmp32)
	bs[1] = byte(utmp32 >> 8)
	bs[2] = byte(utmp32 >> 16)
	bs[3] = byte(utmp32 >> 24)
	w.Write(bs)

	// OpId
	bs = b[:4]
	tmp32 := t.OpId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	w.Write(bs)
	// Op
	bs = b[:1]
	b[0] = byte(t.Op)
	w.Write(bs)

	bs = b[:8]
	// K
	binary.LittleEndian.PutUint64(bs, uint64(t.K))
	w.Write(bs)
	// V
	binary.LittleEndian.PutUint64(bs, uint64(t.V))
	w.Write(bs)
}

func (t *Command) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:4]

	// ClientId
	if _, err := io.ReadAtLeast(r, bs, 4); err != nil {
		return err
	}
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	// OpId
	bs = b[:4]
	if _, err := io.ReadAtLeast(r, bs, 4); err != nil {
		return err
	}
	//t.OpId = OperationId((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OpId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	// Op
	bs = b[:1]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.Op = Operation(b[0])
	bs = b[:8]
	// K
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.K = Key(binary.LittleEndian.Uint64(bs))
	// V
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	t.V = Value(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *CommandAvi) Marshal(w io.Writer) {
	var b [8]byte

	t.Cmd.Marshal(w)

	// DoMock uint8
	bs := b[:1]
	if t.DoMock {
		bs[0] = 1
	} else {
		bs[0] = 0
	}
	w.Write(bs)
}

func (t *CommandAvi) Unmarshal(r io.Reader) error {
	var b [8]byte

	t.Cmd.Unmarshal(r)

	// read DoMock, uint8
	bs := b[:1]
	if _, err := io.ReadAtLeast(r, bs, 1); err != nil {
		return err
	}
	t.DoMock = bs[0] != 0
	return nil
}

func (t *Key) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Value) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Key) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *Value) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Value(binary.LittleEndian.Uint64(bs))
	return nil
}

// added for CommandId
func (t *CommandId) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type CommandIdCache struct {
	mu    sync.Mutex
	cache []*CommandId
}

func NewCommandIdCache() *CommandIdCache {
	c := &CommandIdCache{}
	c.cache = make([]*CommandId, 0)
	return c
}

func (p *CommandIdCache) Get() *CommandId {
	var t *CommandId
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommandId{}
	}
	return t
}
func (p *CommandIdCache) Put(t *CommandId) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommandId) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	utmp32 := t.OpId
	bs[4] = byte(utmp32)
	bs[5] = byte(utmp32 >> 8)
	bs[6] = byte(utmp32 >> 16)
	bs[7] = byte(utmp32 >> 24)
	wire.Write(bs)
}

func (t *CommandId) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ClientId = uint32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OpId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}
