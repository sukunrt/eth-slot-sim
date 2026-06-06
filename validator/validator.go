// Package validator produces a node's per-slot duties and the messages they
// carry. It is the message-creating half of the Node/Validator split: the Node
// asks Duties(slot) each slot and never looks inside a Message. Phase 1's only
// duty is proposing a block when it's this node's turn (cyclic).
package validator

import (
	"encoding/binary"
	"math/rand/v2"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// BlockTopic is the global gossipsub topic for beacon blocks (placeholder fork
// digest; ssz_snappy is in the name only — we publish raw bytes).
const BlockTopic = "/eth2/00000000/beacon_block/ssz_snappy"

// Message is a marshaled wire payload ready to publish on a topic.
type Message struct {
	Topic   string
	Payload []byte
	Slot    int // for metrics correlation; also inside the payload
}

// Duty is a message to publish at an offset into the slot.
type Duty struct {
	Msg Message
	At  time.Duration
}

// Validator is the duty actor a node runs. Phase 1: one per node.
type Validator struct {
	self      int
	n         int
	blockSize int
	offset    time.Duration
	jitter    time.Duration
	rng       *rand.Rand
}

// New returns a Validator that proposes blockSize-byte blocks on its turn,
// publishing at offset + rand(0, jitter) into the slot. rng should be seeded by
// the caller for reproducibility.
func New(self, n, blockSize int, offset, jitter time.Duration, rng *rand.Rand) *Validator {
	return &Validator{self: self, n: n, blockSize: blockSize, offset: offset, jitter: jitter, rng: rng}
}

// Duties returns this slot's duties. Phase 1: propose a block iff it's our turn.
func (v *Validator) Duties(slot int) []Duty {
	if slot%v.n != v.self {
		return nil
	}
	at := v.offset
	if v.jitter > 0 {
		at += time.Duration(v.rng.Int64N(int64(v.jitter)))
	}
	return []Duty{{Msg: makeBlock(slot, v.self, v.blockSize), At: at}}
}

// makeBlock builds a Block whose marshaled size is exactly target bytes, with
// random (incompressible) filler. The header size is measured, not assumed:
// proto3 omits zero-valued fields and varint widths grow with the value, so the
// payload length is solved against the actual header and length-prefix width.
func makeBlock(slot, origin, target int) Message {
	blk := &pb.Block{Slot: uint32(slot), Origin: uint32(origin)}
	header := proto.Size(blk) // slot + origin fields, payload still empty

	// marshaled = header + 1 (payload tag) + SizeVarint(len) + len == target.
	// Find the length-prefix width lv that's self-consistent with the length.
	rem := target - header - 1
	payloadLen := -1
	for lv := 1; lv <= protowire.SizeVarint(uint64(target)); lv++ {
		if n := rem - lv; n >= 0 && protowire.SizeVarint(uint64(n)) == lv {
			payloadLen = n
			break
		}
	}
	if payloadLen < 0 {
		panic("makeBlock: target too small for header")
	}

	blk.Payload = randomFiller(slot, payloadLen)
	buf, err := proto.Marshal(blk)
	if err != nil {
		panic("makeBlock: marshal: " + err.Error())
	}
	if len(buf) != target {
		panic("makeBlock: sized payload but marshaled size != target")
	}
	return Message{Topic: BlockTopic, Payload: buf, Slot: slot}
}

// randomFiller returns n incompressible bytes, deterministic per slot so runs
// are reproducible under synctest.
func randomFiller(slot, n int) []byte {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(slot))
	r := rand.NewChaCha8(seed)
	b := make([]byte, n)
	r.Read(b)
	return b
}
