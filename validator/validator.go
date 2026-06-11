// Package validator produces a node's per-slot duties and the messages they
// carry. It is the message-creating half: a Driver asks Duties(slot) each slot
// and never looks inside a Message. Phase 1's only duty is proposing a block
// when it's this node's turn (cyclic).
package validator

import (
	"bytes"
	"math/rand/v2"
	"time"

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

// PublishMessage is a message to publish at an offset into the slot.
type PublishMessage struct {
	Msg Message
	At  time.Duration
}

// Proposer is a node's block-proposing duty actor: one per node. The node's other (voting)
// duties come from the schedule, not from here — this only answers "do I propose this slot?".
type Proposer struct {
	self      int
	n         int
	blockSize int
	offset    time.Duration
	jitter    time.Duration
	rng       *rand.Rand
	proposers []int // proposers[slot] = proposing node; nil ⇒ cyclic slot%n
}

// NewProposer returns a Proposer that proposes blockSize-byte blocks on its turn, publishing at
// offset + rand(0, jitter) into the slot. proposers is the per-slot proposer schedule (from
// schedule.json, all supernodes); nil falls back to the cyclic slot%n rule (block-only
// runs). rng should be seeded by the caller for reproducibility.
func NewProposer(self, n, blockSize int, offset, jitter time.Duration, rng *rand.Rand, proposers []int) *Proposer {
	return &Proposer{
		self: self, n: n, blockSize: blockSize,
		offset: offset, jitter: jitter, rng: rng, proposers: proposers,
	}
}

// BlocksToPublish returns this slot's block-publish work: propose a block iff this node is the
// slot's proposer (from the schedule when set, else the cyclic slot%n rule).
func (p *Proposer) BlocksToPublish(slot int) []PublishMessage {
	proposer := slot % p.n
	if len(p.proposers) > 0 {
		proposer = p.proposers[slot]
	}
	if proposer != p.self {
		return nil
	}
	at := p.offset
	if p.jitter > 0 {
		at += time.Duration(p.rng.Int64N(int64(p.jitter)))
	}
	return []PublishMessage{{Msg: makeBlock(slot, p.self, p.blockSize), At: at}}
}

// makeBlock builds a block-sized Message: a pb.Block carrying size bytes of
// all-ones filler (non-zero so proto3 doesn't drop it). The slot/origin fields
// add a negligible handful of header bytes — we don't size to an exact wire
// total.
func makeBlock(slot, origin, size int) Message {
	blk := &pb.Block{
		Slot:    uint32(slot),
		Origin:  uint32(origin),
		Payload: bytes.Repeat([]byte{1}, size),
	}
	buf, err := proto.Marshal(blk)
	if err != nil {
		panic("makeBlock: marshal: " + err.Error())
	}
	return Message{Topic: BlockTopic, Payload: buf, Slot: slot}
}
