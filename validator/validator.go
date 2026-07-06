// Package validator builds the wire messages a node publishes. It is the
// message-creating half: one Make* builder per message family, size-realistic
// filler, and no knowledge of who sends what when — duties come from the
// schedule and timing from the driver.
package validator

import (
	"bytes"

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

// MakeBlock builds a block-sized Message: a pb.Block carrying size bytes of
// all-ones filler (non-zero so proto3 doesn't drop it). The slot/origin fields
// add a negligible handful of header bytes — we don't size to an exact wire
// total.
func MakeBlock(slot, origin, size int) Message {
	blk := &pb.Block{
		Slot:    uint32(slot),
		Origin:  uint32(origin),
		Payload: bytes.Repeat([]byte{1}, size),
	}
	buf, err := proto.Marshal(blk)
	if err != nil {
		panic("MakeBlock: marshal: " + err.Error())
	}
	return Message{Topic: BlockTopic, Payload: buf, Slot: slot}
}
