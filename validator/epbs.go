package validator

import (
	"bytes"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// ConsensusBlockTopic is the global gossipsub topic for ePBS consensus blocks (Gloas
// beacon_block: the small proposer message carrying the builder's bid, no payload).
const ConsensusBlockTopic = "/eth2/00000000/consensus_block/ssz_snappy"

// ExecutionPayloadTopic is the global gossipsub topic for the builder's payload reveal
// (Gloas execution_payload: the SignedExecutionPayloadEnvelope, the big message).
const ExecutionPayloadTopic = "/eth2/00000000/execution_payload/ssz_snappy"

// PTCVoteTopic is the single global gossipsub topic for PTC votes (Gloas
// payload_attestation_message): every node subscribes it and downloads all 512 votes per
// slot, like the availability-vote flood.
const PTCVoteTopic = "/eth2/00000000/payload_attestation_message/ssz_snappy"

// PTCVoteSize is the target marshaled size of one PTC vote (PayloadAttestationData:
// root 32 + slot 8 + 2 bools, + validator index + BLS sig 96 ≈ 176 B). Realistic
// per-message flood size is what matters; the filler lands close, not exact.
const PTCVoteSize = 176

// MakeConsensusBlock builds the ePBS consensus block as a publish-ready Message: size bytes
// of all-ones filler, like MakeBlock (the two are the block family split in two; sizing is
// approximate by the same few header bytes).
func MakeConsensusBlock(slot, origin, size int) Message {
	cb := &pb.ConsensusBlock{
		Slot:    uint32(slot),
		Origin:  uint32(origin),
		Payload: bytes.Repeat([]byte{1}, size),
	}
	buf, err := proto.Marshal(cb)
	if err != nil {
		panic("MakeConsensusBlock: marshal: " + err.Error())
	}
	return Message{Topic: ConsensusBlockTopic, Payload: buf, Slot: slot}
}

// MakeExecutionPayload builds the ePBS payload reveal as a publish-ready Message. Direct
// all-ones filler like MakeBlock: the payload is the big message (>16 KiB), past
// sizedFiller's 2-byte-varint envelope.
func MakeExecutionPayload(slot, origin, size int) Message {
	ep := &pb.ExecutionPayload{
		Slot:    uint32(slot),
		Origin:  uint32(origin),
		Payload: bytes.Repeat([]byte{1}, size),
	}
	buf, err := proto.Marshal(ep)
	if err != nil {
		panic("MakeExecutionPayload: marshal: " + err.Error())
	}
	return Message{Topic: ExecutionPayloadTopic, Payload: buf, Slot: slot}
}

// MakePTCVote builds one PTC vote as a publish-ready Message on the global topic. origin
// is the publishing node (gossip origin for the loopback skip). payloadPresent records
// whether the payload and the node's custody columns had arrived by the PTC deadline.
// payload is non-zero filler sized to ≈ 176 B.
func MakePTCVote(slot, val, origin int, payloadPresent bool) Message {
	v := &pb.PTCVote{
		Slot:           uint32(slot),
		Val:            uint32(val),
		Origin:         uint32(origin),
		PayloadPresent: payloadPresent,
	}
	v.Payload = sizedFiller(v, PTCVoteSize)
	buf, err := proto.Marshal(v)
	if err != nil {
		panic("MakePTCVote: marshal: " + err.Error())
	}
	return Message{Topic: PTCVoteTopic, Payload: buf, Slot: slot}
}
