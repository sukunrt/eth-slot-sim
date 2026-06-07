package validator

import (
	"bytes"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// AttestationTopicPrefix is the per-subnet attestation topic stem; the subnet id and
// the ssz_snappy suffix complete it (ssz_snappy is in the name only — raw bytes).
const AttestationTopicPrefix = "/eth2/00000000/beacon_attestation_"

// AttestationSize is the target marshaled size of one attestation (SingleAttestation,
// 240 B). The exact wire total isn't load-bearing — only that the flood carries a
// realistic per-message size — so the filler lands close, not exact.
const AttestationSize = 240

// PriorHead is the voted_origin sentinel meaning "voted for the prior head", recorded
// when a node hadn't processed the block by the attestation deadline.
const PriorHead uint32 = math.MaxUint32

// AttestationTopic is the gossipsub topic for committee subnet's attestations.
func AttestationTopic(subnet int) string {
	return fmt.Sprintf("%s%d/ssz_snappy", AttestationTopicPrefix, subnet)
}

// VotedBlock reports whether an attestation voted for the slot's block (vs. the prior
// head sentinel).
func VotedBlock(att *pb.Attestation) bool { return att.VotedOrigin != PriorHead }

// MakeAttestation builds one SingleAttestation as a publish-ready Message. votedOrigin
// is the voted block's origin (proposer node) when >= 0, or the prior-head sentinel
// when < 0. payload is non-zero filler sized so the marshaled message ≈ AttestationSize.
func MakeAttestation(slot, subnet, val, origin, votedOrigin int) Message {
	voted := PriorHead
	if votedOrigin >= 0 {
		voted = uint32(votedOrigin)
	}
	att := &pb.Attestation{
		Slot:        uint32(slot),
		Subnet:      uint32(subnet),
		Val:         uint32(val),
		Origin:      uint32(origin),
		VotedOrigin: voted,
	}
	att.Payload = sizedFiller(att, AttestationSize)
	buf, err := proto.Marshal(att)
	if err != nil {
		panic("MakeAttestation: marshal: " + err.Error())
	}
	return Message{Topic: AttestationTopic(subnet), Payload: buf, Slot: slot}
}

// sizedFiller returns all-ones filler so proto.Marshal(att) lands near target bytes.
// Accounts for the payload field's wire framing (tag + length varint) and the other
// fields' current size, which varies (e.g. the prior-head sentinel is a 5-byte varint).
func sizedFiller(att *pb.Attestation, target int) []byte {
	att.Payload = nil
	overhead := proto.Size(att) // size of fields 1..5, no payload
	n := target - overhead - 1  // minus the payload field's tag byte
	if n > 127 {
		n -= 2 // 2-byte length varint (payloads here are < 16384 B)
	} else {
		n--
	}
	if n < 1 {
		n = 1
	}
	return bytes.Repeat([]byte{1}, n)
}
