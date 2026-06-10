package validator

import (
	"bytes"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// Partial-transport size knobs (partial-attestation-spec.md §2). Classic's 240 B attestation
// splits consistently by construction: 128 (data) + 96 (sig) + identity overhead ≈ 240.
const (
	// PartialAttDataSize is the default marshaled size of one PartialAttData (≈ a real
	// AttestationData) — sent once per batch, shared by every vote in the bucket.
	PartialAttDataSize = 128
	// PartialSignatureSize is the default per-vote signature filler (BLS, 96 B) — the
	// marginal payload of one extra vote in a batch.
	PartialSignatureSize = 96
)

// MakePartialAttData builds the shared attestation_data for one (slot/group, subnet, vote)
// bucket: the vote scalars + all-ones filler to size. Deterministic — every attester voting the
// same way produces byte-identical data, and the bytes ARE the bucket key (the transport never
// parses them). votedOrigin is the voted block's origin when >= 0 (pass 0 for vote-free kinds,
// i.e. finality votes this cut) or the prior-head sentinel when < 0, exactly as MakeAttestation
// encodes it — so a committee's block-voters and prior-head-voters form two fork buckets.
func MakePartialAttData(slot, subnet, votedOrigin, size int) []byte {
	voted := PriorHead
	if votedOrigin >= 0 {
		voted = uint32(votedOrigin)
	}
	ad := &pb.PartialAttData{
		Slot:        uint32(slot),
		Subnet:      uint32(subnet),
		VotedOrigin: voted,
	}
	ad.Payload = sizedFiller(ad, size)
	buf, err := proto.Marshal(ad)
	if err != nil {
		panic("MakePartialAttData: marshal: " + err.Error())
	}
	return buf
}

// MakePartialSignature returns the deterministic all-ones signature filler for one vote.
func MakePartialSignature(size int) []byte {
	return bytes.Repeat([]byte{1}, size)
}
