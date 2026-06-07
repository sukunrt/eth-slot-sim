package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestAttestationTopic(t *testing.T) {
	if got, want := AttestationTopic(7), "/eth2/00000000/beacon_attestation_7/ssz_snappy"; got != want {
		t.Fatalf("AttestationTopic(7) = %q, want %q", got, want)
	}
}

func TestMakeAttestationFields(t *testing.T) {
	msg := MakeAttestation(3, 7, 42, 5, 9) // slot, subnet, val, origin, votedOrigin
	if msg.Topic != AttestationTopic(7) {
		t.Fatalf("topic = %q, want %q", msg.Topic, AttestationTopic(7))
	}
	if msg.Slot != 3 {
		t.Fatalf("msg.Slot = %d, want 3", msg.Slot)
	}
	var att pb.Attestation
	if err := proto.Unmarshal(msg.Payload, &att); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if att.Slot != 3 || att.Subnet != 7 || att.Val != 42 || att.Origin != 5 || att.VotedOrigin != 9 {
		t.Fatalf("decoded %+v, want slot3 subnet7 val42 origin5 voted9", &att)
	}
}

func TestMakeAttestationSizeAndFiller(t *testing.T) {
	msg := MakeAttestation(1, 0, 1, 1, 1)
	// SingleAttestation is 240 B; we size the filler to land close (exact wire total
	// isn't load-bearing, only that the flood carries a realistic per-message size).
	if n := len(msg.Payload); n < 230 || n > 250 {
		t.Fatalf("marshaled attestation = %d bytes, want ~240", n)
	}
	var att pb.Attestation
	if err := proto.Unmarshal(msg.Payload, &att); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(att.Payload) == 0 || bytes.Equal(att.Payload, make([]byte, len(att.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}

func TestMakeAttestationVote(t *testing.T) {
	// votedOrigin >= 0 → voted for that block (proposer node); VotedBlock true.
	block := MakeAttestation(1, 0, 1, 2, 4)
	att := decodeAtt(t, block)
	if !VotedBlock(att) || att.VotedOrigin != 4 {
		t.Fatalf("block vote: VotedBlock=%v voted_origin=%d, want true/4", VotedBlock(att), att.VotedOrigin)
	}
	// votedOrigin < 0 → prior head; VotedBlock false, sentinel stored.
	prior := MakeAttestation(1, 0, 1, 2, -1)
	att = decodeAtt(t, prior)
	if VotedBlock(att) || att.VotedOrigin != PriorHead {
		t.Fatalf("prior vote: VotedBlock=%v voted_origin=%d, want false/%d", VotedBlock(att), att.VotedOrigin, PriorHead)
	}
}

func decodeAtt(t *testing.T, msg Message) *pb.Attestation {
	t.Helper()
	var att pb.Attestation
	if err := proto.Unmarshal(msg.Payload, &att); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &att
}
