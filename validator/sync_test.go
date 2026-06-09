package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestSyncMessageTopic(t *testing.T) {
	if got, want := SyncMessageTopic(3), "/eth2/00000000/sync_committee_3/ssz_snappy"; got != want {
		t.Fatalf("SyncMessageTopic(3) = %q, want %q", got, want)
	}
}

func TestSyncContributionTopic(t *testing.T) {
	want := "/eth2/00000000/sync_committee_contribution_and_proof/ssz_snappy"
	if SyncContributionTopic != want {
		t.Fatalf("SyncContributionTopic = %q, want %q", SyncContributionTopic, want)
	}
}

func TestMakeSyncMessageFields(t *testing.T) {
	msg := MakeSyncMessage(3, 2, 5, 9) // slot, subnet, origin, votedOrigin
	if msg.Topic != SyncMessageTopic(2) {
		t.Fatalf("topic = %q, want %q", msg.Topic, SyncMessageTopic(2))
	}
	if msg.Slot != 3 {
		t.Fatalf("msg.Slot = %d, want 3", msg.Slot)
	}
	var sm pb.SyncMessage
	if err := proto.Unmarshal(msg.Payload, &sm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sm.Slot != 3 || sm.Subnet != 2 || sm.Origin != 5 || sm.VotedOrigin != 9 {
		t.Fatalf("decoded %+v, want slot3 subnet2 origin5 voted9", &sm)
	}
}

func TestMakeSyncMessageSizeAndFiller(t *testing.T) {
	msg := MakeSyncMessage(1, 0, 1, 1)
	// SyncCommitteeMessage is 144 B; the filler lands close (only a realistic per-message
	// size is load-bearing, not the exact wire total).
	if n := len(msg.Payload); n < 134 || n > 154 {
		t.Fatalf("marshaled sync message = %d bytes, want ~144", n)
	}
	var sm pb.SyncMessage
	if err := proto.Unmarshal(msg.Payload, &sm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(sm.Payload) == 0 || bytes.Equal(sm.Payload, make([]byte, len(sm.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}

func TestMakeSyncMessageVote(t *testing.T) {
	// votedOrigin >= 0 → voted that block (proposer node).
	head := MakeSyncMessage(1, 0, 2, 4)
	var sm pb.SyncMessage
	if err := proto.Unmarshal(head.Payload, &sm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sm.VotedOrigin != 4 {
		t.Fatalf("head vote: voted_origin=%d, want 4", sm.VotedOrigin)
	}
	// votedOrigin < 0 → prior head; the shared PriorHead sentinel is stored.
	prior := MakeSyncMessage(1, 0, 2, -1)
	if err := proto.Unmarshal(prior.Payload, &sm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sm.VotedOrigin != PriorHead {
		t.Fatalf("prior vote: voted_origin=%d, want %d", sm.VotedOrigin, PriorHead)
	}
}

func TestMakeSyncContributionFields(t *testing.T) {
	msg := MakeSyncContribution(3, 2, 5) // slot, subnet, origin (aggregator)
	if msg.Topic != SyncContributionTopic {
		t.Fatalf("topic = %q, want %q", msg.Topic, SyncContributionTopic)
	}
	if msg.Slot != 3 {
		t.Fatalf("msg.Slot = %d, want 3", msg.Slot)
	}
	var sc pb.SyncContribution
	if err := proto.Unmarshal(msg.Payload, &sc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sc.Slot != 3 || sc.Subnet != 2 || sc.Origin != 5 {
		t.Fatalf("decoded %+v, want slot3 subnet2 origin5", &sc)
	}
}

func TestMakeSyncContributionSizeAndFiller(t *testing.T) {
	msg := MakeSyncContribution(1, 0, 1)
	// SignedContributionAndProof is 360 B; the filler lands close.
	if n := len(msg.Payload); n < 350 || n > 370 {
		t.Fatalf("marshaled sync contribution = %d bytes, want ~360", n)
	}
	var sc pb.SyncContribution
	if err := proto.Unmarshal(msg.Payload, &sc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(sc.Payload) == 0 || bytes.Equal(sc.Payload, make([]byte, len(sc.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}
