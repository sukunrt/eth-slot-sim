package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestDecoupledTopics(t *testing.T) {
	if got, want := AvailabilityVoteTopic, "/eth2/00000000/availability_vote/ssz_snappy"; got != want {
		t.Fatalf("AvailabilityVoteTopic = %q, want %q", got, want)
	}
	if got, want := FinalityVoteTopic(7), "/eth2/00000000/finality_vote_7/ssz_snappy"; got != want {
		t.Fatalf("FinalityVoteTopic(7) = %q, want %q", got, want)
	}
	if got, want := FinalityAggregateTopic, "/eth2/00000000/finality_aggregate_and_proof/ssz_snappy"; got != want {
		t.Fatalf("FinalityAggregateTopic = %q, want %q", got, want)
	}
}

func TestMakeACVoteFields(t *testing.T) {
	msg := MakeACVote(3, 42, 5, 9) // slot, val, origin, votedOrigin
	if msg.Topic != AvailabilityVoteTopic {
		t.Fatalf("topic = %q, want %q", msg.Topic, AvailabilityVoteTopic)
	}
	if msg.Slot != 3 {
		t.Fatalf("msg.Slot = %d, want 3", msg.Slot)
	}
	var v pb.ACVote
	if err := proto.Unmarshal(msg.Payload, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v.Slot != 3 || v.Val != 42 || v.Origin != 5 || v.VotedOrigin != 9 {
		t.Fatalf("decoded %+v, want slot3 val42 origin5 voted9", &v)
	}
}

func TestMakeACVoteVote(t *testing.T) {
	// votedOrigin >= 0 → voted for that block (proposer node).
	block := decodeACVote(t, MakeACVote(1, 1, 2, 4))
	if block.VotedOrigin != 4 {
		t.Fatalf("block vote: voted_origin = %d, want 4", block.VotedOrigin)
	}
	// votedOrigin < 0 → prior head; the PriorHead sentinel (reused from attestations) is stored.
	prior := decodeACVote(t, MakeACVote(1, 1, 2, -1))
	if prior.VotedOrigin != PriorHead {
		t.Fatalf("prior vote: voted_origin = %d, want %d", prior.VotedOrigin, PriorHead)
	}
}

func TestMakeACVoteSizeAndFiller(t *testing.T) {
	msg := MakeACVote(1, 1, 1, 1)
	if n := len(msg.Payload); n < ACVoteSize-10 || n > ACVoteSize+10 {
		t.Fatalf("marshaled AC vote = %d bytes, want ~%d", n, ACVoteSize)
	}
	v := decodeACVote(t, msg)
	if len(v.Payload) == 0 || bytes.Equal(v.Payload, make([]byte, len(v.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}

func TestMakeFinalityVoteFields(t *testing.T) {
	msg := MakeFinalityVote(2, 3, 42, 5) // finalitySlot, subnet, val, origin
	if msg.Topic != FinalityVoteTopic(3) {
		t.Fatalf("topic = %q, want %q", msg.Topic, FinalityVoteTopic(3))
	}
	if msg.Slot != 2 {
		t.Fatalf("msg.Slot = %d, want 2 (finality slot rides Slot for metrics correlation)", msg.Slot)
	}
	var v pb.FinalityVote
	if err := proto.Unmarshal(msg.Payload, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v.FinalitySlot != 2 || v.Subnet != 3 || v.Val != 42 || v.Origin != 5 {
		t.Fatalf("decoded %+v, want fslot2 subnet3 val42 origin5", &v)
	}
}

func TestMakeFinalityVoteSizeAndFiller(t *testing.T) {
	msg := MakeFinalityVote(1, 0, 1, 1)
	if n := len(msg.Payload); n < FinalityVoteSize-10 || n > FinalityVoteSize+10 {
		t.Fatalf("marshaled finality vote = %d bytes, want ~%d", n, FinalityVoteSize)
	}
	var v pb.FinalityVote
	if err := proto.Unmarshal(msg.Payload, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(v.Payload) == 0 || bytes.Equal(v.Payload, make([]byte, len(v.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}

func TestFinalityAggregateSizeScales(t *testing.T) {
	// base + ceil(vps/8) bitfield, exact arithmetic on the size function.
	cases := []struct{ vps, want int }{
		{0, FinalityAggregateBase}, {1, FinalityAggregateBase + 1}, {8, FinalityAggregateBase + 1},
		{9, FinalityAggregateBase + 2}, {25000, FinalityAggregateBase + 3125},
	}
	for _, c := range cases {
		if got := FinalityAggregateSize(c.vps); got != c.want {
			t.Fatalf("FinalityAggregateSize(%d) = %d, want %d", c.vps, got, c.want)
		}
	}
}

func TestMakeFinalityAggregateFieldsAndSize(t *testing.T) {
	const vps = 25000
	msg := MakeFinalityAggregate(2, 3, 5, vps) // finalitySlot, subnet, origin, vps
	if msg.Topic != FinalityAggregateTopic {
		t.Fatalf("topic = %q, want %q", msg.Topic, FinalityAggregateTopic)
	}
	if msg.Slot != 2 {
		t.Fatalf("msg.Slot = %d, want 2", msg.Slot)
	}
	var a pb.FinalityAggregate
	if err := proto.Unmarshal(msg.Payload, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.FinalitySlot != 2 || a.Subnet != 3 || a.Origin != 5 {
		t.Fatalf("decoded %+v, want fslot2 subnet3 origin5", &a)
	}
	want := FinalityAggregateSize(vps)
	if n := len(msg.Payload); n < want-12 || n > want+12 {
		t.Fatalf("marshaled aggregate = %d bytes, want ~%d (328 + ceil(vps/8))", n, want)
	}
	// Size grows with the subnet's voting population (the scaled bitfield).
	small := len(MakeFinalityAggregate(2, 3, 5, 16).Payload)
	big := len(MakeFinalityAggregate(2, 3, 5, 100000).Payload)
	if big <= small {
		t.Fatalf("aggregate size did not scale with vps: small=%d big=%d", small, big)
	}
}

func decodeACVote(t *testing.T, msg Message) *pb.ACVote {
	t.Helper()
	var v pb.ACVote
	if err := proto.Unmarshal(msg.Payload, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &v
}
