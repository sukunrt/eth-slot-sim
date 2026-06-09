package node

import (
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// Decode infers the message kind from the gossipsub topic and unmarshals it. The decoupled topics
// must each route to their Kind with fields intact — in particular finality_vote_ and
// finality_aggregate_ are distinct stems that must not be confused. ID/Origin carry the registry's
// identity extraction (the cross-package join is pinned in metrics/roundtrip_test.go).
func TestDecodeDecoupled(t *testing.T) {
	at := time.Unix(1, 0)

	acRec, err := Decode(validator.AvailabilityVoteTopic, validator.MakeACVote(3, 42, 5, 9).Payload, at)
	if err != nil {
		t.Fatalf("decode AC vote: %v", err)
	}
	if acRec.Kind != KindACVote {
		t.Fatalf("AC vote kind = %d, want %d", acRec.Kind, KindACVote)
	}
	if v := acRec.Obj.(*pb.ACVote); v.Slot != 3 || v.Val != 42 || v.Origin != 5 || v.VotedOrigin != 9 {
		t.Fatalf("AC vote decoded %+v, want slot3 val42 origin5 voted9", v)
	}
	if want := (Identity{Slot: 3, Subnet: -1, Attester: 42, Origin: 5}); acRec.ID != want || acRec.Origin != 5 {
		t.Fatalf("AC vote identity %+v origin %d, want %+v origin 5", acRec.ID, acRec.Origin, want)
	}

	fvMsg := validator.MakeFinalityVote(2, 7, 42, 5)
	fvRec, err := Decode(fvMsg.Topic, fvMsg.Payload, at)
	if err != nil {
		t.Fatalf("decode finality vote: %v", err)
	}
	if fvRec.Kind != KindFinalityVote {
		t.Fatalf("finality vote kind = %d, want %d", fvRec.Kind, KindFinalityVote)
	}
	if v := fvRec.Obj.(*pb.FinalityVote); v.FinalitySlot != 2 || v.Subnet != 7 || v.Val != 42 || v.Origin != 5 {
		t.Fatalf("finality vote decoded %+v, want fslot2 subnet7 val42 origin5", v)
	}
	if want := (Identity{Slot: 2, Subnet: 7, Attester: 42, Origin: 5}); fvRec.ID != want || fvRec.Origin != 5 {
		t.Fatalf("finality vote identity %+v origin %d, want %+v origin 5", fvRec.ID, fvRec.Origin, want)
	}

	faRec, err := Decode(validator.FinalityAggregateTopic, validator.MakeFinalityAggregate(2, 3, 5, 25000).Payload, at)
	if err != nil {
		t.Fatalf("decode finality aggregate: %v", err)
	}
	if faRec.Kind != KindFinalityAggregate {
		t.Fatalf("finality aggregate kind = %d, want %d", faRec.Kind, KindFinalityAggregate)
	}
	if a := faRec.Obj.(*pb.FinalityAggregate); a.FinalitySlot != 2 || a.Subnet != 3 || a.Origin != 5 {
		t.Fatalf("finality aggregate decoded %+v, want fslot2 subnet3 origin5", a)
	}
	// The aggregator rides Attester with ID.Origin -1; Received.Origin still carries the publisher.
	if want := (Identity{Slot: 2, Subnet: 3, Attester: 5, Origin: -1}); faRec.ID != want || faRec.Origin != 5 {
		t.Fatalf("finality aggregate identity %+v origin %d, want %+v origin 5", faRec.ID, faRec.Origin, want)
	}
}
