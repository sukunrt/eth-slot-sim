package node

import (
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// decode infers the message kind from the gossipsub topic and unmarshals it. The decoupled topics
// must each route to their Kind with fields intact — in particular finality_vote_ and
// finality_aggregate_ are distinct stems that must not be confused.
func TestDecodeDecoupled(t *testing.T) {
	at := time.Unix(1, 0)

	acRec, err := decode(validator.AvailabilityVoteTopic, validator.MakeACVote(3, 42, 5, 9).Payload, at)
	if err != nil {
		t.Fatalf("decode AC vote: %v", err)
	}
	if acRec.Kind != KindACVote {
		t.Fatalf("AC vote kind = %d, want %d", acRec.Kind, KindACVote)
	}
	if v := acRec.Obj.(*pb.ACVote); v.Slot != 3 || v.Val != 42 || v.Origin != 5 || v.VotedOrigin != 9 {
		t.Fatalf("AC vote decoded %+v, want slot3 val42 origin5 voted9", v)
	}

	fvMsg := validator.MakeFinalityVote(2, 7, 42, 5)
	fvRec, err := decode(fvMsg.Topic, fvMsg.Payload, at)
	if err != nil {
		t.Fatalf("decode finality vote: %v", err)
	}
	if fvRec.Kind != KindFinalityVote {
		t.Fatalf("finality vote kind = %d, want %d", fvRec.Kind, KindFinalityVote)
	}
	if v := fvRec.Obj.(*pb.FinalityVote); v.FinalitySlot != 2 || v.Subnet != 7 || v.Val != 42 || v.Origin != 5 {
		t.Fatalf("finality vote decoded %+v, want fslot2 subnet7 val42 origin5", v)
	}

	faRec, err := decode(validator.FinalityAggregateTopic, validator.MakeFinalityAggregate(2, 3, 5, 25000).Payload, at)
	if err != nil {
		t.Fatalf("decode finality aggregate: %v", err)
	}
	if faRec.Kind != KindFinalityAggregate {
		t.Fatalf("finality aggregate kind = %d, want %d", faRec.Kind, KindFinalityAggregate)
	}
	if a := faRec.Obj.(*pb.FinalityAggregate); a.FinalitySlot != 2 || a.Subnet != 3 || a.Origin != 5 {
		t.Fatalf("finality aggregate decoded %+v, want fslot2 subnet3 origin5", a)
	}
}
