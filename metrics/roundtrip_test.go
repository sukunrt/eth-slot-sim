package metrics

import (
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

// Every kind's receive-side identity (the node registry's extractor) must equal its
// publish-side metrics constructor: the Recorder joins arrivals to publishes on MsgID, so
// any skew silently orphans a whole message class. Received.Origin must carry the publisher
// even for the kinds whose identity drops it (the aggregate-likes: ID.Origin -1, publisher
// in Attester) — that asymmetry is what the loopback skip relies on.
func TestPublishReceiveIdentityRoundTrip(t *testing.T) {
	at := time.Unix(1, 0)
	blockMsg := validator.MakeBlock(3, 0, 64)

	cases := []struct {
		name   string
		msg    validator.Message
		want   MsgID
		origin int
	}{
		{"block", blockMsg, BlockID(3, 0), 0},
		{"attestation", validator.MakeAttestation(3, 7, 42, 5, 9), AttestID(3, 7, 42, 5), 5},
		{"aggregate", validator.MakeAggregate(3, 7, 5), AggregateID(3, 7, 5), 5},
		{"column", validator.MakeColumn(3, 11, 5), ColumnID(3, 11, 5), 5},
		{"sync message", validator.MakeSyncMessage(3, 7, 5, 9), SyncMessageID(3, 7, 5), 5},
		{"sync contribution", validator.MakeSyncContribution(3, 7, 5), SyncContributionID(3, 7, 5), 5},
		{"ac vote", validator.MakeACVote(3, 42, 5, 9), ACVoteID(3, 42, 5), 5},
		{"finality vote", validator.MakeFinalityVote(2, 7, 42, 5), FinalityVoteID(2, 7, 42, 5), 5},
		{"finality aggregate", validator.MakeFinalityAggregate(2, 7, 5, 25000),
			FinalityAggregateID(2, 7, 5), 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, err := node.Decode(c.msg.Topic, c.msg.Payload, at)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got := (MsgID{Kind: rec.Kind, Identity: rec.ID}); got != c.want {
				t.Errorf("receive identity %+v, want %+v", got, c.want)
			}
			if rec.Origin != c.origin {
				t.Errorf("receive origin %d, want publisher %d", rec.Origin, c.origin)
			}
		})
	}
}
