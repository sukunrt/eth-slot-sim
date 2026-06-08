package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestAggregateTopicIsGlobal(t *testing.T) {
	// One global topic (mainnet beacon_aggregate_and_proof), not per-subnet.
	if want := "/eth2/00000000/beacon_aggregate_and_proof/ssz_snappy"; AggregateTopic != want {
		t.Fatalf("AggregateTopic = %q, want %q", AggregateTopic, want)
	}
}

func TestMakeAggregateFields(t *testing.T) {
	msg := MakeAggregate(3, 7, 2) // slot, subnet, origin (aggregator node)
	if msg.Topic != AggregateTopic {
		t.Fatalf("topic = %q, want %q", msg.Topic, AggregateTopic)
	}
	if msg.Slot != 3 {
		t.Fatalf("msg.Slot = %d, want 3", msg.Slot)
	}
	var agg pb.Aggregate
	if err := proto.Unmarshal(msg.Payload, &agg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if agg.Slot != 3 || agg.Subnet != 7 || agg.Origin != 2 {
		t.Fatalf("decoded %+v, want slot3 subnet7 origin2", &agg)
	}
}

func TestMakeAggregateSizeAndFiller(t *testing.T) {
	msg := MakeAggregate(1, 0, 0)
	// SignedAggregateAndProof ~505 B; size the filler to land close (exact wire total
	// isn't load-bearing, only that the flood carries a realistic per-message size).
	if n := len(msg.Payload); n < 490 || n > 520 {
		t.Fatalf("marshaled aggregate = %d bytes, want ~505", n)
	}
	var agg pb.Aggregate
	if err := proto.Unmarshal(msg.Payload, &agg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(agg.Payload) == 0 || bytes.Equal(agg.Payload, make([]byte, len(agg.Payload))) {
		t.Fatal("payload empty or all-zero; proto3 would drop it")
	}
}

// Each aggregator's aggregate is distinct because origin (its key) differs — so gossipsub
// does NOT dedup them (16 aggregators ⇒ 16 distinct messages). Same (slot, subnet, origin) is
// deterministic; a different aggregator, subnet, or slot yields different bytes.
func TestMakeAggregateDistinctPerAggregator(t *testing.T) {
	a := MakeAggregate(5, 3, 1)
	if b := MakeAggregate(5, 3, 1); !bytes.Equal(a.Payload, b.Payload) {
		t.Fatal("same (slot,subnet,origin) must be deterministic")
	}
	for _, other := range []Message{
		MakeAggregate(5, 3, 2), // different aggregator
		MakeAggregate(5, 4, 1), // different subnet
		MakeAggregate(6, 3, 1), // different slot
	} {
		if bytes.Equal(a.Payload, other.Payload) {
			t.Fatalf("aggregate collided with %+v (each aggregator's must be distinct)", other)
		}
	}
}
