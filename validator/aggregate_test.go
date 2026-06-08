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
	msg := MakeAggregate(3, 7, 2) // slot, subnet, aggIdx
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
	if agg.Slot != 3 || agg.Subnet != 7 || agg.AggIdx != 2 {
		t.Fatalf("decoded %+v, want slot3 subnet7 aggIdx2", &agg)
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

// The dedup precondition: a committee's aggregators publish byte-identical messages, so the
// content-hash message-id collapses them. MakeAggregate must therefore be a pure function of
// (slot, subnet, aggIdx) — no origin, no randomness — while distinct logical aggregates
// (different aggIdx/subnet/slot) must differ on the wire.
func TestMakeAggregateByteIdentityAndDistinctness(t *testing.T) {
	a := MakeAggregate(5, 3, 1)
	if b := MakeAggregate(5, 3, 1); !bytes.Equal(a.Payload, b.Payload) {
		t.Fatal("same (slot,subnet,aggIdx) must be byte-identical (the dedup precondition)")
	}
	for _, other := range []Message{
		MakeAggregate(5, 3, 0), // different aggIdx
		MakeAggregate(5, 4, 1), // different subnet
		MakeAggregate(6, 3, 1), // different slot
	} {
		if bytes.Equal(a.Payload, other.Payload) {
			t.Fatalf("distinct aggregate collided with %+v", other)
		}
	}
}
