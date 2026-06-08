package validator

import (
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// AggregateTopic is the single global gossipsub topic for aggregates (mainnet
// beacon_aggregate_and_proof): every node subscribes it and downloads all aggregates.
const AggregateTopic = "/eth2/00000000/beacon_aggregate_and_proof/ssz_snappy"

// AggregateSize is the target marshaled size of one aggregate (SignedAggregateAndProof,
// ~505 B at s_c=512; slot-messages.md §5). Only that the flood carries a realistic
// per-message size is load-bearing, so the filler lands close, not exact.
const AggregateSize = 505

// MakeAggregate builds one aggregator's aggregate for committee subnet as a publish-ready
// Message on the global aggregate topic. origin is the aggregator node — it stands in for the
// aggregator's signature, making each aggregator's aggregate distinct (so gossipsub does not
// dedup them) and serving as the gossip origin for the loopback skip.
func MakeAggregate(slot, subnet, origin int) Message {
	agg := &pb.Aggregate{
		Slot:   uint32(slot),
		Subnet: uint32(subnet),
		Origin: uint32(origin),
	}
	agg.Payload = sizedFiller(agg, AggregateSize)
	buf, err := proto.Marshal(agg)
	if err != nil {
		panic("MakeAggregate: marshal: " + err.Error())
	}
	return Message{Topic: AggregateTopic, Payload: buf, Slot: slot}
}
