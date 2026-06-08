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

// MakeAggregate builds aggregate aggIdx of committee subnet's M aggregates as a publish-ready
// Message on the global aggregate topic. It is a pure function of (slot, subnet, aggIdx) with
// no aggregator identity, so every aggregator of a committee produces byte-identical messages
// that gossipsub's content-hash message-id deduplicates (the multi-source model).
func MakeAggregate(slot, subnet, aggIdx int) Message {
	agg := &pb.Aggregate{
		Slot:   uint32(slot),
		Subnet: uint32(subnet),
		AggIdx: uint32(aggIdx),
	}
	agg.Payload = sizedFiller(agg, AggregateSize)
	buf, err := proto.Marshal(agg)
	if err != nil {
		panic("MakeAggregate: marshal: " + err.Error())
	}
	return Message{Topic: AggregateTopic, Payload: buf, Slot: slot}
}
