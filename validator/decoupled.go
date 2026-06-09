package validator

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// AvailabilityVoteTopic is the single global gossipsub topic for availability-chain votes: every
// node subscribes it and downloads all AC votes (no subnets, no committees, no aggregation), like
// the block and aggregate global topics.
const AvailabilityVoteTopic = "/eth2/00000000/availability_vote/ssz_snappy"

// FinalityVoteTopicPrefix is the per-subnet finality-vote topic stem; the subnet id and the
// ssz_snappy suffix complete it. It does NOT share a prefix with FinalityAggregateTopic
// (finality_vote_ vs finality_aggregate_), so topic dispatch needs no ordering care.
const FinalityVoteTopicPrefix = "/eth2/00000000/finality_vote_"

// FinalityAggregateTopic is the single global topic for finality-chain aggregates: every node
// subscribes it and downloads all aggregates, like sync contributions.
const FinalityAggregateTopic = "/eth2/00000000/finality_aggregate_and_proof/ssz_snappy"

// ACVoteSize is the target marshaled size of one AC vote (block_hash 32 + BLS sig 96 + selection
// proof 96 + ids ≈ 240 B). Only that the flood carries a realistic per-message size is
// load-bearing, so the filler lands close, not exact.
const ACVoteSize = 240

// FinalityVoteSize is the target marshaled size of one finality vote (att data 128 + sig 96 +
// validator id 2 = 226 B).
const FinalityVoteSize = 226

// FinalityAggregateBase is the fixed envelope of a finality aggregate (the SignedAggregateAndProof
// analogue minus the aggregation bitfield); FinalityAggregateSize adds the scaled bitfield.
const FinalityAggregateBase = 328

// FinalityVoteTopic is the gossipsub topic for finality subnet i's votes.
func FinalityVoteTopic(i int) string {
	return fmt.Sprintf("%s%d/ssz_snappy", FinalityVoteTopicPrefix, i)
}

// FinalityAggregateSize is the target marshaled size of one finality aggregate: the fixed envelope
// plus a ceil(vps/8) aggregation bitfield over the subnet's voting population vps. At vps=25 000
// (mainnet) it is 3 453 B; the shared sizedFiller's 2-byte varint holds while the payload stays
// < 16 384 B (vps ≲ 128 000). Beyond that, size the payload directly like makeBlock (the §15 caveat).
func FinalityAggregateSize(vps int) int {
	return FinalityAggregateBase + (vps+7)/8
}

// MakeACVote builds one ACVote as a publish-ready Message on the global availability-vote topic.
// origin is the publishing node (gossip origin for the loopback skip). votedOrigin is the voted
// block's origin (proposer node) when >= 0, or the prior-head sentinel when < 0 (the AC vote is
// data-availability gated, like an attestation). payload is non-zero filler sized to ≈ 240 B.
func MakeACVote(slot, val, origin, votedOrigin int) Message {
	voted := PriorHead
	if votedOrigin >= 0 {
		voted = uint32(votedOrigin)
	}
	v := &pb.ACVote{
		Slot:        uint32(slot),
		Val:         uint32(val),
		Origin:      uint32(origin),
		VotedOrigin: voted,
	}
	v.Payload = sizedFiller(v, ACVoteSize)
	buf, err := proto.Marshal(v)
	if err != nil {
		panic("MakeACVote: marshal: " + err.Error())
	}
	return Message{Topic: AvailabilityVoteTopic, Payload: buf, Slot: slot}
}

// MakeFinalityVote builds one FinalityVote as a publish-ready Message on its subnet. origin is the
// publishing member node (gossip origin for the loopback skip). The vote is dissemination-only this
// cut (un-gated, no fork-choice outcome), so it carries no voted_origin. payload is non-zero filler
// sized to ≈ 226 B. finalitySlot rides Message.Slot for metrics correlation.
func MakeFinalityVote(finalitySlot, subnet, val, origin int) Message {
	fv := &pb.FinalityVote{
		FinalitySlot: uint32(finalitySlot),
		Subnet:       uint32(subnet),
		Val:          uint32(val),
		Origin:       uint32(origin),
	}
	fv.Payload = sizedFiller(fv, FinalityVoteSize)
	buf, err := proto.Marshal(fv)
	if err != nil {
		panic("MakeFinalityVote: marshal: " + err.Error())
	}
	return Message{Topic: FinalityVoteTopic(subnet), Payload: buf, Slot: finalitySlot}
}

// MakeFinalityAggregate builds one aggregator's aggregate as a publish-ready Message on the global
// finality-aggregate topic. origin is the aggregator node — it stands in for the aggregator's
// signature, making each aggregator's aggregate distinct (so gossipsub does not dedup them) and
// serving as the gossip origin for the loopback skip. The payload is sized to scale with the
// subnet's voting population vps (FinalityAggregateSize). finalitySlot rides Message.Slot.
func MakeFinalityAggregate(finalitySlot, subnet, origin, vps int) Message {
	fa := &pb.FinalityAggregate{
		FinalitySlot: uint32(finalitySlot),
		Subnet:       uint32(subnet),
		Origin:       uint32(origin),
	}
	fa.Payload = sizedFiller(fa, FinalityAggregateSize(vps))
	buf, err := proto.Marshal(fa)
	if err != nil {
		panic("MakeFinalityAggregate: marshal: " + err.Error())
	}
	return Message{Topic: FinalityAggregateTopic, Payload: buf, Slot: finalitySlot}
}
