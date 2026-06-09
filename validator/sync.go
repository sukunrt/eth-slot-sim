package validator

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// SyncMessageTopicPrefix is the per-subnet sync-committee message topic stem; the subcommittee
// id and the ssz_snappy suffix complete it. Note SyncContributionTopic shares this prefix, so
// any topic dispatch must match the exact contribution topic before this prefix.
const SyncMessageTopicPrefix = "/eth2/00000000/sync_committee_"

// SyncContributionTopic is the single global topic for sync-committee contributions (mainnet
// sync_committee_contribution_and_proof): every node subscribes it and downloads all
// contributions, like aggregates.
const SyncContributionTopic = "/eth2/00000000/sync_committee_contribution_and_proof/ssz_snappy"

// SyncMessageSize is the target marshaled size of one SyncCommitteeMessage (144 B). Only that
// the flood carries a realistic per-message size is load-bearing, so the filler lands close.
const SyncMessageSize = 144

// SyncContributionSize is the target marshaled size of one SignedContributionAndProof (360 B).
const SyncContributionSize = 360

// SyncMessageTopic is the gossipsub topic for subcommittee subnet i's sync-committee messages.
func SyncMessageTopic(i int) string {
	return fmt.Sprintf("%s%d/ssz_snappy", SyncMessageTopicPrefix, i)
}

// MakeSyncMessage builds one SyncCommitteeMessage as a publish-ready Message on its subnet.
// origin is the publishing member node (the gossip origin for the loopback skip). votedOrigin
// is the voted block's origin (proposer node) when >= 0, or the prior-head sentinel when < 0
// (the vote is un-gated by data availability). payload is non-zero filler sized to ≈ 144 B.
func MakeSyncMessage(slot, subnet, origin, votedOrigin int) Message {
	voted := PriorHead
	if votedOrigin >= 0 {
		voted = uint32(votedOrigin)
	}
	sm := &pb.SyncMessage{
		Slot:        uint32(slot),
		Subnet:      uint32(subnet),
		Origin:      uint32(origin),
		VotedOrigin: voted,
	}
	sm.Payload = sizedFiller(sm, SyncMessageSize)
	buf, err := proto.Marshal(sm)
	if err != nil {
		panic("MakeSyncMessage: marshal: " + err.Error())
	}
	return Message{Topic: SyncMessageTopic(subnet), Payload: buf, Slot: slot}
}

// MakeSyncContribution builds one aggregator's contribution as a publish-ready Message on the
// global contribution topic. origin is the aggregator node — it stands in for the aggregator's
// signature, making each aggregator's contribution distinct (so gossipsub does not dedup them)
// and serving as the gossip origin for the loopback skip. payload is filler sized to ≈ 360 B.
func MakeSyncContribution(slot, subnet, origin int) Message {
	sc := &pb.SyncContribution{
		Slot:   uint32(slot),
		Subnet: uint32(subnet),
		Origin: uint32(origin),
	}
	sc.Payload = sizedFiller(sc, SyncContributionSize)
	buf, err := proto.Marshal(sc)
	if err != nil {
		panic("MakeSyncContribution: marshal: " + err.Error())
	}
	return Message{Topic: SyncContributionTopic, Payload: buf, Slot: slot}
}
