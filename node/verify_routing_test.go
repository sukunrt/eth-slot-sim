package node

import (
	"testing"

	"github.com/ethp2p/slot-sim/validator"
)

// verifyClassFor decides which verify queue a topic routes through: the block gets the fixed
// per-hop delay, columns the width-P verifier, the existing attestation/aggregate/sync floods
// share "consensus", and the decoupled floods each get their own ("ac"/"fcvote"/"fcagg").
func TestVerifyClassFor(t *testing.T) {
	cases := []struct {
		topic string
		want  verifyClass
	}{
		{validator.BlockTopic, vcFixed},
		{"/eth2/never/registered", vcFixed},
		{validator.ColumnTopic(0), vcColumn},
		{validator.AttestationTopic(0), vcConsensus},
		{validator.AttestationTopic(63), vcConsensus},
		{validator.AggregateTopic, vcConsensus},
		{validator.SyncMessageTopic(0), vcConsensus},
		{validator.SyncContributionTopic, vcConsensus},
		{validator.AvailabilityVoteTopic, vcAC},
		{validator.FinalityVoteTopic(0), vcFCVote},
		{validator.FinalityVoteTopic(39), vcFCVote},
		{validator.FinalityAggregateTopic, vcFCAgg},
	}
	for _, c := range cases {
		if got := verifyClassFor(c.topic); got != c.want {
			t.Errorf("verifyClassFor(%q) = %q, want %q", c.topic, got, c.want)
		}
	}
}
