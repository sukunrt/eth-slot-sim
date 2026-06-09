package node

import (
	"testing"

	"github.com/ethp2p/slot-sim/validator"
)

// batchClass decides which single-server queue a topic's verification routes through: the existing
// attestation/aggregate/sync floods share "consensus"; the decoupled floods each get their own
// ("ac"/"fcvote"/"fcagg"); the block and anything else get "" (fixed per-hop / column verifier).
func TestBatchClass(t *testing.T) {
	cases := []struct {
		topic string
		want  string
	}{
		{validator.BlockTopic, ""},
		{validator.ColumnTopic(0), ""},
		{validator.AttestationTopic(0), "consensus"},
		{validator.AttestationTopic(63), "consensus"},
		{validator.AggregateTopic, "consensus"},
		{validator.SyncMessageTopic(0), "consensus"},
		{validator.SyncContributionTopic, "consensus"},
		{validator.AvailabilityVoteTopic, "ac"},
		{validator.FinalityVoteTopic(0), "fcvote"},
		{validator.FinalityVoteTopic(39), "fcvote"},
		{validator.FinalityAggregateTopic, "fcagg"},
	}
	for _, c := range cases {
		if got := batchClass(c.topic); got != c.want {
			t.Errorf("batchClass(%q) = %q, want %q", c.topic, got, c.want)
		}
	}
}
