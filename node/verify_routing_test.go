package node

import (
	"testing"

	"github.com/ethp2p/slot-sim/validator"
)

// batchedTopic decides which topics route through the per-node batched verifier — the t≈4s
// attestation flood and the t≈8s aggregate flood — vs the fixed per-hop delay (the block,
// one per slot).
func TestBatchedTopic(t *testing.T) {
	cases := []struct {
		topic string
		want  bool
	}{
		{validator.BlockTopic, false},
		{validator.AttestationTopic(0), true},
		{validator.AttestationTopic(63), true},
		{validator.AggregateTopic, true},
	}
	for _, c := range cases {
		if got := batchedTopic(c.topic); got != c.want {
			t.Errorf("batchedTopic(%q) = %v, want %v", c.topic, got, c.want)
		}
	}
}
