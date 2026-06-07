package driver

import (
	"testing"
	"time"
)

// emitDecision is the load-bearing rule of the block→attestation coupling, tested here
// purely (no network, no clock): given whether/when the block was processed and the
// deadline, it returns when to emit and whether to vote for the block. §8.3-A.
func TestEmitDecision(t *testing.T) {
	base := time.Unix(1000, 0)
	deadline := base.Add(4 * time.Second)

	cases := []struct {
		name      string
		seen      bool
		processed time.Time
		prep      time.Duration
		wantAt    time.Time
		wantBlock bool
	}{
		{"before deadline → block", true, base.Add(2 * time.Second), 0, base.Add(2 * time.Second), true},
		{"exactly at deadline → block (tie)", true, deadline, 0, deadline, true},
		{"after deadline → prior", true, base.Add(5 * time.Second), 0, deadline, false},
		{"never seen → prior at deadline", false, time.Time{}, 0, deadline, false},
		{"prep pushes under → block", true, base.Add(2500 * time.Millisecond), time.Second, base.Add(3500 * time.Millisecond), true},
		{"prep pushes over → prior", true, base.Add(3500 * time.Millisecond), time.Second, deadline, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			at, block := emitDecision(c.seen, c.processed, deadline, c.prep)
			if !at.Equal(c.wantAt) || block != c.wantBlock {
				t.Fatalf("emitDecision = (%v, %v), want (%v, %v)", at, block, c.wantAt, c.wantBlock)
			}
		})
	}
}
