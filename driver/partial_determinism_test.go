package driver_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
)

// Determinism under the partial transport: the same seed in two separate bubbles delivers the
// same arrival set — the seeded tick-jitter guard (the prior art's bare rand would diverge).
// Identities only, the house norm: gossipsub's mesh selection draws from the process-global
// RNG, which advances between bubbles, so relay paths (and per-arrival delays) differ even at
// a fixed seed.
func TestPartialRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	runOnce := func(t *testing.T) []string {
		var keys []string
		synctest.Test(t, func(t *testing.T) {
			a := genAssignment(16, 32, 4, 4, 2, 5, 42)
			rec := metrics.NewRecorder()
			s := buildPartialScenario(t, a, 4*time.Second, nil, rec, 6)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			s.run(t, ctx, 1)
			for _, ar := range rec.Arrivals() {
				keys = append(keys, fmt.Sprintf("%d,%d,%d,%d,%d",
					ar.Node, ar.ID.Kind, ar.ID.Slot, ar.ID.Subnet, ar.ID.Attester))
			}
			slices.Sort(keys)
		})
		return keys
	}
	var first, second []string
	t.Run("run1", func(t *testing.T) { first = runOnce(t) })
	t.Run("run2", func(t *testing.T) { second = runOnce(t) })
	if len(first) == 0 || !slices.Equal(first, second) {
		t.Fatalf("partial run not deterministic across two same-seed bubbles (%d vs %d arrivals)",
			len(first), len(second))
	}
}
