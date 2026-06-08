package node

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// The column verifier is a width-P semaphore (one per node, shared across all column topics):
// a burst of c columns serializes to ceil(c/P)·service — neither all-at-once (one CPU can't
// verify a full-custody node's 128-column t=0 burst in 3 ms) nor one-at-a-time. Mirrors the
// batched-verifier integration inequality (TestVerifierBatchDelayGrowsWithK), for the
// P-server model. synctest's fake clock makes the round count exact.
func TestColumnVerifierSerializes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const service = 3 * time.Millisecond
		measure := func(p, c int) time.Duration {
			v := newColumnVerifier(p, func() time.Duration { return service })
			start := time.Now()
			var wg sync.WaitGroup
			for range c {
				wg.Go(v.verify)
			}
			wg.Wait()
			return time.Since(start)
		}
		for _, tc := range []struct{ p, c int }{
			{16, 32}, // full-custody node: ceil(32/16)=2 rounds
			{4, 8},   // ordinary node: ceil(8/4)=2 rounds
			{4, 3},   // under one round: ceil(3/4)=1
			{1, 5},   // single server ⇒ fully serial: 5 rounds
		} {
			rounds := (tc.c + tc.p - 1) / tc.p
			want := time.Duration(rounds) * service
			if got := measure(tc.p, tc.c); got < want || got >= want+service {
				t.Fatalf("P=%d c=%d: cleared in %v, want [%v, %v) (ceil(c/P)·service)",
					tc.p, tc.c, got, want, want+service)
			}
		}
	})
}
