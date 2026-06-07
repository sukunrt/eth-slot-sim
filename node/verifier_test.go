package node

import (
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// M3: the batched verifier ported from ../batched-attestation-sim — it models the t≈4s
// attestation flood as a single-server (M/D/1) CPU queue on the per-hop path. The test
// suite IS the component spec; ported to stdlib asserts (the repo is testify-free).

// recordingSink collects items the verifier marks verified; concurrency-safe.
type recordingSink struct {
	mu    sync.Mutex
	items []verificationItem
}

func (r *recordingSink) callback() func(verificationItem) {
	return func(item verificationItem) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.items = append(r.items, item)
	}
}

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func (r *recordingSink) totalAttestations() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	for _, it := range r.items {
		n += len(it.Attestations)
	}
	return n
}

// fixedDelay records how many times the delay func was called (once per batch).
type fixedDelay struct {
	mu    sync.Mutex
	delay time.Duration
	calls int
}

func (f *fixedDelay) fn() func() time.Duration {
	return func() time.Duration {
		f.mu.Lock()
		f.calls++
		f.mu.Unlock()
		return f.delay
	}
}

func (f *fixedDelay) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestVerifier(t *testing.T, delay func() time.Duration, perAtt, window time.Duration) *batchVerifier {
	t.Helper()
	v := newBatchVerifier(delay, perAtt, window, slog.Default())
	go v.run()
	t.Cleanup(func() { v.stop() })
	return v
}

func TestVerifierSingleItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 20 * time.Millisecond }, 0, 5*time.Millisecond)

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1, 2, 3}}, sink.callback())
		time.Sleep(50 * time.Millisecond)

		if got := sink.count(); got != 1 {
			t.Fatalf("batches dispatched = %d, want 1", got)
		}
		if got := sink.totalAttestations(); got != 3 {
			t.Fatalf("attestations = %d, want 3", got)
		}
	})
}

func TestVerifierSubmitAndWaitBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := newTestVerifier(t, func() time.Duration { return 10 * time.Millisecond }, 0, 5*time.Millisecond)

		start := time.Now()
		v.submitAndWait(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1}})

		if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
			t.Fatalf("submitAndWait returned after %v, want >= 10ms (cost on the path)", elapsed)
		}
	})
}

func TestVerifierBatchesWithinWindow(t *testing.T) {
	// Items arriving while the batch window is open dispatch together; with
	// verifyDelay << window the first submission gets its own batch and the
	// followers form the next.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		delay := &fixedDelay{delay: 5 * time.Millisecond}
		v := newTestVerifier(t, delay.fn(), 0, 50*time.Millisecond)

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		time.Sleep(10 * time.Millisecond) // verifyBatch finished, window still open
		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{2}}, sink.callback())
		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{3}}, sink.callback())
		time.Sleep(100 * time.Millisecond)

		if got := sink.totalAttestations(); got != 3 {
			t.Fatalf("attestations = %d, want 3", got)
		}
		if got := delay.callCount(); got != 2 {
			t.Fatalf("batches = %d, want 2 (first item + window-batched pair)", got)
		}
	})
}

func TestVerifierItemsDuringVerifyAreQueued(t *testing.T) {
	// Items submitted while a batch is verifying (verifyDelay > window) must not be
	// dropped — the run loop must accumulate, not drain-and-drop, while timerRunning.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 30 * time.Millisecond }, 0, 5*time.Millisecond)

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		time.Sleep(10 * time.Millisecond) // mid-verification
		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{2}}, sink.callback())
		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{3}}, sink.callback())
		time.Sleep(200 * time.Millisecond)

		if got := sink.totalAttestations(); got != 3 {
			t.Fatalf("attestations = %d, want 3 (none dropped)", got)
		}
	})
}

func TestVerifierManyItemsNoneDropped(t *testing.T) {
	// The t≈4s flood in a test tube: a burst from many goroutines, all verified.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 5 * time.Millisecond }, 0, 5*time.Millisecond)

		const n = 50
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{i}}, sink.callback())
			})
		}
		wg.Wait()
		time.Sleep(500 * time.Millisecond)

		if got := sink.totalAttestations(); got != n {
			t.Fatalf("attestations = %d, want %d (none dropped)", got, n)
		}
	})
}

func TestVerifierStopDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newBatchVerifier(func() time.Duration { return 5 * time.Millisecond }, 0, 2*time.Millisecond, slog.Default())
		go v.run()

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1, 2}}, sink.callback())
		v.stop() // blocks until queued items are validated

		if got := sink.totalAttestations(); got != 2 {
			t.Fatalf("attestations = %d, want 2 (drained on stop)", got)
		}
	})
}

func TestVerifierStopWithoutWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newBatchVerifier(func() time.Duration { return 5 * time.Millisecond }, 0, 2*time.Millisecond, slog.Default())
		go v.run()
		v.stop() // idle verifier returns immediately (the busy-spin guard)
		if got := sink.count(); got != 0 {
			t.Fatalf("count = %d, want 0", got)
		}
	})
}

func TestVerifierPerAttestationDelayCounted(t *testing.T) {
	// Each batch invokes verificationDelay() exactly once; the per-attestation cost is
	// N * perAttDelay, not N * delay().
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		delay := &fixedDelay{delay: 10 * time.Millisecond}
		v := newTestVerifier(t, delay.fn(), 1*time.Millisecond, 5*time.Millisecond)

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1, 2, 3}}, sink.callback())
		time.Sleep(50 * time.Millisecond)
		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{4, 5}}, sink.callback())
		time.Sleep(50 * time.Millisecond)

		if got := delay.callCount(); got != 2 {
			t.Fatalf("delay calls = %d, want 2 (one per batch)", got)
		}
		if got := sink.totalAttestations(); got != 5 {
			t.Fatalf("attestations = %d, want 5", got)
		}
	})
}

func TestVerifierMultipleTopicsAndSlots(t *testing.T) {
	// Items from different (topic, slot) tuples in one batch dispatch separately with
	// their Topic/Slot intact.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 10 * time.Millisecond }, 0, 5*time.Millisecond)

		v.submit(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		v.submit(verificationItem{Topic: "t0", Slot: 2, Attestations: []any{2}}, sink.callback())
		v.submit(verificationItem{Topic: "t1", Slot: 1, Attestations: []any{3}}, sink.callback())
		time.Sleep(50 * time.Millisecond)

		seen := map[string]int{}
		sink.mu.Lock()
		for _, it := range sink.items {
			seen[it.Topic+":"+strconv.Itoa(it.Slot)] += len(it.Attestations)
		}
		sink.mu.Unlock()

		want := map[string]int{"t0:1": 1, "t0:2": 1, "t1:1": 1}
		if len(seen) != len(want) {
			t.Fatalf("seen = %v, want %v", seen, want)
		}
		for k, v := range want {
			if seen[k] != v {
				t.Fatalf("seen[%q] = %d, want %d (seen=%v)", k, seen[k], v, seen)
			}
		}
	})
}

func TestVerifierSubmitAndWaitMultipleConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := newTestVerifier(t, func() time.Duration { return 15 * time.Millisecond }, 0, 5*time.Millisecond)

		var wg sync.WaitGroup
		var mu sync.Mutex
		var done [4]bool
		for i := range 4 {
			wg.Go(func() {
				v.submitAndWait(verificationItem{Topic: "t0", Slot: 1, Attestations: []any{i}})
				mu.Lock()
				done[i] = true
				mu.Unlock()
			})
		}
		wg.Wait()

		for i, d := range done {
			if !d {
				t.Fatalf("waiter %d did not unblock", i)
			}
		}
	})
}

func TestVerifierStopIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := newBatchVerifier(func() time.Duration { return time.Millisecond }, 0, time.Millisecond, slog.Default())
		go v.run()
		v.stop() // single stop must not panic
	})
}

// The integration inequality (§8.4): a batch's release delay grows with the number of
// attestations in it (base + k·perItem), proving the CPU queue is on the path.
func TestVerifierBatchDelayGrowsWithK(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const base, perAtt = 10 * time.Millisecond, 2 * time.Millisecond
		measure := func(k int) time.Duration {
			v := newTestVerifier(t, func() time.Duration { return base }, perAtt, 5*time.Millisecond)
			atts := make([]any, k)
			start := time.Now()
			v.submitAndWait(verificationItem{Topic: "t0", Slot: 1, Attestations: atts})
			return time.Since(start)
		}
		d1, d5 := measure(1), measure(5)
		if d5 <= d1 {
			t.Fatalf("D5 (%v) <= D1 (%v): queue not on the path", d5, d1)
		}
		if want := base + 5*perAtt; d5 < want {
			t.Fatalf("D5 = %v, want >= base+5*perItem = %v", d5, want)
		}
	})
}
