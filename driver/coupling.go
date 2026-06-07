package driver

import "time"

// emitDecision is the block→attestation coupling rule: emit at
// min(block_processed + prep, deadline), voting for the block iff it was processed
// (plus prep) by the deadline, else for the prior head. A tie exactly at the deadline
// votes block (the `<=` boundary). Pure — the only place the rule is decided, so the
// receipt path and the deadline timer both call it and agree regardless of which fires
// first.
func emitDecision(seen bool, processed, deadline time.Time, prep time.Duration) (emitAt time.Time, voteBlock bool) {
	if seen {
		if ready := processed.Add(prep); !ready.After(deadline) { // ready <= deadline
			return ready, true
		}
	}
	return deadline, false
}
