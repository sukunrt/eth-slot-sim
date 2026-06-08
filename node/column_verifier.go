package node

import "time"

// columnVerifier models a node's per-core column validation as a width-P semaphore (one per
// node, shared across all column topics): each arriving DataColumnSidecar acquires one of P
// verify slots, sleeps the per-column service time (validation-as-sleep, ~3 ms for the KZG
// cell-proof batch), then releases — so a burst of c columns clears in ceil(c/P)·service.
//
// This is deliberately NOT the attestation batchVerifier (a batch-window accumulator for the
// t≈4 s flood): columns arrive in a bursty t=0 wave and are verified per-core, not batched.
// P is sized from the node's role (16 for a full-custody node — it relays the most — 4
// otherwise), so the backbone's serialization sets the column-arrival tail.
type columnVerifier struct {
	sema    chan struct{}
	service func() time.Duration
}

// newColumnVerifier builds a width-p column verifier. p is the node's parallel verify slots
// (clamped to ≥1); service is the per-column cost (nil-safe via the caller's default).
func newColumnVerifier(p int, service func() time.Duration) *columnVerifier {
	if p < 1 {
		p = 1
	}
	return &columnVerifier{sema: make(chan struct{}, p), service: service}
}

// verify blocks until one of the P slots is free, sleeps the service time, then frees the
// slot. Called from the column topic validator hook; gossipsub runs validators concurrently,
// so a burst of more than P columns serializes through the P slots.
func (v *columnVerifier) verify() {
	v.sema <- struct{}{}
	time.Sleep(v.service())
	<-v.sema
}
