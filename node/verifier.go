package node

import (
	"log/slog"
	"math"
	"sync"
	"time"
)

// verificationItem represents attestations submitted for batch verification. The node
// is a single CPU: the t≈4s attestation burst queues single-file here (≈ M/D/1), so
// per-subnet floods cost real per-hop time. Attestations is opaque — only its length
// (the per-item cost multiplier) matters.
type verificationItem struct {
	Slot         int
	Topic        string
	Attestations []any
}

// queuedItem pairs a submitted item with its per-submit callback.
type queuedItem struct {
	item       verificationItem
	onVerified func(verificationItem)
}

// batchVerifier pipelines attestation verification. submit() pushes items into a
// mutex-protected queue. run() loops on a timer: every batchWindow it drains up to
// maxBatchItems attestations from the queue, verifies the batch (sleeping for the
// verification delay), then loops. While one batch verifies, new items — and any capped
// leftover — accumulate in the queue for the next round, so throughput is bounded at
// maxBatchItems per window and a single batch never blocks longer than
// base + maxBatchItems·perItem.
//
// Copied from batched-attestation-sim's verifier — the batch window + base delay +
// per-item cost is the validation-as-sleep model on the per-hop path.
type batchVerifier struct {
	verificationDelay   func() time.Duration
	perAttestationDelay time.Duration
	batchWindow         time.Duration
	maxBatchItems       int // max attestations drained per batch; 0 = uncapped
	logger              *slog.Logger

	mu      sync.Mutex
	queue   []queuedItem
	notify  chan struct{} // signal that queue is non-empty
	stopped bool
	done    chan struct{}
}

func newBatchVerifier(
	verificationDelay func() time.Duration,
	perAttestationDelay time.Duration,
	batchWindow time.Duration,
	maxBatchItems int,
	logger *slog.Logger,
) *batchVerifier {
	return &batchVerifier{
		verificationDelay:   verificationDelay,
		perAttestationDelay: perAttestationDelay,
		batchWindow:         batchWindow,
		maxBatchItems:       maxBatchItems,
		logger:              logger,
		notify:              make(chan struct{}, 1),
		done:                make(chan struct{}),
	}
}

// submit enqueues attestations for batch verification. Non-blocking. onVerified is
// invoked once the containing batch finishes its verification sleep; pass nil if no
// callback is needed.
func (v *batchVerifier) submit(item verificationItem, onVerified func(verificationItem)) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.queue = append(v.queue, queuedItem{item: item, onVerified: onVerified})
	// Non-blocking signal: if notify already has a value, skip.
	select {
	case v.notify <- struct{}{}:
	default:
	}
}

// submitAndWait enqueues a verification item and blocks until the batch completes.
func (v *batchVerifier) submitAndWait(item verificationItem) {
	done := make(chan struct{})
	v.submit(item, func(verificationItem) { close(done) })
	<-done
}

// run is the pipeline loop. Call as a goroutine.
func (v *batchVerifier) run() {
	defer close(v.done)

	timer := time.NewTimer(math.MaxInt64)
	defer timer.Stop()

	timerRunning := false
	for {
		select {
		case <-v.notify:
		case <-timer.C:
			timerRunning = false
		}

		v.mu.Lock()
	OUTER:
		for {
			select {
			case <-v.notify:
			default:
				break OUTER
			}
		}
		stopped := v.stopped
		// While the batch window is still open, accumulate items in the queue without
		// draining it — the timer firing is the signal to verify.
		if timerRunning && !stopped {
			v.mu.Unlock()
			continue
		}
		queueLen := len(v.queue)
		n := batchEnd(v.queue, v.maxBatchItems)
		batch := v.queue[:n]
		if v.queue = v.queue[n:]; len(v.queue) == 0 {
			v.queue = nil
		}
		v.mu.Unlock()

		if stopped && queueLen == 0 {
			return
		}
		if queueLen == 0 {
			continue
		}
		timer.Reset(v.batchWindow)
		timerRunning = true
		v.verifyBatch(batch)
		v.mu.Lock()
		stopped = v.stopped
		drained := len(v.queue) == 0
		v.mu.Unlock()
		if stopped {
			if drained {
				return
			}
			// Capped leftover on stop: re-signal so the select above doesn't park on the
			// window timer — stopped drains run back-to-back until the queue is empty.
			select {
			case v.notify <- struct{}{}:
			default:
			}
		}
	}
}

// batchEnd returns how many queued items the next batch takes: whole items while their
// attestation total (the per-item sleep multiplier) stays within maxItems, always at
// least one. The leftover stays queued for the next window. maxItems 0 ⇒ uncapped.
func batchEnd(queue []queuedItem, maxItems int) int {
	if maxItems <= 0 {
		return len(queue)
	}
	n, atts := 0, 0
	for n < len(queue) {
		if atts += len(queue[n].item.Attestations); n > 0 && atts > maxItems {
			break
		}
		n++
	}
	return n
}

// verifyBatch simulates batch verification and dispatches each item.
func (v *batchVerifier) verifyBatch(batch []queuedItem) {
	if len(batch) == 0 {
		return
	}
	var totalAttestations int
	for _, qi := range batch {
		totalAttestations += len(qi.item.Attestations)
	}
	time.Sleep(v.verificationDelay() + time.Duration(totalAttestations)*v.perAttestationDelay)

	for _, qi := range batch {
		if qi.onVerified != nil {
			qi.onVerified(qi.item)
		}
	}
}

// stop signals the pipeline to drain remaining items and exit.
func (v *batchVerifier) stop() {
	v.mu.Lock()
	v.stopped = true
	// Wake up run() in case it's waiting on notify.
	select {
	case v.notify <- struct{}{}:
	default:
	}
	v.mu.Unlock()
	<-v.done
}
