// Package metrics records block-arrival times and summarizes them as a CDF.
// The Node calls a Tracer on each publish/receive; the in-process Recorder
// keeps everything in memory so tests assert on it directly and the binary
// dumps it to CSV.
package metrics

import (
	"encoding/csv"
	"io"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"
)

// Tracer receives app-level publish/receive events. The Node owns nothing about
// "block" — it just reports (slot, origin) and timestamps.
type Tracer interface {
	OnPublish(slot, origin int, at time.Time)
	OnReceive(node, slot, origin int, at time.Time)
}

// Arrival is one node's receipt of one block.
type Arrival struct {
	Node, Slot, Origin int
	Delay              time.Duration // recv_time - publish_time
}

type pubKey struct{ slot, origin int }

// Recorder is an in-memory, concurrency-safe Tracer. One proposer per slot means
// (slot, origin) uniquely identifies a block, so arrival delay is recv - publish.
type Recorder struct {
	mu       sync.Mutex
	pub      map[pubKey]time.Time
	arrivals []Arrival
}

func NewRecorder() *Recorder {
	return &Recorder{pub: make(map[pubKey]time.Time)}
}

func (r *Recorder) OnPublish(slot, origin int, at time.Time) {
	r.mu.Lock()
	r.pub[pubKey{slot, origin}] = at
	r.mu.Unlock()
}

func (r *Recorder) OnReceive(node, slot, origin int, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pubAt, ok := r.pub[pubKey{slot, origin}]
	if !ok {
		return // publish not recorded (shouldn't happen in-process); skip rather than guess
	}
	r.arrivals = append(r.arrivals, Arrival{node, slot, origin, at.Sub(pubAt)})
}

// Arrivals returns a copy of the recorded arrivals.
func (r *Recorder) Arrivals() []Arrival {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.arrivals)
}

// WriteCSV dumps every arrival as node,slot,origin,delay_ms rows.
func (r *Recorder) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"node", "slot", "origin", "delay_ms"}); err != nil {
		return err
	}
	for _, a := range r.Arrivals() {
		row := []string{
			strconv.Itoa(a.Node), strconv.Itoa(a.Slot), strconv.Itoa(a.Origin),
			strconv.FormatInt(a.Delay.Milliseconds(), 10),
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// Summary holds the headline arrival percentiles. p99–p100 is the number that
// matters for slot-timing decisions.
type Summary struct {
	Count               int
	P50, P90, P99, P100 time.Duration
}

// Summarize computes p50/p90/p99/p100 over the delays.
func Summarize(delays []time.Duration) Summary {
	sorted := slices.Clone(delays)
	slices.Sort(sorted)
	return Summary{
		Count: len(sorted),
		P50:   percentile(sorted, 50),
		P90:   percentile(sorted, 90),
		P99:   percentile(sorted, 99),
		P100:  percentile(sorted, 100),
	}
}

// percentile returns the p-th percentile of an already-sorted slice using the
// nearest-rank method: rank = ceil(p/100 * n), 1-indexed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := max(int(math.Ceil(p/100*float64(len(sorted)))), 1)
	return sorted[rank-1]
}
