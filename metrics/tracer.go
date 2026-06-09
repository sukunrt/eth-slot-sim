// Package metrics records message-arrival times and summarizes them as a CDF.
// A Driver calls a Tracer on each publish/receive; the in-process Recorder keeps
// everything in memory so tests assert on it directly and the binary dumps it to CSV.
package metrics

import (
	"encoding/csv"
	"io"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ethp2p/slot-sim/node"
)

// MsgID identifies one disseminated message. A block is the special case
// {KindBlock, slot, -1, -1, origin} (no subnet/attester); an attestation is
// {KindAttestation, slot, subnet, attester, origin}; an aggregate is
// {KindAggregate, slot, subnet, aggregator, -1} (Attester carries the aggregator node; one
// distinct aggregate per aggregator). The tuple is unique per message, so arrival delay is
// recv - publish.
type MsgID struct {
	Kind                           node.Kind
	Slot, Subnet, Attester, Origin int
}

// BlockID is the MsgID for a block published by origin in slot.
func BlockID(slot, origin int) MsgID {
	return MsgID{Kind: node.KindBlock, Slot: slot, Subnet: -1, Attester: -1, Origin: origin}
}

// AttestID is the MsgID for one attestation (validator attester on subnet, published by
// origin) in slot.
func AttestID(slot, subnet, attester, origin int) MsgID {
	return MsgID{Kind: node.KindAttestation, Slot: slot, Subnet: subnet, Attester: attester, Origin: origin}
}

// AggregateID is the MsgID for the aggregate published by aggregator (a node) for subnet's
// committee in slot. Each aggregator publishes one distinct aggregate, identified by the
// aggregator — carried in the Attester field so it survives the CSV (which has no origin
// column), exactly as an attestation's attester does.
func AggregateID(slot, subnet, aggregator int) MsgID {
	return MsgID{Kind: node.KindAggregate, Slot: slot, Subnet: subnet, Attester: aggregator, Origin: -1}
}

// ColumnID is the MsgID for the data column (column/subnet index) published by origin (the
// proposer) in slot. The column index rides the Subnet field so it survives the CSV;
// Attester is unused (-1), exactly as a block's is.
func ColumnID(slot, column, origin int) MsgID {
	return MsgID{Kind: node.KindColumn, Slot: slot, Subnet: column, Attester: -1, Origin: origin}
}

// SyncMessageID is the MsgID for the sync-committee message published by member (a node) on
// subnet in slot. One message per member, so the member is the distinct identity — carried in
// the Attester field so it survives the CSV (which has no origin column), exactly as an
// aggregate's aggregator is (and unlike a column, which is one-per-subnet). It carries the
// head-vote bool via OnPublish, the sync analogue of an attestation's block vote.
func SyncMessageID(slot, subnet, member int) MsgID {
	return MsgID{Kind: node.KindSyncMessage, Slot: slot, Subnet: subnet, Attester: member, Origin: -1}
}

// SyncContributionID is the MsgID for the contribution published by aggregator (a node) for
// subnet's subcommittee in slot, on the global contribution topic. Each aggregator publishes one
// distinct contribution, identified by the aggregator carried in the Attester field (so it
// survives the CSV), exactly as an aggregate is.
func SyncContributionID(slot, subnet, aggregator int) MsgID {
	return MsgID{Kind: node.KindSyncContribution, Slot: slot, Subnet: subnet, Attester: aggregator, Origin: -1}
}

// ACVoteID is the MsgID for the availability-chain vote by validator val, published by origin in
// slot on the single global topic. It mirrors AttestID minus the subnet (there are no subnets on
// the AC), so Subnet is -1; it carries the voted-block bool via OnPublish, like an attestation.
func ACVoteID(slot, val, origin int) MsgID {
	return MsgID{Kind: node.KindACVote, Slot: slot, Subnet: -1, Attester: val, Origin: origin}
}

// FinalityVoteID is the MsgID for the finality-chain vote by validator val on subnet in finality
// slot fslot, published by origin. It mirrors AttestID (the finality slot rides Slot); one vote per
// validator, so val is the identity carried in the Attester field.
func FinalityVoteID(fslot, subnet, val, origin int) MsgID {
	return MsgID{Kind: node.KindFinalityVote, Slot: fslot, Subnet: subnet, Attester: val, Origin: origin}
}

// FinalityAggregateID is the MsgID for the aggregate published by aggregator (a node) for subnet's
// subcommittee in finality slot fslot, on the global topic. Each aggregator publishes one distinct
// aggregate, identified by the aggregator carried in the Attester field (so it survives the CSV),
// exactly as an aggregate or sync contribution is.
func FinalityAggregateID(fslot, subnet, aggregator int) MsgID {
	return MsgID{Kind: node.KindFinalityAggregate, Slot: fslot, Subnet: subnet, Attester: aggregator, Origin: -1}
}

// Tracer receives app-level publish/receive events. votedBlock is meaningful only for
// attestations (false for blocks); the receive side recovers it by joining to the
// publish record, so OnReceive needs only the identity.
type Tracer interface {
	OnPublish(id MsgID, votedBlock bool, at time.Time)
	OnReceive(rcv int, id MsgID, at time.Time)
}

// SlogTracer is a Tracer that emits one structured slog record per event. A Shadow run
// is one process per node, all sharing Shadow's virtual clock, so a run is reassembled
// from per-host logs by joining on (slot, subnet, attester, origin) with absolute
// UnixNano timestamps. The publish carries the vote.
type SlogTracer struct{ log *slog.Logger }

// NewSlogTracer returns a SlogTracer writing through h.
func NewSlogTracer(h slog.Handler) *SlogTracer { return &SlogTracer{log: slog.New(h)} }

func (t *SlogTracer) OnPublish(id MsgID, votedBlock bool, at time.Time) {
	t.log.Info("publish", "kind", int(id.Kind), "slot", id.Slot, "subnet", id.Subnet,
		"attester", id.Attester, "origin", id.Origin, "voted_block", votedBlock, "t_ns", at.UnixNano())
}

func (t *SlogTracer) OnReceive(rcv int, id MsgID, at time.Time) {
	t.log.Info("arrival", "node", rcv, "kind", int(id.Kind), "slot", id.Slot, "subnet", id.Subnet,
		"attester", id.Attester, "origin", id.Origin, "t_ns", at.UnixNano())
}

// Arrival is one node's receipt of one message. VotedBlock is carried for attestations
// (joined from the publish record).
type Arrival struct {
	Node       int
	ID         MsgID
	Delay      time.Duration // recv_time - publish_time
	VotedBlock bool
}

type pubRec struct {
	at         time.Time
	votedBlock bool
}

// Recorder is an in-memory, concurrency-safe Tracer keyed on MsgID.
type Recorder struct {
	mu       sync.Mutex
	pub      map[MsgID]pubRec
	arrivals []Arrival
	orphans  int
}

func NewRecorder() *Recorder {
	return &Recorder{pub: make(map[MsgID]pubRec)}
}

func (r *Recorder) OnPublish(id MsgID, votedBlock bool, at time.Time) {
	r.mu.Lock()
	r.pub[id] = pubRec{at: at, votedBlock: votedBlock}
	r.mu.Unlock()
}

func (r *Recorder) OnReceive(rcv int, id MsgID, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.pub[id]
	if !ok {
		r.orphans++ // count it (drops / late publishes) rather than hide the data
		return
	}
	r.arrivals = append(r.arrivals, Arrival{Node: rcv, ID: id, Delay: at.Sub(p.at), VotedBlock: p.votedBlock})
}

// Arrivals returns a copy of the recorded arrivals.
func (r *Recorder) Arrivals() []Arrival {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.arrivals)
}

// Orphans is the count of receives with no recorded publish.
func (r *Recorder) Orphans() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.orphans
}

// FractionVotedBlock is the headline attestation metric: over a slot's published attestations, the
// fraction that voted for the block (vs. the prior head). 0 if none were published.
func (r *Recorder) FractionVotedBlock(slot int) float64 {
	return r.fractionVoted(slot, node.KindAttestation)
}

// FractionVotedHead is the sync analogue: over a slot's published sync messages, the fraction that
// voted the head block. Reported next to FractionVotedBlock — sync is un-gated by data
// availability, so their difference isolates the column gate's effect on the head vote.
func (r *Recorder) FractionVotedHead(slot int) float64 {
	return r.fractionVoted(slot, node.KindSyncMessage)
}

// FractionVotedACVote is the availability-chain analogue of FractionVotedBlock: over a slot's
// published AC votes, the fraction that voted for the block (vs. the prior head). The AC vote is
// the column-gated attestation retargeted, so this is its headline data-availability metric.
func (r *Recorder) FractionVotedACVote(slot int) float64 {
	return r.fractionVoted(slot, node.KindACVote)
}

// fractionVoted is the shared core of FractionVotedBlock/FractionVotedHead/FractionVotedACVote: over
// a slot's published messages of the given kind, the fraction whose publish recorded a vote. 0 if none.
func (r *Recorder) fractionVoted(slot int, kind node.Kind) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total, voted int
	for id, p := range r.pub {
		if id.Kind != kind || id.Slot != slot {
			continue
		}
		total++
		if p.votedBlock {
			voted++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(voted) / float64(total)
}

// CustodyCompleteRate is the gate's companion to FractionVotedBlock: over a slot's custodiers,
// the share that had ALL their custody columns by the deadline `due`. Together they attribute a
// prior-head vote to a missing column (rate < 1) vs. a missing block. custody[node] is the
// node's custody column set (from schedule.View.CustodyColumns); the Recorder lacks the
// custody sets and the deadline, so they're passed in. The proposer (the slot's block origin)
// holds every column it made and counts complete without arrivals. 0 if no custodiers.
func (r *Recorder) CustodyCompleteRate(slot int, custody map[int][]int, due time.Duration) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	proposer := -1
	for id := range r.pub {
		if id.Kind == node.KindBlock && id.Slot == slot {
			proposer = id.Origin
			break
		}
	}
	arrived := map[int]map[int]bool{} // node → columns received by the deadline
	for _, a := range r.arrivals {
		if a.ID.Kind != node.KindColumn || a.ID.Slot != slot || a.Delay > due {
			continue
		}
		if arrived[a.Node] == nil {
			arrived[a.Node] = map[int]bool{}
		}
		arrived[a.Node][a.ID.Subnet] = true
	}
	var total, complete int
	for nd, cols := range custody {
		if len(cols) == 0 {
			continue
		}
		total++
		if nd == proposer {
			complete++
			continue
		}
		got := arrived[nd]
		if slices.IndexFunc(cols, func(c int) bool { return !got[c] }) < 0 {
			complete++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(complete) / float64(total)
}

// FinalityCoverageAtDeadline is the finality-chain companion to CustodyCompleteRate: over a finality
// subnet's published votes paired with its aggregators (a vote is not expected back at its own
// publisher), the share that reached the aggregator by the aggregation deadline `due`. It answers
// "how much of the subnet's vote does each aggregate capture?" — the aggregators come from the
// schedule (the Recorder lacks membership), so they're passed in. 0 if the subnet had no votes.
func (r *Recorder) FinalityCoverageAtDeadline(fslot, subnet int, aggregators []int, due time.Duration) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	type voteKey struct{ val, origin, agg int }
	var votes []MsgID
	for id := range r.pub {
		if id.Kind == node.KindFinalityVote && id.Slot == fslot && id.Subnet == subnet {
			votes = append(votes, id)
		}
	}
	arrived := map[voteKey]bool{}
	aggSet := map[int]bool{}
	for _, a := range aggregators {
		aggSet[a] = true
	}
	for _, a := range r.arrivals {
		if a.ID.Kind != node.KindFinalityVote || a.ID.Slot != fslot || a.ID.Subnet != subnet {
			continue
		}
		if aggSet[a.Node] && a.Delay <= due {
			arrived[voteKey{a.ID.Attester, a.ID.Origin, a.Node}] = true
		}
	}
	var total, covered int
	for _, v := range votes {
		for _, agg := range aggregators {
			if agg == v.Origin {
				continue // an aggregator doesn't receive its own vote (loopback)
			}
			total++
			if arrived[voteKey{v.Attester, v.Origin, agg}] {
				covered++
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(covered) / float64(total)
}

// WriteCSV dumps every arrival as node,slot,kind,subnet,attester,delay_ms,voted_block.
func (r *Recorder) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"node", "slot", "kind", "subnet", "attester", "delay_ms", "voted_block"}); err != nil {
		return err
	}
	for _, a := range r.Arrivals() {
		row := []string{
			strconv.Itoa(a.Node), strconv.Itoa(a.ID.Slot), strconv.Itoa(int(a.ID.Kind)),
			strconv.Itoa(a.ID.Subnet), strconv.Itoa(a.ID.Attester),
			strconv.FormatInt(a.Delay.Milliseconds(), 10), strconv.FormatBool(a.VotedBlock),
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
