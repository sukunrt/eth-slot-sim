package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/node"
)

func TestPercentile(t *testing.T) {
	delays := make([]time.Duration, 100)
	for i := range delays {
		delays[i] = time.Duration(i+1) * time.Millisecond // 1ms .. 100ms
	}
	s := Summarize(delays)
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"p50", s.P50, 50 * time.Millisecond},
		{"p90", s.P90, 90 * time.Millisecond},
		{"p99", s.P99, 99 * time.Millisecond},
		{"p100", s.P100, 100 * time.Millisecond},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if s.Count != 100 {
		t.Errorf("count = %d, want 100", s.Count)
	}
}

func TestRecorderDelay(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	r.OnPublish(BlockID(1, 0), false, t0)
	r.OnReceive(1, BlockID(1, 0), t0.Add(5*time.Millisecond))
	r.OnReceive(2, BlockID(1, 0), t0.Add(12*time.Millisecond))

	arr := r.Arrivals()
	if len(arr) != 2 {
		t.Fatalf("got %d arrivals, want 2", len(arr))
	}
	byNode := map[int]time.Duration{}
	for _, a := range arr {
		if a.ID.Kind != node.KindBlock || a.ID.Slot != 1 || a.ID.Origin != 0 {
			t.Fatalf("arrival id = %+v, want block slot1 origin0", a.ID)
		}
		byNode[a.Node] = a.Delay
	}
	if byNode[1] != 5*time.Millisecond || byNode[2] != 12*time.Millisecond {
		t.Fatalf("delays = %v, want node1=5ms node2=12ms", byNode)
	}
}

// An attestation arrival carries its identity (subnet, attester) and the publisher's
// vote (joined from the publish record).
func TestRecorderAttestationVote(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	r.OnPublish(AttestID(1, 3, 7, 5), true, t0) // slot1 subnet3 attester7 origin5, voted block
	r.OnReceive(9, AttestID(1, 3, 7, 5), t0.Add(2*time.Millisecond))

	arr := r.Arrivals()
	if len(arr) != 1 {
		t.Fatalf("got %d arrivals, want 1", len(arr))
	}
	a := arr[0]
	if a.ID.Kind != node.KindAttestation || a.ID.Subnet != 3 || a.ID.Attester != 7 || a.ID.Origin != 5 {
		t.Fatalf("arrival id = %+v, want attestation subnet3 attester7 origin5", a.ID)
	}
	if !a.VotedBlock {
		t.Fatal("VotedBlock = false, want true (joined from publish)")
	}
}

// A receive with no recorded publish is counted as an orphan, not silently dropped —
// under drops or late publishes this can happen and would otherwise hide data.
func TestRecorderOrphanCounted(t *testing.T) {
	r := NewRecorder()
	r.OnReceive(1, BlockID(7, 0), time.Unix(1000, 0))
	if got := len(r.Arrivals()); got != 0 {
		t.Fatalf("got %d arrivals, want 0", got)
	}
	if got := r.Orphans(); got != 1 {
		t.Fatalf("orphans = %d, want 1", got)
	}
}

// FractionVotedBlock is the headline metric: over a slot's published attestations, the
// share that voted for the block.
func TestRecorderFractionVotedBlock(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	// 3 of 4 attesters in slot 1 voted block; a block publish must not count.
	r.OnPublish(AttestID(1, 0, 0, 0), true, t0)
	r.OnPublish(AttestID(1, 0, 1, 1), true, t0)
	r.OnPublish(AttestID(1, 0, 2, 2), true, t0)
	r.OnPublish(AttestID(1, 0, 3, 3), false, t0)
	r.OnPublish(BlockID(1, 0), false, t0)
	if got := r.FractionVotedBlock(1); got != 0.75 {
		t.Fatalf("FractionVotedBlock(1) = %v, want 0.75", got)
	}
}

// An aggregate's identity is (slot, subnet, aggregator) — the aggregator node (in Attester)
// makes each aggregator's aggregate distinct (no dedup). Delay joins to the publish record.
func TestRecorderAggregate(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	id := AggregateID(2, 5, 7) // slot2 subnet5 aggregator-node7
	if want := (MsgID{Kind: node.KindAggregate, Slot: 2, Subnet: 5, Attester: 7, Origin: -1}); id != want {
		t.Fatalf("AggregateID = %+v, want %+v", id, want)
	}
	r.OnPublish(id, false, t0)
	r.OnReceive(9, id, t0.Add(4*time.Millisecond))

	arr := r.Arrivals()
	if len(arr) != 1 {
		t.Fatalf("got %d arrivals, want 1", len(arr))
	}
	if a := arr[0]; a.ID.Kind != node.KindAggregate || a.ID.Subnet != 5 ||
		a.ID.Attester != 7 || a.Delay != 4*time.Millisecond {
		t.Fatalf("arrival = %+v, want aggregate subnet5 aggregator7 delay4ms", a)
	}
	if r.Orphans() != 0 {
		t.Fatalf("orphans = %d, want 0", r.Orphans())
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	for slot := range 10 {
		r.OnPublish(BlockID(slot, 0), false, t0)
	}
	var wg sync.WaitGroup
	for nd := 1; nd <= 50; nd++ {
		wg.Go(func() {
			for slot := range 10 {
				r.OnReceive(nd, BlockID(slot, 0), t0.Add(time.Millisecond))
			}
		})
	}
	wg.Wait()
	if got := len(r.Arrivals()); got != 500 {
		t.Fatalf("got %d arrivals, want 500", got)
	}
}

func TestWriteCSV(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	r.OnPublish(AttestID(1, 3, 7, 0), true, t0)
	r.OnReceive(2, AttestID(1, 3, 7, 0), t0.Add(3*time.Millisecond))

	var b strings.Builder
	if err := r.WriteCSV(&b); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := b.String()
	if !strings.HasPrefix(out, "node,slot,kind,subnet,attester,delay_ms,voted_block\n") {
		t.Fatalf("missing/!wrong header, got:\n%s", out)
	}
	// node2 slot1 kind2(attestation) subnet3 attester7 delay3 voted_block true
	if !strings.Contains(out, "2,1,2,3,7,3,true\n") {
		t.Fatalf("missing data row, got:\n%s", out)
	}
}
