package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
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
	r.OnPublish(1, 0, t0)
	r.OnReceive(1, 1, 0, t0.Add(5*time.Millisecond))
	r.OnReceive(2, 1, 0, t0.Add(12*time.Millisecond))

	arr := r.Arrivals()
	if len(arr) != 2 {
		t.Fatalf("got %d arrivals, want 2", len(arr))
	}
	byNode := map[int]time.Duration{}
	for _, a := range arr {
		byNode[a.Node] = a.Delay
	}
	if byNode[1] != 5*time.Millisecond || byNode[2] != 12*time.Millisecond {
		t.Fatalf("delays = %v, want node1=5ms node2=12ms", byNode)
	}
}

// A receive with no recorded publish is skipped, not panicked on.
func TestRecorderMissingPublish(t *testing.T) {
	r := NewRecorder()
	r.OnReceive(1, 7, 0, time.Unix(1000, 0))
	if len(r.Arrivals()) != 0 {
		t.Fatalf("got %d arrivals, want 0 (no publish recorded)", len(r.Arrivals()))
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder()
	t0 := time.Unix(1000, 0)
	for slot := range 10 {
		r.OnPublish(slot, 0, t0)
	}
	var wg sync.WaitGroup
	for node := 1; node <= 50; node++ {
		wg.Go(func() {
			for slot := range 10 {
				r.OnReceive(node, slot, 0, t0.Add(time.Millisecond))
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
	r.OnPublish(1, 0, t0)
	r.OnReceive(2, 1, 0, t0.Add(3*time.Millisecond))

	var b strings.Builder
	if err := r.WriteCSV(&b); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := b.String()
	if !strings.HasPrefix(out, "node,slot,origin,delay_ms\n") {
		t.Fatalf("missing header, got:\n%s", out)
	}
	if !strings.Contains(out, "2,1,0,3\n") {
		t.Fatalf("missing data row, got:\n%s", out)
	}
}
