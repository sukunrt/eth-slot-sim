package metrics

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"
)

var _ Tracer = (*SlogTracer)(nil)

// rec mirrors a SlogTracer JSON line. t_ns is int64 (UnixNano exceeds float64's
// exact-integer range, so it must not be decoded through interface{}/float64).
type rec struct {
	Msg    string `json:"msg"`
	Slot   int    `json:"slot"`
	Origin int    `json:"origin"`
	Node   int    `json:"node"`
	TNs    int64  `json:"t_ns"`
}

// SlogTracer emits one JSON object per publish/receive event, with absolute
// nanosecond timestamps (comparable across Shadow hosts, which share one clock).
func TestSlogTracerEmitsJSONLines(t *testing.T) {
	var buf bytes.Buffer
	tr := NewSlogTracer(slog.NewJSONHandler(&buf, nil))

	pub := time.Unix(1_700_000_000, 111)
	arr := time.Unix(1_700_000_000, 222)
	tr.OnPublish(3, 7, pub)
	tr.OnReceive(5, 3, 7, arr)

	dec := json.NewDecoder(&buf)
	var got []rec
	for dec.More() {
		var r rec
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}

	wantPub := rec{Msg: "publish", Slot: 3, Origin: 7, TNs: pub.UnixNano()}
	if got[0] != wantPub {
		t.Errorf("publish line = %+v, want %+v", got[0], wantPub)
	}
	wantArr := rec{Msg: "arrival", Node: 5, Slot: 3, Origin: 7, TNs: arr.UnixNano()}
	if got[1] != wantArr {
		t.Errorf("arrival line = %+v, want %+v", got[1], wantArr)
	}
}
