package metrics

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/node"
)

var _ Tracer = (*SlogTracer)(nil)

// rec mirrors a SlogTracer JSON line. t_ns is int64 (UnixNano exceeds float64's
// exact-integer range, so it must not be decoded through interface{}/float64).
type rec struct {
	Msg        string `json:"msg"`
	Kind       int    `json:"kind"`
	Slot       int    `json:"slot"`
	Subnet     int    `json:"subnet"`
	Attester   int    `json:"attester"`
	Origin     int    `json:"origin"`
	Node       int    `json:"node"`
	VotedBlock bool   `json:"voted_block"`
	TNs        int64  `json:"t_ns"`
}

// SlogTracer emits one JSON object per publish/receive event, with absolute
// nanosecond timestamps (comparable across Shadow hosts, which share one clock). A
// block is the subnet/attester = -1 special case; the publish carries the vote so a
// Shadow run reassembles by (slot, subnet, attester, origin).
func TestSlogTracerEmitsJSONLines(t *testing.T) {
	var buf bytes.Buffer
	tr := NewSlogTracer(slog.NewJSONHandler(&buf, nil))

	pub := time.Unix(1_700_000_000, 111)
	arr := time.Unix(1_700_000_000, 222)
	tr.OnPublish(AttestID(3, 9, 4, 7), true, pub)
	tr.OnReceive(5, AttestID(3, 9, 4, 7), arr)

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

	wantPub := rec{
		Msg: "publish", Kind: int(node.KindAttestation), Slot: 3, Subnet: 9, Attester: 4,
		Origin: 7, VotedBlock: true, TNs: pub.UnixNano(),
	}
	if got[0] != wantPub {
		t.Errorf("publish line = %+v, want %+v", got[0], wantPub)
	}
	wantArr := rec{
		Msg: "arrival", Kind: int(node.KindAttestation), Slot: 3, Subnet: 9, Attester: 4,
		Origin: 7, Node: 5, TNs: arr.UnixNano(),
	}
	if got[1] != wantArr {
		t.Errorf("arrival line = %+v, want %+v", got[1], wantArr)
	}
}
