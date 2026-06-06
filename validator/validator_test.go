package validator

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func newRNG() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestDutiesProposerSelection(t *testing.T) {
	v := New(2, 4, 1024, 0, 0, newRNG())
	for slot := range 10 {
		duties := v.Duties(slot)
		want := slot%4 == 2
		if got := len(duties) == 1; got != want {
			t.Fatalf("slot %d: got %d duties, want proposer=%v", slot, len(duties), want)
		}
	}
}

func TestDutiesAt(t *testing.T) {
	// jitter=0 -> At is exactly offset.
	v := New(0, 1, 1024, 500*time.Millisecond, 0, newRNG())
	if at := v.Duties(0)[0].At; at != 500*time.Millisecond {
		t.Fatalf("jitter=0: At=%v, want 500ms", at)
	}

	// jitter>0 -> At in [offset, offset+jitter).
	v = New(0, 1, 1024, time.Second, 2*time.Second, newRNG())
	for slot := range 100 {
		at := v.Duties(slot)[0].At
		if at < time.Second || at >= 3*time.Second {
			t.Fatalf("slot %d: At=%v out of [1s,3s)", slot, at)
		}
	}
}

func TestDutiesMessage(t *testing.T) {
	v := New(3, 8, 4096, 0, 0, newRNG())
	d := v.Duties(3)[0]
	if d.Msg.Topic != BlockTopic {
		t.Fatalf("topic=%q, want %q", d.Msg.Topic, BlockTopic)
	}
	if d.Msg.Slot != 3 {
		t.Fatalf("slot=%d, want 3", d.Msg.Slot)
	}
	var blk pb.Block
	if err := proto.Unmarshal(d.Msg.Payload, &blk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if blk.Origin != 3 || blk.Slot != 3 {
		t.Fatalf("block origin=%d slot=%d, want 3/3", blk.Origin, blk.Slot)
	}
}

func TestMakeBlockSizeExact(t *testing.T) {
	// Cover the varint length-prefix boundary at 16384 and the 128 KiB default.
	targets := []int{64, 200, 16383, 16384, 16385, 131072, 1<<20 - 100}
	for _, target := range targets {
		for _, sl := range []int{0, 1, 9999, 1 << 20} {
			msg := makeBlock(sl, 7, target)
			if len(msg.Payload) != target {
				t.Fatalf("target=%d slot=%d: marshaled %d bytes, want %d",
					target, sl, len(msg.Payload), target)
			}
			var blk pb.Block
			if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
				t.Fatalf("target=%d: unmarshal: %v", target, err)
			}
			if int(blk.Slot) != sl || blk.Origin != 7 {
				t.Fatalf("target=%d: decoded slot=%d origin=%d", target, blk.Slot, blk.Origin)
			}
		}
	}
}

func TestMakeBlockPayloadRandom(t *testing.T) {
	msg := makeBlock(5, 1, 4096)
	var blk pb.Block
	if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if bytes.Equal(blk.Payload, make([]byte, len(blk.Payload))) {
		t.Fatal("payload is all zeros; want random filler")
	}
	// Deterministic: same slot -> same filler.
	msg2 := makeBlock(5, 1, 4096)
	if !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatal("makeBlock not deterministic for same slot")
	}
}
