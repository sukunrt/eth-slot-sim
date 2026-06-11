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
	v := NewProposer(2, 4, 1024, 0, 0, newRNG(), nil)
	for slot := range 10 {
		duties := v.BlocksToPublish(slot)
		want := slot%4 == 2
		if got := len(duties) == 1; got != want {
			t.Fatalf("slot %d: got %d duties, want proposer=%v", slot, len(duties), want)
		}
	}
}

func TestDutiesProposerFromSchedule(t *testing.T) {
	// A non-nil schedule decides the proposer, not slot%n: node 2 proposes only on the
	// slots the schedule names (0 and 2 here), and not on slot 1.
	sched := []int{2, 3, 2}
	v := NewProposer(2, 4, 1024, 0, 0, newRNG(), sched)
	for slot := range len(sched) {
		want := sched[slot] == 2
		if got := len(v.BlocksToPublish(slot)) == 1; got != want {
			t.Fatalf("slot %d: proposer=%v, want %v", slot, got, want)
		}
	}
}

func TestDutiesAt(t *testing.T) {
	// jitter=0 -> At is exactly offset.
	v := NewProposer(0, 1, 1024, 500*time.Millisecond, 0, newRNG(), nil)
	if at := v.BlocksToPublish(0)[0].At; at != 500*time.Millisecond {
		t.Fatalf("jitter=0: At=%v, want 500ms", at)
	}

	// jitter>0 -> At in [offset, offset+jitter).
	v = NewProposer(0, 1, 1024, time.Second, 2*time.Second, newRNG(), nil)
	for slot := range 100 {
		at := v.BlocksToPublish(slot)[0].At
		if at < time.Second || at >= 3*time.Second {
			t.Fatalf("slot %d: At=%v out of [1s,3s)", slot, at)
		}
	}
}

func TestDutiesMessage(t *testing.T) {
	v := NewProposer(3, 8, 4096, 0, 0, newRNG(), nil)
	d := v.BlocksToPublish(3)[0]
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

func TestMakeBlockSize(t *testing.T) {
	for _, size := range []int{0, 200, 16384, 131072} {
		for _, sl := range []int{0, 1, 9999, 1 << 20} {
			msg := makeBlock(sl, 7, size)
			var blk pb.Block
			if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
				t.Fatalf("size=%d: unmarshal: %v", size, err)
			}
			if len(blk.Payload) != size {
				t.Fatalf("size=%d slot=%d: payload %d bytes, want %d", size, sl, len(blk.Payload), size)
			}
			if int(blk.Slot) != sl || blk.Origin != 7 {
				t.Fatalf("size=%d: decoded slot=%d origin=%d", size, blk.Slot, blk.Origin)
			}
		}
	}
}

func TestMakeBlockPayloadNonZero(t *testing.T) {
	msg := makeBlock(5, 1, 4096)
	var blk pb.Block
	if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if bytes.Equal(blk.Payload, make([]byte, len(blk.Payload))) {
		t.Fatal("payload is all zeros; proto3 would drop it")
	}
}
