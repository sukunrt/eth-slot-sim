package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestMakeBlockMessage(t *testing.T) {
	msg := MakeBlock(3, 7, 4096)
	if msg.Topic != BlockTopic {
		t.Fatalf("topic=%q, want %q", msg.Topic, BlockTopic)
	}
	if msg.Slot != 3 {
		t.Fatalf("slot=%d, want 3", msg.Slot)
	}
	var blk pb.Block
	if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if blk.Origin != 7 || blk.Slot != 3 {
		t.Fatalf("block origin=%d slot=%d, want 7/3", blk.Origin, blk.Slot)
	}
}

func TestMakeBlockSize(t *testing.T) {
	for _, size := range []int{0, 200, 16384, 131072} {
		for _, sl := range []int{0, 1, 9999, 1 << 20} {
			msg := MakeBlock(sl, 7, size)
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
	msg := MakeBlock(5, 1, 4096)
	var blk pb.Block
	if err := proto.Unmarshal(msg.Payload, &blk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if bytes.Equal(blk.Payload, make([]byte, len(blk.Payload))) {
		t.Fatal("payload is all zeros; proto3 would drop it")
	}
}
