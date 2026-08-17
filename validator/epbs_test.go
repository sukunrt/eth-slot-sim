package validator

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

func TestMakeConsensusBlockMessage(t *testing.T) {
	msg := MakeConsensusBlock(3, 7, 2048)
	if msg.Topic != ConsensusBlockTopic {
		t.Fatalf("topic=%q, want %q", msg.Topic, ConsensusBlockTopic)
	}
	if msg.Slot != 3 {
		t.Fatalf("slot=%d, want 3", msg.Slot)
	}
	var cb pb.ConsensusBlock
	if err := proto.Unmarshal(msg.Payload, &cb); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cb.Origin != 7 || cb.Slot != 3 || len(cb.Payload) != 2048 {
		t.Fatalf("decoded origin=%d slot=%d payload=%d, want 7/3/2048",
			cb.Origin, cb.Slot, len(cb.Payload))
	}
}

func TestMakeExecutionPayloadMessage(t *testing.T) {
	msg := MakeExecutionPayload(3, 7, 131072)
	if msg.Topic != ExecutionPayloadTopic {
		t.Fatalf("topic=%q, want %q", msg.Topic, ExecutionPayloadTopic)
	}
	var ep pb.ExecutionPayload
	if err := proto.Unmarshal(msg.Payload, &ep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ep.Origin != 7 || ep.Slot != 3 || len(ep.Payload) != 131072 {
		t.Fatalf("decoded origin=%d slot=%d payload=%d, want 7/3/131072",
			ep.Origin, ep.Slot, len(ep.Payload))
	}
}
