package validator

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// The bucket-key property (partial-attestation-spec.md §2): every attester voting the same way
// produces byte-identical attestation_data — the bytes ARE the bucket key — and a different
// vote produces different bytes (the fork buckets).
func TestMakePartialAttDataDeterministic(t *testing.T) {
	a := MakePartialAttData(3, 7, 5, PartialAttDataSize)
	b := MakePartialAttData(3, 7, 5, PartialAttDataSize)
	if !bytes.Equal(a, b) {
		t.Fatal("same scalars produced different attestation_data bytes")
	}
	if bytes.Equal(a, MakePartialAttData(3, 7, -1, PartialAttDataSize)) {
		t.Fatal("block vote and prior-head vote share attestation_data (fork buckets collapse)")
	}
	if bytes.Equal(a, MakePartialAttData(4, 7, 5, PartialAttDataSize)) {
		t.Fatal("different slots share attestation_data")
	}
}

// The data lands near the size knob (default 128 ≈ a real AttestationData) regardless of the
// scalars' varint widths, and the vote round-trips: >= 0 verbatim, < 0 the PriorHead sentinel.
func TestMakePartialAttDataSizeAndVote(t *testing.T) {
	for _, votedOrigin := range []int{-1, 0, 5, 1 << 20} {
		data := MakePartialAttData(9, 2, votedOrigin, PartialAttDataSize)
		if n := len(data); n < PartialAttDataSize-10 || n > PartialAttDataSize+10 {
			t.Fatalf("votedOrigin %d: data = %d bytes, want ~%d", votedOrigin, n, PartialAttDataSize)
		}
		var ad pb.PartialAttData
		if err := proto.Unmarshal(data, &ad); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		want := PriorHead
		if votedOrigin >= 0 {
			want = uint32(votedOrigin)
		}
		if ad.Slot != 9 || ad.Subnet != 2 || ad.VotedOrigin != want {
			t.Fatalf("votedOrigin %d: decoded %+v, want slot9 subnet2 voted_origin %d",
				votedOrigin, &ad, want)
		}
	}
}

// The signature filler is the per-vote marginal payload: exactly size bytes, deterministic.
func TestMakePartialSignature(t *testing.T) {
	sig := MakePartialSignature(PartialSignatureSize)
	if len(sig) != PartialSignatureSize {
		t.Fatalf("signature = %d bytes, want %d", len(sig), PartialSignatureSize)
	}
	if !bytes.Equal(sig, MakePartialSignature(PartialSignatureSize)) {
		t.Fatal("signature filler not deterministic")
	}
}
