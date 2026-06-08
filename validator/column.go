package validator

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// ColumnTopicPrefix is the per-subnet data-column topic stem; the column (subnet) index and
// the ssz_snappy suffix complete it (ssz_snappy is in the name only — raw bytes).
const ColumnTopicPrefix = "/eth2/00000000/data_column_sidecar_"

// Blobs is the blob count B a column erasure-codes; it sets the column's wire size
// (slot-messages.md §5). Default 6 ⇒ ~12.9 KiB; the blob-count roadmap (9 → 72) grows
// columns to ~151 KiB.
const Blobs = 6

// ColumnSize is the target marshaled size of one DataColumnSidecar (B*2144 + 356 B). Only
// that the burst carries a realistic per-message size is load-bearing, so the filler lands
// close, not exact. At B=6 (13220 B < 16384) the shared sizedFiller's 2-byte varint holds.
const ColumnSize = Blobs*2144 + 356

// ColumnTopic is the gossipsub topic for column subnet i's data-column sidecars.
func ColumnTopic(i int) string {
	return fmt.Sprintf("%s%d/ssz_snappy", ColumnTopicPrefix, i)
}

// MakeColumn builds one DataColumnSidecar for the given column subnet as a publish-ready
// Message. origin is the proposer node (the gossip origin for the loopback skip). payload is
// non-zero filler sized so the marshaled message ≈ ColumnSize.
func MakeColumn(slot, column, origin int) Message {
	col := &pb.Column{
		Slot:   uint32(slot),
		Column: uint32(column),
		Origin: uint32(origin),
	}
	col.Payload = sizedFiller(col, ColumnSize)
	buf, err := proto.Marshal(col)
	if err != nil {
		panic("MakeColumn: marshal: " + err.Error())
	}
	return Message{Topic: ColumnTopic(column), Payload: buf, Slot: slot}
}
