package storage

import (
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestMarshaledBlockHeaderSize(t *testing.T) {
	// This test makes sure marshaled format isn't changed.
	// If this test breaks then the storage format has been changed,
	// so it may become incompatible with the previously written data.
	expectedSize := 89
	if marshaledBlockHeaderSize != expectedSize {
		t.Fatalf("unexpected marshaledBlockHeaderSize; got %d; want %d", marshaledBlockHeaderSize, expectedSize)
	}
}

func TestBlockHeaderUnmarshalAllFields(t *testing.T) {
	// Every field gets a distinct non-zero value, so a wrong offset in Unmarshal is detected.
	bh := blockHeader{
		TSID: TSID{
			AccountID: 1, ProjectID: 2, MetricGroupID: 3<<40 + 4, JobID: 5, InstanceID: 6, MetricID: 7<<33 + 8,
		},
		MinTimestamp:          -1 << 40,
		MaxTimestamp:          1<<41 + 9,
		FirstValue:            -(1<<42 + 10),
		TimestampsBlockOffset: 11<<34 + 12,
		ValuesBlockOffset:     13<<35 + 14,
		TimestampsBlockSize:   15,
		ValuesBlockSize:       16,
		RowsCount:             17,
		Scale:                 -18,
		TimestampsMarshalType: encoding.MarshalTypeZSTDNearestDelta2,
		ValuesMarshalType:     encoding.MarshalTypeNearestDelta,
		PrecisionBits:         19,
	}
	data := bh.Marshal([]byte("prefix"))[len("prefix"):]
	data = append(data, "tail"...)

	var bh2 blockHeader
	tail, err := bh2.Unmarshal(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(tail) != "tail" {
		t.Fatalf("unexpected tail; got %q; want %q", tail, "tail")
	}
	if bh2 != bh {
		t.Fatalf("unexpected blockHeader;\ngot  %+v\nwant %+v", bh2, bh)
	}
}

func TestBlockHeaderMarshalUnmarshal(t *testing.T) {
	var bh blockHeader
	for i := range 1000 {
		bh.TSID.MetricID = uint64(i + 1)
		bh.MinTimestamp = int64(-i*1e3 + 2)
		bh.MaxTimestamp = int64(i*2e3 + 3)
		bh.TimestampsBlockOffset = uint64(i*12345 + 4)
		bh.ValuesBlockOffset = uint64(i*3243 + 5)
		bh.TimestampsBlockSize = uint32((i*892 + 6) % maxBlockSize)
		bh.ValuesBlockSize = uint32((i*894 + 7) % maxBlockSize)
		bh.RowsCount = uint32(i*3 + 8)
		bh.Scale = int16(i - 434 + 9)
		bh.TimestampsMarshalType = encoding.MarshalType((i + 10) % 7)
		bh.ValuesMarshalType = encoding.MarshalType((i + 11) % 7)
		bh.PrecisionBits = 1 + uint8((i+12)%64)

		testBlockHeaderMarshalUnmarshal(t, &bh)
	}
}

func testBlockHeaderMarshalUnmarshal(t *testing.T, bh *blockHeader) {
	t.Helper()

	dst := bh.Marshal(nil)
	if len(dst) != marshaledBlockHeaderSize {
		t.Fatalf("unexpected dst size; got %d; want %d", len(dst), marshaledBlockHeaderSize)
	}
	var bh1 blockHeader
	tail, err := bh1.Unmarshal(dst)
	if err != nil {
		t.Fatalf("cannot unmarshal bh=%+v from dst=%x: %s", bh, dst, err)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected tail left after unmarshaling of bh=%+v: %x", bh, tail)
	}
	if !reflect.DeepEqual(bh, &bh1) {
		t.Fatalf("unexpected bh unmarshaled; got\n%+v; want\n%+v", &bh1, bh)
	}

	prefix := []byte("foo")
	dstNew := bh.Marshal(prefix)
	if string(dstNew[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix after marshaling bh=%+v; got\n%x; want\n%x", bh, dstNew[:len(prefix)], prefix)
	}
	if string(dstNew[len(prefix):]) != string(dst) {
		t.Fatalf("unexpected prefixed dst for bh=%+v; got\n%x; want\n%x", bh, dstNew[len(prefix):], dst)
	}

	suffix := []byte("bar")
	dst = append(dst, suffix...)
	var bh2 blockHeader
	tail, err = bh2.Unmarshal(dst)
	if err != nil {
		t.Fatalf("cannot unmarshal bh=%+v from suffixed dst=%x: %s", bh, dst, err)
	}
	if string(tail) != string(suffix) {
		t.Fatalf("unexpected tail after unmarshaling bh=%+v; got\n%x; want\n%x", bh, tail, suffix)
	}
	if !reflect.DeepEqual(bh, &bh2) {
		t.Fatalf("unexpected bh unmarshaled after adding suffix; got\n%+v; want\n%+v", &bh2, bh)
	}
}
