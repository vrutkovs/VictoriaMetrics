package storage

import (
	"bytes"
	"testing"
)

func TestSplitMetricBlock(t *testing.T) {
	tsid := &TSID{MetricID: 123, JobID: 7}
	var mb MetricBlock
	mb.MetricName = []byte("foo-bar-metric-name")
	mb.Block.Init(tsid, []int64{1000, 2000, 3000, 4500}, []int64{10, 11, 15, 16}, -2, 64)
	mb.Block.MarshalData(11, 22)
	src := mb.Marshal(nil)

	metricName, block, rows, err := SplitMetricBlock(src)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(metricName) != string(mb.MetricName) {
		t.Fatalf("unexpected metricName; got %q; want %q", metricName, mb.MetricName)
	}
	if rows != 4 {
		t.Fatalf("unexpected rows; got %d; want 4", rows)
	}

	// The returned block must be byte-identical to the block marshaled after the full unmarshal.
	var mb2 MetricBlock
	if _, err := mb2.Unmarshal(src); err != nil {
		t.Fatalf("cannot unmarshal MetricBlock: %s", err)
	}
	if want := MarshalBlock(nil, &mb2.Block); !bytes.Equal(block, want) {
		t.Fatalf("block differs from MarshalBlock output;\ngot  %X\nwant %X", block, want)
	}
	var b Block
	if tail, err := UnmarshalBlock(&b, block); err != nil || len(tail) > 0 {
		t.Fatalf("cannot unmarshal the returned block: err=%v, tail=%d", err, len(tail))
	}
	if err := b.UnmarshalData(); err != nil {
		t.Fatalf("cannot unmarshal block data: %s", err)
	}
	if b.RowsCount() != 4 {
		t.Fatalf("unexpected RowsCount after the round trip; got %d; want 4", b.RowsCount())
	}
}

func TestSplitMetricBlockErrors(t *testing.T) {
	var mb MetricBlock
	mb.MetricName = []byte("foo")
	mb.Block.Init(&TSID{MetricID: 1}, []int64{1000, 2000, 3000}, []int64{1, 2, 3}, 0, 64)
	mb.Block.MarshalData(0, 0)
	src := mb.Marshal(nil)

	f := func(name string, src []byte) {
		t.Helper()
		if _, _, _, err := SplitMetricBlock(src); err == nil {
			t.Fatalf("%s: expecting non-empty error", name)
		}
	}
	f("empty", nil)
	f("trailing data", append(src[:len(src):len(src)], 1))
	for _, n := range []int{1, 4, len(src) / 2, len(src) - 1} {
		f("truncated", src[:n])
	}

	// zero RowsCount is rejected by the block header validation
	bad := append([]byte{}, src...)
	headerStart := 1 + len(mb.MetricName)
	rowsOffset := headerStart + marshaledTSIDSize + 8*5 + 4*2
	copy(bad[rowsOffset:], []byte{0, 0, 0, 0})
	f("zero rows", bad)
}
