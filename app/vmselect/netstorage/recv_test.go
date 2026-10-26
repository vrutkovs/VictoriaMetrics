package netstorage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// TestRegisterAndWriteBlockRoundTrip checks that raw blocks received from two storage nodes
// are stored verbatim and read back equal, including interleaved series and a spill to file.
func TestRegisterAndWriteBlockRoundTrip(t *testing.T) {
	f := func(spill bool) {
		t.Helper()
		const series = 5
		tbfw := &tmpBlocksFileWrapper{shards: make([]tmpBlocksFileWrapperShardWithPadding, 2)}
		for i := range tbfw.shards {
			tbfw.shards[i].initIfNeeded()
			if spill {
				tbfw.shards[i].tbf.maxBufLen = 100
			}
		}

		type blockKey struct {
			name string
			ts0  int64
		}
		want := map[blockKey][]int64{}
		for w := uint(0); w < 2; w++ {
			// a, a, b, a, b, b, c ... - series are interleaved, so the previous name fast path misses
			for i := range 3 * series {
				s := (i * 7 / 3) % series
				name := fmt.Sprintf("metric-%d", s)
				ts0 := int64(1000000*(i+1)) + int64(w)
				timestamps := []int64{ts0, ts0 + 10, ts0 + 20}
				values := []int64{int64(s), int64(s + 1), int64(s + 5)}
				var mb storage.MetricBlock
				mb.MetricName = []byte(name)
				mb.Block.Init(&storage.TSID{MetricID: uint64(s)}, timestamps, values, 0, 64)
				mb.Block.MarshalData(0, 0)
				raw := mb.Marshal(nil)

				var tmp storage.MetricBlock
				if err := recvBlock(tbfw, &tmp, raw, w); err != nil {
					t.Fatalf("spill=%v: unexpected error: %s", spill, err)
				}
				want[blockKey{name, ts0}] = timestamps
			}
		}

		names, addrss, m, _, err := tbfw.Finalize()
		if err != nil {
			t.Fatalf("spill=%v: cannot finalize: %s", spill, err)
		}
		defer closeTmpBlockFiles(tbfw.getTmpBlockFiles())
		if len(names) != series || len(m) != series {
			t.Fatalf("spill=%v: unexpected series count; got %d, %d; want %d", spill, len(names), len(m), series)
		}

		tbfs := tbfw.getTmpBlockFiles()
		got := 0
		for _, name := range names {
			for _, addr := range addrss[m[name]].addrs {
				var b storage.Block
				tbfs[addr.tbfIdx].MustReadBlockAt(&b, addr)
				if err := b.UnmarshalData(); err != nil {
					t.Fatalf("spill=%v: cannot unmarshal data: %s", spill, err)
				}
				timestamps, _ := b.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MaxTimestamp: 1<<63 - 1})
				if !reflect.DeepEqual(timestamps, want[blockKey{name, timestamps[0]}]) {
					t.Fatalf("spill=%v: unexpected timestamps for %q: %v", spill, name, timestamps)
				}
				got++
			}
		}
		if got != len(want) {
			t.Fatalf("spill=%v: unexpected block count; got %d; want %d", spill, got, len(want))
		}
	}
	f(false)
	f(true)
}
