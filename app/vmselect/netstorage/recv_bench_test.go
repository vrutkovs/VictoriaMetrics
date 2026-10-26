package netstorage

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

const (
	recvBenchSeries     = 2000
	recvBenchBlocks     = 14
	recvBenchRows       = 23
	recvBenchWorkers    = 2 // RF=2 -> every series arrives from both storage nodes
	recvBenchSpillBytes = 256 * 1024
)

// recvBlocks holds raw MetricBlocks as they arrive from vmstorage: per worker,
// all the blocks of a series are adjacent, series are ordered by metric name.
type recvBlocks struct {
	raw [recvBenchWorkers][][]byte
}

func newRecvBlocks() *recvBlocks {
	var rb recvBlocks
	for w := range recvBenchWorkers {
		for s := range recvBenchSeries {
			labels := []prompb.Label{
				{Name: "__name__", Value: "mon_g1"},
				{Name: "job", Value: fmt.Sprintf("job-%d", s%10)},
				{Name: "instance", Value: fmt.Sprintf("10.20.%d.%d:9100", s/250, s%250)},
				{Name: "namespace", Value: "monitoring"},
				{Name: "pod", Value: fmt.Sprintf("node-exporter-%05d", s)},
			}
			mn := storage.MarshalMetricNameRaw(nil, 4, 0, labels)
			tsid := &storage.TSID{AccountID: 4, MetricID: uint64(1000 + s), MetricGroupID: 77, JobID: uint32(s % 10), InstanceID: uint32(s)}
			for b := range recvBenchBlocks {
				// replica w has shifted block boundaries
				ts0 := int64(1700000000000 + (b*recvBenchRows+w*3)*30000)
				timestamps := make([]int64, recvBenchRows)
				values := make([]int64, recvBenchRows)
				for i := range timestamps {
					timestamps[i] = ts0 + int64(i)*30000
					values[i] = int64(s*1000 + b*recvBenchRows + i + (i*i)%7)
				}
				var blk storage.Block
				blk.Init(tsid, timestamps, values, 0, 64)
				blk.MarshalData(0, 0)
				mb := storage.MetricBlock{MetricName: mn, Block: blk}
				rb.raw[w] = append(rb.raw[w], mb.Marshal(nil))
			}
		}
	}
	return &rb
}

func (rb *recvBlocks) blocks() int {
	return recvBenchWorkers * recvBenchSeries * recvBenchBlocks
}

// feed receives all the raw blocks like the vmstorage RPC callback does.
func (rb *recvBlocks) feed(tb testing.TB, tbfw *tmpBlocksFileWrapper) {
	var mbs [recvBenchWorkers]storage.MetricBlock
	for w := range recvBenchWorkers {
		for _, raw := range rb.raw[w] {
			if err := recvBlock(tbfw, &mbs[w], raw, uint(w)); err != nil {
				tb.Fatalf("recv: %s", err)
			}
		}
	}
}

func newBenchWrapper(spill bool) *tmpBlocksFileWrapper {
	tbfw := &tmpBlocksFileWrapper{shards: make([]tmpBlocksFileWrapperShardWithPadding, recvBenchWorkers)}
	for i := range tbfw.shards {
		tbfw.shards[i].initIfNeeded()
		if spill {
			tbfw.shards[i].tbf.maxBufLen = recvBenchSpillBytes
		}
	}
	return tbfw
}

// readAll mimics sortBlock.unpackFrom without UnmarshalData.
func readAll(tbfw *tmpBlocksFileWrapper, names []string, addrss []blockAddrs, m map[string]int) int {
	tbfs := tbfw.getTmpBlockFiles()
	var b storage.Block
	n := 0
	for _, name := range names {
		for _, addr := range addrss[m[name]].addrs {
			b.Reset()
			tbfs[addr.tbfIdx].MustReadBlockAt(&b, addr)
			n++
		}
	}
	return n
}

func BenchmarkRecvPath(b *testing.B) {
	rb := newRecvBlocks()
	nb := float64(rb.blocks())
	for _, spill := range []bool{false, true} {
		suffix := "mem"
		if spill {
			suffix = "spill"
		}

		b.Run("recv-"+suffix, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				tbfw := newBenchWrapper(spill)
				b.StartTimer()
				rb.feed(b, tbfw)
				b.StopTimer()
				closeTmpBlockFiles(tbfw.getTmpBlockFiles())
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nb, "ns/block")
		})

		b.Run("finalize-"+suffix, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				tbfw := newBenchWrapper(spill)
				rb.feed(b, tbfw)
				b.StartTimer()
				if _, _, _, _, err := tbfw.Finalize(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				closeTmpBlockFiles(tbfw.getTmpBlockFiles())
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nb, "ns/block")
		})

		b.Run("read-"+suffix, func(b *testing.B) {
			b.ReportAllocs()
			tbfw := newBenchWrapper(spill)
			rb.feed(b, tbfw)
			names, addrss, m, _, err := tbfw.Finalize()
			if err != nil {
				b.Fatal(err)
			}
			defer closeTmpBlockFiles(tbfw.getTmpBlockFiles())
			for b.Loop() {
				if n := readAll(tbfw, names, addrss, m); n != rb.blocks() {
					b.Fatalf("read %d blocks; want %d", n, rb.blocks())
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nb, "ns/block")
		})

		b.Run("total-"+suffix, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tbfw := newBenchWrapper(spill)
				rb.feed(b, tbfw)
				names, addrss, m, _, err := tbfw.Finalize()
				if err != nil {
					b.Fatal(err)
				}
				readAll(tbfw, names, addrss, m)
				closeTmpBlockFiles(tbfw.getTmpBlockFiles())
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nb, "ns/block")
		})
	}
}
