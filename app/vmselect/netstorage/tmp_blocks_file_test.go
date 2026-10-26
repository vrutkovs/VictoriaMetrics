package netstorage

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	tmpDir := "TestTmpBlocks"
	InitTmpBlocksDir(tmpDir)
	statusCode := m.Run()
	if err := os.RemoveAll(tmpDir); err != nil {
		logger.Panicf("cannot remove %q: %s", tmpDir, err)
	}
	os.Exit(statusCode)
}

func TestTmpBlocksFileSerial(t *testing.T) {
	if err := testTmpBlocksFile(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestTmpBlocksFileWriteFailure(t *testing.T) {
	// Emulate the disk write failure by pointing tmpBlocksDir to a non-existing directory.
	tmpBlocksDirOrig := tmpBlocksDir
	tmpBlocksDir = filepath.Join(tmpBlocksDirOrig, "non-existing-dir")
	defer func() {
		tmpBlocksDir = tmpBlocksDirOrig
	}()

	tbf := getTmpBlocksFile()
	defer putTmpBlocksFile(tbf)

	// Write blocks until the in-memory data is flushed to the file. The flush must fail.
	b := make([]byte, 64*1024)
	var writeErr error
	for range maxInmemoryTmpBlocksFile()/len(b) + 2 {
		if _, err := tbf.WriteBlockData(b, 0); err != nil {
			writeErr = err
			break
		}
	}
	if writeErr == nil {
		t.Fatalf("expecting non-nil error from WriteBlockData")
	}

	if _, err := tbf.WriteBlockData(b, 0); err != writeErr {
		t.Fatalf("expecting non-nil error from WriteBlockData after the failed write")
	}

	if err := tbf.Finalize(); err != writeErr {
		t.Fatalf("expecting non-nil error from Finalize after the failed write")
	}
}

func TestTmpBlocksFileConcurrent(t *testing.T) {
	concurrency := 3
	ch := make(chan error, concurrency)
	for range concurrency {
		go func() {
			ch <- testTmpBlocksFile()
		}()
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for range concurrency {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-timer.C:
			t.Fatalf("timeout")
		}
	}
}

func testTmpBlocksFile() error {
	createBlock := func() *storage.Block {
		rowsCount := rand.Intn(8000) + 1
		var timestamps, values []int64
		ts := int64(rand.Intn(1023434))
		for i := range rowsCount {
			ts += int64(rand.Intn(1000) + 1)
			timestamps = append(timestamps, ts)
			values = append(values, int64(i*i+rand.Intn(20)))
		}
		tsid := &storage.TSID{
			MetricID: 234211,
		}
		scale := int16(rand.Intn(123))
		precisionBits := uint8(rand.Intn(63) + 1)
		var b storage.Block
		b.Init(tsid, timestamps, values, scale, precisionBits)
		_, _, _ = b.MarshalData(0, 0)
		return &b
	}
	tr := storage.TimeRange{
		MinTimestamp: 0,
		MaxTimestamp: 1<<63 - 1,
	}
	for _, size := range []int{1024, 16 * 1024, maxInmemoryTmpBlocksFile() / 2, 2 * maxInmemoryTmpBlocksFile()} {
		err := func() error {
			tbf := getTmpBlocksFile()
			defer putTmpBlocksFile(tbf)

			// Write blocks until their summary size exceeds `size`.
			var addrs []tmpBlockAddr
			var blocks []*storage.Block
			bb := tmpBufPool.Get()
			defer tmpBufPool.Put(bb)
			for tbf.offset < uint64(size) {
				b := createBlock()
				bb.B = storage.MarshalBlock(bb.B[:0], b)
				addr, err := tbf.WriteBlockData(bb.B, 123)
				if err != nil {
					return fmt.Errorf("cannot write block at offset %d: %w", tbf.offset, err)
				}
				if addr.tbfIdx != 123 {
					return fmt.Errorf("unexpected tbfIdx; got %d; want 123", addr.tbfIdx)
				}
				if addr.offset+uint64(addr.size) != tbf.offset {
					return fmt.Errorf("unexpected addr=%+v for offset %v", &addr, tbf.offset)
				}
				addrs = append(addrs, addr)
				blocks = append(blocks, b)
			}
			if err := tbf.Finalize(); err != nil {
				return fmt.Errorf("cannot finalize tbf: %w", err)
			}

			// Read blocks in parallel and verify them
			concurrency := 2
			workCh := make(chan int)
			doneCh := make(chan error)
			for range concurrency {
				go func() {
					doneCh <- func() error {
						var b1 storage.Block
						for idx := range workCh {
							addr := addrs[idx]
							b := blocks[idx]
							if err := b.UnmarshalData(); err != nil {
								return fmt.Errorf("cannot unmarshal data from the original block: %w", err)
							}
							b1.Reset()
							tbf.MustReadBlockAt(&b1, addr)
							if err := b1.UnmarshalData(); err != nil {
								return fmt.Errorf("cannot unmarshal data from tbf: %w", err)
							}
							if b1.RowsCount() != b.RowsCount() {
								return fmt.Errorf("unexpected number of rows in tbf block; got %d; want %d", b1.RowsCount(), b.RowsCount())
							}
							timestamps1, values1 := b1.AppendRowsWithTimeRangeFilter(nil, nil, tr)
							timestamps, values := b.AppendRowsWithTimeRangeFilter(nil, nil, tr)
							if !reflect.DeepEqual(timestamps1, timestamps) {
								return fmt.Errorf("unexpected timestamps; got\n%v\nwant\n%v", timestamps1, timestamps)
							}
							if !reflect.DeepEqual(values1, values) {
								return fmt.Errorf("unexpected values; got\n%v\nwant\n%v", values1, values)
							}
						}
						return nil
					}()
				}()
			}
			for i := range addrs {
				workCh <- i
			}
			close(workCh)
			for range concurrency {
				select {
				case err := <-doneCh:
					if err != nil {
						return err
					}
				case <-time.After(time.Second):
					return fmt.Errorf("timeout")
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

// memBytes returns the data stored at addr in the in-memory part of tbf.
func memBytes(tbf *tmpBlocksFile, addr tmpBlockAddr) []byte {
	b := make([]byte, addr.size)
	tbf.copyBuf(b, addr.offset)
	return b
}

// fileBytes returns the data stored at addr in the finalized file of tbf.
func fileBytes(tbf *tmpBlocksFile, addr tmpBlockAddr) []byte {
	b := make([]byte, addr.size)
	tbf.r.MustReadAt(b, int64(addr.offset))
	return b
}

// testBlock returns a block of the given size, where every byte is derived from seed and its position.
func testBlock(size int, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

func TestTmpBlocksFileLimit(t *testing.T) {
	// f writes blocks of the given sizes and checks the spill state and the content after Finalize.
	f := func(limit int, sizes []int, wantSpill bool) {
		t.Helper()
		tbf := getTmpBlocksFile()
		defer putTmpBlocksFile(tbf)
		tbf.maxBufLen = limit

		var addrs []tmpBlockAddr
		for i, size := range sizes {
			addr, err := tbf.WriteBlockData(testBlock(size, byte(i)), 0)
			if err != nil {
				t.Fatalf("unexpected error at block %d: %s", i, err)
			}
			addrs = append(addrs, addr)
		}
		if gotSpill := tbf.f != nil; gotSpill != wantSpill {
			t.Fatalf("unexpected spill state for limit=%d, sizes=%v; got %v; want %v", limit, sizes, gotSpill, wantSpill)
		}
		if err := tbf.Finalize(); err != nil {
			t.Fatalf("unexpected error in Finalize: %s", err)
		}
		if gotSpill := tbf.r != nil; gotSpill != wantSpill {
			t.Fatalf("unexpected spill state after Finalize; got %v; want %v", gotSpill, wantSpill)
		}
		for i, addr := range addrs {
			var got []byte
			if wantSpill {
				got = fileBytes(tbf, addr)
			} else {
				got = memBytes(tbf, addr)
			}
			if want := testBlock(sizes[i], byte(i)); !reflect.DeepEqual(got, want) {
				t.Fatalf("unexpected data at block %d (%s) for limit=%d", i, addr, limit)
			}
		}
	}

	// exact fit stays in memory
	f(1000, []int{1000}, false)
	f(1000, []int{400, 600}, false)
	f(1000, []int{500, 499, 1}, false)

	// one byte over the limit spills
	f(1000, []int{1001}, true)
	f(1000, []int{500, 499, 2}, true)
	f(1000, []int{999, 2}, true)

	// the block bigger than the limit, followed by small blocks
	f(100, []int{250}, true)
	f(100, []int{50, 250, 30, 30, 30, 30, 30}, true)

	// the blocks straddle the chunk boundaries
	n := tmpBlocksChunkSize
	f(5*n, []int{n - 1, 2, n, n + 1, 5, n - 3, 100}, false)
	f(4*n, []int{3 * n, n + 1}, true)
	f(4*n, []int{n / 3, n/3 + 1, n/3 + 1, n / 3, 2 * n, n + 7, n}, true)
}

func TestTmpBlocksFileReadAfterSpill(t *testing.T) {
	tbf := getTmpBlocksFile()
	defer putTmpBlocksFile(tbf)
	tbf.maxBufLen = 1000

	// The blocks are spread across several flushes of the in-memory part. All of them must be readable from the file.
	var addrs []tmpBlockAddr
	for i := range 25 {
		addr, err := tbf.WriteBlockData(testBlock(100+i, byte(i)), 0)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		addrs = append(addrs, addr)
	}
	if tbf.f == nil {
		t.Fatalf("expecting the spill to file")
	}
	if err := tbf.Finalize(); err != nil {
		t.Fatalf("unexpected error in Finalize: %s", err)
	}
	for i, addr := range addrs {
		if !reflect.DeepEqual(fileBytes(tbf, addr), testBlock(100+i, byte(i))) {
			t.Fatalf("unexpected data at block %d (%s)", i, addr)
		}
	}
}

func TestTmpBlocksFileReuse(t *testing.T) {
	// The pool may drop items, so retry until the same tbf is returned.
	for round := range 100 {
		tbf := getTmpBlocksFile()
		tbf.maxBufLen = 300
		for i := range 10 {
			if _, err := tbf.WriteBlockData(testBlock(100, byte(round+i)), 0); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		}
		if err := tbf.Finalize(); err != nil {
			t.Fatalf("unexpected error in Finalize: %s", err)
		}
		putTmpBlocksFile(tbf)

		tbf1 := getTmpBlocksFile()
		if tbf1.offset != 0 || tbf1.bufLen != 0 || len(tbf1.chunks) != 0 || tbf1.f != nil || tbf1.r != nil || tbf1.err != nil {
			t.Fatalf("unexpected state of the reused tbf: %+v", tbf1)
		}
		if tbf1.maxBufLen != maxInmemoryTmpBlocksFile() {
			t.Fatalf("unexpected maxBufLen; got %d; want %d", tbf1.maxBufLen, maxInmemoryTmpBlocksFile())
		}

		// The previous data must not be visible.
		addr, err := tbf1.WriteBlockData(testBlock(64, 200), 0)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if addr.offset != 0 || !reflect.DeepEqual(memBytes(tbf1, addr), testBlock(64, 200)) {
			t.Fatalf("unexpected data at the reused tbf: addr=%s", addr)
		}
		putTmpBlocksFile(tbf1)
	}
}

func TestTmpBlocksFileReuseAfterFailure(t *testing.T) {
	tmpBlocksDirOrig := tmpBlocksDir
	tmpBlocksDir = filepath.Join(tmpBlocksDirOrig, "non-existing-dir")
	defer func() {
		tmpBlocksDir = tmpBlocksDirOrig
	}()

	for range 100 {
		tbf := getTmpBlocksFile()
		tbf.maxBufLen = 100
		var failed bool
		for range 5 {
			if _, err := tbf.WriteBlockData(testBlock(60, 1), 0); err != nil {
				failed = true
			}
		}
		if !failed {
			t.Fatalf("expecting the failed write")
		}
		putTmpBlocksFile(tbf)

		tbf1 := getTmpBlocksFile()
		if tbf1.err != nil || tbf1.bufLen != 0 || tbf1.offset != 0 {
			t.Fatalf("unexpected state of the tbf reused after the failure: %+v", tbf1)
		}
		addr, err := tbf1.WriteBlockData(testBlock(10, 3), 0)
		if err != nil || addr.offset != 0 {
			t.Fatalf("unexpected result at the tbf reused after the failure: addr=%s, err=%v", addr, err)
		}
		putTmpBlocksFile(tbf1)
	}
}

func TestTmpBlocksFileConcurrentShards(t *testing.T) {
	// Every goroutine writes its own byte pattern, so any sharing of the in-memory data between files shows up as corruption.
	const goroutines = 8
	errCh := make(chan error, goroutines)
	for g := range goroutines {
		go func() {
			errCh <- func() error {
				for round := range 40 {
					tbf := getTmpBlocksFile()
					tbf.maxBufLen = 20*tmpBlocksChunkSize + round*1000
					size := 5000 + g*997
					count := 150 + round*5
					seed := byte(g*16 + round)
					addrs := make([]tmpBlockAddr, count)
					for i := range addrs {
						addr, err := tbf.WriteBlockData(testBlock(size, seed+byte(i)), uint(g))
						if err != nil {
							return err
						}
						addrs[i] = addr
					}
					if err := tbf.Finalize(); err != nil {
						return err
					}
					for i, addr := range addrs {
						var got []byte
						if tbf.r != nil {
							got = fileBytes(tbf, addr)
						} else {
							got = memBytes(tbf, addr)
						}
						if !reflect.DeepEqual(got, testBlock(size, seed+byte(i))) {
							return fmt.Errorf("goroutine %d, round %d: corrupted block %d (%s)", g, round, i, addr)
						}
					}
					putTmpBlocksFile(tbf)
				}
				return nil
			}()
		}()
	}
	for range goroutines {
		if err := <-errCh; err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}
