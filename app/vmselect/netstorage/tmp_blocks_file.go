package netstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
)

var tmpBufSize = flagutil.NewBytes("search.inmemoryBufSizeBytes", 0, "Size for in-memory data blocks used during processing search requests. "+
	"By default, the size is automatically calculated based on available memory. "+
	"Adjust this flag value if you observe that vm_tmp_blocks_max_inmemory_file_size_bytes metric constantly shows much higher values than vm_tmp_blocks_inmemory_file_size_bytes. See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/6851")

// InitTmpBlocksDir initializes directory to store temporary search results.
//
// It stores data in system-defined temporary directory if tmpDirPath is empty.
func InitTmpBlocksDir(tmpDirPath string) {
	if len(tmpDirPath) == 0 {
		tmpDirPath = os.TempDir()
	}
	tmpBlocksDir = filepath.Join(tmpDirPath, "searchResults")
	fs.RegisterPathFsMetrics(tmpDirPath)
	fs.MustRemoveDirContents(tmpBlocksDir)
	fs.MustMkdirIfNotExist(tmpBlocksDir)
}

var tmpBlocksDir string

func maxInmemoryTmpBlocksFile() int {
	if tmpBufSize.IntN() > 0 {
		return tmpBufSize.IntN()
	}
	mem := memory.Allowed()
	maxLen := mem / 1024
	if maxLen < 64*1024 {
		return 64 * 1024
	}
	if maxLen > 4*1024*1024 {
		return 4 * 1024 * 1024
	}
	return maxLen
}

var (
	_ = metrics.NewGauge(`vm_tmp_blocks_max_inmemory_file_size_bytes`, func() float64 {
		return float64(maxInmemoryTmpBlocksFile())
	})
	tmpBufSizeSummary = metrics.NewSummary(`vm_tmp_blocks_inmemory_file_size_bytes`)
)

// tmpBlocksChunkSize is the size of a chunk of the in-memory part of tmpBlocksFile.
const tmpBlocksChunkSize = 64 * 1024

type tmpBlocksChunk [tmpBlocksChunkSize]byte

// tmpBlocksChunkPool is shared by all tmpBlocksFile, so the memory kept between
// queries depends on the bytes in flight and not on maxBufLen.
var tmpBlocksChunkPool sync.Pool

type tmpBlocksFile struct {
	// chunks contain bufLen bytes not flushed to the file yet. All the chunks except the last one are full.
	chunks    []*tmpBlocksChunk
	bufLen    int
	maxBufLen int

	f *os.File
	r *fs.ReaderAt

	offset uint64

	// err stores the first error occurred while writing the temporary blocks file.
	err error
}

func getTmpBlocksFile() *tmpBlocksFile {
	v := tmpBlocksFilePool.Get()
	if v == nil {
		v = &tmpBlocksFile{}
	}
	tbf := v.(*tmpBlocksFile)
	tbf.maxBufLen = maxInmemoryTmpBlocksFile()
	return tbf
}

func putTmpBlocksFile(tbf *tmpBlocksFile) {
	tbf.MustClose()
	bufLen := tbf.Len()
	tmpBufSizeSummary.Update(float64(bufLen))
	tbf.resetBuf()
	tbf.f = nil
	tbf.r = nil
	tbf.offset = 0
	tbf.err = nil
	tmpBlocksFilePool.Put(tbf)
}

var tmpBlocksFilePool sync.Pool

type tmpBlockAddr struct {
	offset uint64
	size   int
	tbfIdx uint
}

func (addr tmpBlockAddr) String() string {
	return fmt.Sprintf("offset %d, size %d, tbfIdx %d", addr.offset, addr.size, addr.tbfIdx)
}

var (
	tmpBlocksFilesCreated = metrics.NewCounter(`vm_tmp_blocks_files_created_total`)
	_                     = metrics.NewGauge(`vm_tmp_blocks_files_directory_free_bytes`, func() float64 {
		return float64(fs.MustGetFreeSpace(tmpBlocksDir))
	})
)

// WriteBlockData writes b to tbf.
//
// It returns errors since the operation may fail on space shortage
// and this must be handled.
//
// The tbf is left unusable after the first error, since a failed write cannot be undone:
// the returned addresses are derived from tbf.offset before the data is flushed from tbf.buf
// to the file, the failed flush may be partial, and the remaining buffer is dropped.
func (tbf *tmpBlocksFile) WriteBlockData(b []byte, tbfIdx uint) (tmpBlockAddr, error) {
	var addr tmpBlockAddr
	if tbf.err != nil {
		// Do not write anything to the tbf after the first failed write
		return addr, tbf.err
	}
	addr.tbfIdx = tbfIdx
	addr.offset = tbf.offset
	addr.size = len(b)
	tbf.offset += uint64(addr.size)
	if tbf.bufLen+len(b) <= tbf.maxBufLen {
		// Fast path - the data fits the in-memory chunks
		tbf.appendBuf(b)
		return addr, nil
	}

	// Slow path: flush the data from the in-memory chunks to file.
	if tbf.f == nil {
		f, err := os.CreateTemp(tmpBlocksDir, "")
		if err != nil {
			tbf.err = fmt.Errorf("cannot create temporary blocks file at %q: %w", tmpBlocksDir, err)
			return addr, tbf.err
		}
		tbf.f = f
		tmpBlocksFilesCreated.Inc()
	}
	err := tbf.flushBuf()
	tbf.appendBuf(b)
	if err != nil {
		// The blocks buffered at tbf.buf could be partially lost, mark the tbf as unusable.
		tbf.err = fmt.Errorf("cannot write block to %q: %w", tbf.f.Name(), err)
		return addr, tbf.err
	}
	return addr, nil
}

// Len() return tbf size in bytes.
func (tbf *tmpBlocksFile) Len() uint64 {
	return tbf.offset
}

func (tbf *tmpBlocksFile) Finalize() error {
	if tbf.err != nil {
		return tbf.err
	}
	if tbf.f == nil {
		return nil
	}
	fname := tbf.f.Name()
	bufLen := tbf.bufLen
	if err := tbf.flushBuf(); err != nil {
		tbf.err = fmt.Errorf("cannot write the remaining %d bytes to %q: %w", bufLen, fname, err)
		return tbf.err
	}
	r := fs.NewReaderAt(tbf.f)

	// Hint the OS that the file is read almost sequentially.
	// This should reduce the number of disk seeks, which is important
	// for HDDs.
	r.MustFadviseSequentialRead(true)

	// Collect local stats in order to improve performance on systems with big number of CPU cores.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3966
	r.SetUseLocalStats()

	tbf.r = r
	tbf.f = nil
	return nil
}

func (tbf *tmpBlocksFile) MustReadBlockAt(dst *storage.Block, addr tmpBlockAddr) {
	if tbf.err != nil {
		// This should never happen, since Finalize() already returns the error for such a tbf.
		logger.Panicf("BUG: cannot read block at %s from the temporary blocks file with the failed write: %s", addr, tbf.err)
	}
	var buf []byte
	if off := addr.offset % tmpBlocksChunkSize; tbf.r == nil && off+uint64(addr.size) <= tmpBlocksChunkSize {
		buf = tbf.chunks[addr.offset/tmpBlocksChunkSize][off : off+uint64(addr.size)]
	} else {
		bb := tmpBufPool.Get()
		defer tmpBufPool.Put(bb)
		bb.B = bytesutil.ResizeNoCopyMayOverallocate(bb.B, addr.size)
		if tbf.r != nil {
			tbf.r.MustReadAt(bb.B, int64(addr.offset))
		} else {
			// The block straddles the chunks.
			tbf.copyBuf(bb.B, addr.offset)
		}
		buf = bb.B
	}
	tail, err := storage.UnmarshalBlock(dst, buf)
	if err != nil {
		logger.Panicf("FATAL: cannot unmarshal data at %s: %s", addr, err)
	}
	if len(tail) > 0 {
		logger.Panicf("FATAL: unexpected non-empty tail left after unmarshaling data at %s; len(tail)=%d", addr, len(tail))
	}
}

var tmpBufPool bytesutil.ByteBufferPool

func (tbf *tmpBlocksFile) appendBuf(b []byte) {
	for len(b) > 0 {
		pos := tbf.bufLen % tmpBlocksChunkSize
		if pos == 0 {
			c, _ := tmpBlocksChunkPool.Get().(*tmpBlocksChunk)
			if c == nil {
				c = &tmpBlocksChunk{}
			}
			tbf.chunks = append(tbf.chunks, c)
		}
		n := copy(tbf.chunks[len(tbf.chunks)-1][pos:], b)
		tbf.bufLen += n
		b = b[n:]
	}
}

// copyBuf copies len(dst) bytes starting at the given offset from the in-memory chunks to dst.
func (tbf *tmpBlocksFile) copyBuf(dst []byte, offset uint64) {
	for len(dst) > 0 {
		n := copy(dst, tbf.chunks[offset/tmpBlocksChunkSize][offset%tmpBlocksChunkSize:])
		dst = dst[n:]
		offset += uint64(n)
	}
}

// flushBuf writes the in-memory chunks to tbf.f and releases them.
func (tbf *tmpBlocksFile) flushBuf() error {
	var err error
	left := tbf.bufLen
	for _, c := range tbf.chunks {
		if _, err = tbf.f.Write(c[:min(left, tmpBlocksChunkSize)]); err != nil {
			break
		}
		left -= tmpBlocksChunkSize
	}
	tbf.resetBuf()
	return err
}

func (tbf *tmpBlocksFile) resetBuf() {
	for i, c := range tbf.chunks {
		tmpBlocksChunkPool.Put(c)
		tbf.chunks[i] = nil
	}
	tbf.chunks = tbf.chunks[:0]
	tbf.bufLen = 0
}

func (tbf *tmpBlocksFile) MustClose() {
	if tbf.f != nil {
		// tbf.f could be non-nil if Finalize wasn't called.
		// In this case tbf.r must be nil.
		if tbf.r != nil {
			logger.Panicf("BUG: tbf.r must be nil when tbf.f!=nil")
		}

		// Try removing the file before closing it in order to prevent from flushing the in-memory data
		// from page cache to the disk and save disk write IO. This may fail on non-posix systems such as Windows.
		// Gracefully handle this case by attempting to remove the file after closing it.
		fname := tbf.f.Name()
		errRemove := os.Remove(fname)
		if err := tbf.f.Close(); err != nil {
			logger.Panicf("FATAL: cannot close %q: %s", fname, err)
		}
		if errRemove != nil {
			if err := os.Remove(fname); err != nil {
				logger.Panicf("FATAL: cannot remove %q: %s", fname, err)
			}
		}
		tbf.f = nil
		return
	}

	if tbf.r == nil {
		// Nothing to do
		return
	}

	// Try removing the file before closing it in order to prevent from flushing the in-memory data
	// from page cache to the disk and save disk write IO. This may fail on non-posix systems such as Windows.
	// Gracefully handle this case by attempting to remove the file after closing it.
	fname := tbf.r.Path()
	errRemove := os.Remove(fname)
	tbf.r.MustClose()
	if errRemove != nil {
		if err := os.Remove(fname); err != nil {
			logger.Panicf("FATAL: cannot remove %q: %s", fname, err)
		}
	}
	tbf.r = nil
}
