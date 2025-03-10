package netstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/flagutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fs"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/memory"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/VictoriaMetrics/metrics"
)

var tmpBufSize = flagutil.NewBytes("search.inmemoryBufSizeBytes", 0, "Size for in-memory data blocks used during processing search requests. "+
	"By default, the size is automatically calculated based on available memory. "+
	"Adjust this flag value if you observe that vm_tmp_blocks_max_inmemory_file_size_bytes metric constantly shows much higher values than vm_tmp_blocks_inmemory_file_size_bytes. See https://github.com/zzylol/VictoriaMetrics-cluster/pull/6851")

// InitTmpBlocksDir initializes directory to store temporary search results.
//
// It stores data in system-defined temporary directory if tmpDirPath is empty.
func InitTmpBlocksDir(tmpDirPath string) {
	if len(tmpDirPath) == 0 {
		tmpDirPath = os.TempDir()
	}
	tmpBlocksDir = filepath.Join(tmpDirPath, "searchResults")
	fs.MustRemoveAll(tmpBlocksDir)
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

type tmpBlocksFile struct {
	buf []byte

	f *os.File
	r *fs.ReaderAt

	offset uint64
}

func getTmpBlocksFile() *tmpBlocksFile {
	v := tmpBlocksFilePool.Get()
	if v == nil {
		return &tmpBlocksFile{
			buf: make([]byte, 0, maxInmemoryTmpBlocksFile()),
		}
	}
	return v.(*tmpBlocksFile)
}

func putTmpBlocksFile(tbf *tmpBlocksFile) {
	tbf.MustClose()
	bufLen := tbf.Len()
	tmpBufSizeSummary.Update(float64(bufLen))
	tbf.buf = tbf.buf[:0]
	tbf.f = nil
	tbf.r = nil
	tbf.offset = 0
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
func (tbf *tmpBlocksFile) WriteBlockData(b []byte, tbfIdx uint) (tmpBlockAddr, error) {
	var addr tmpBlockAddr
	addr.tbfIdx = tbfIdx
	addr.offset = tbf.offset
	addr.size = len(b)
	tbf.offset += uint64(addr.size)
	if len(tbf.buf)+len(b) <= cap(tbf.buf) {
		// Fast path - the data fits tbf.buf
		tbf.buf = append(tbf.buf, b...)
		return addr, nil
	}

	// Slow path: flush the data from tbf.buf to file.
	if tbf.f == nil {
		f, err := os.CreateTemp(tmpBlocksDir, "")
		if err != nil {
			return addr, err
		}
		tbf.f = f
		tmpBlocksFilesCreated.Inc()
	}
	_, err := tbf.f.Write(tbf.buf)
	tbf.buf = append(tbf.buf[:0], b...)
	if err != nil {
		return addr, fmt.Errorf("cannot write block to %q: %w", tbf.f.Name(), err)
	}
	return addr, nil
}

// Len() returnt tbf size in bytes.
func (tbf *tmpBlocksFile) Len() uint64 {
	return tbf.offset
}

func (tbf *tmpBlocksFile) Finalize() error {
	if tbf.f == nil {
		return nil
	}
	fname := tbf.f.Name()
	if _, err := tbf.f.Write(tbf.buf); err != nil {
		return fmt.Errorf("cannot write the remaining %d bytes to %q: %w", len(tbf.buf), fname, err)
	}
	tbf.buf = tbf.buf[:0]
	r := fs.NewReaderAt(tbf.f)

	// Hint the OS that the file is read almost sequentially.
	// This should reduce the number of disk seeks, which is important
	// for HDDs.
	r.MustFadviseSequentialRead(true)

	// Collect local stats in order to improve performance on systems with big number of CPU cores.
	// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/3966
	r.SetUseLocalStats()

	tbf.r = r
	tbf.f = nil
	return nil
}

func (tbf *tmpBlocksFile) MustReadBlockAt(dst *storage.Block, addr tmpBlockAddr) {
	var buf []byte
	if tbf.r == nil {
		buf = tbf.buf[addr.offset : addr.offset+uint64(addr.size)]
	} else {
		bb := tmpBufPool.Get()
		defer tmpBufPool.Put(bb)
		bb.B = bytesutil.ResizeNoCopyMayOverallocate(bb.B, addr.size)
		tbf.r.MustReadAt(bb.B, int64(addr.offset))
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
