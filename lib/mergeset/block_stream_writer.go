package mergeset

import (
	"path/filepath"
	"sync"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/filestream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fs"
)

type blockStreamWriter struct {
	compressLevel int

	metaindexWriter filestream.WriteCloser
	indexWriter     filestream.WriteCloser
	itemsWriter     filestream.WriteCloser
	lensWriter      filestream.WriteCloser

	sb storageBlock
	bh blockHeader
	mr metaindexRow

	unpackedIndexBlockBuf []byte
	packedIndexBlockBuf   []byte

	unpackedMetaindexBuf []byte
	packedMetaindexBuf   []byte

	itemsBlockOffset uint64
	lensBlockOffset  uint64
	indexBlockOffset uint64

	// whether the first item for mr has been caught.
	mrFirstItemCaught bool
}

func (bsw *blockStreamWriter) reset() {
	bsw.compressLevel = 0

	bsw.metaindexWriter = nil
	bsw.indexWriter = nil
	bsw.itemsWriter = nil
	bsw.lensWriter = nil

	bsw.sb.Reset()
	bsw.bh.Reset()
	bsw.mr.Reset()

	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]
	bsw.packedIndexBlockBuf = bsw.packedIndexBlockBuf[:0]

	bsw.unpackedMetaindexBuf = bsw.unpackedMetaindexBuf[:0]
	bsw.packedMetaindexBuf = bsw.packedMetaindexBuf[:0]

	bsw.itemsBlockOffset = 0
	bsw.lensBlockOffset = 0
	bsw.indexBlockOffset = 0

	bsw.mrFirstItemCaught = false
}

func (bsw *blockStreamWriter) MustInitFromInmemoryPart(mp *inmemoryPart, compressLevel int) {
	bsw.reset()

	bsw.compressLevel = compressLevel
	bsw.metaindexWriter = &mp.metaindexData
	bsw.indexWriter = &mp.indexData
	bsw.itemsWriter = &mp.itemsData
	bsw.lensWriter = &mp.lensData
}

// MustInitFromFilePart initializes bsw from a file-based part on the given path.
//
// The bsw doesn't pollute OS page cache if nocache is set.
func (bsw *blockStreamWriter) MustInitFromFilePart(path string, nocache bool, compressLevel int) {
	path = filepath.Clean(path)

	// Create the directory
	fs.MustMkdirFailIfExist(path)

	// Create part files in the directory.

	// Always cache metaindex file in OS page cache, since it is immediately
	// read after the merge.
	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustCreate(metaindexPath, false)

	indexPath := filepath.Join(path, indexFilename)
	indexFile := filestream.MustCreate(indexPath, nocache)

	itemsPath := filepath.Join(path, itemsFilename)
	itemsFile := filestream.MustCreate(itemsPath, nocache)

	lensPath := filepath.Join(path, lensFilename)
	lensFile := filestream.MustCreate(lensPath, nocache)

	bsw.reset()
	bsw.compressLevel = compressLevel

	bsw.metaindexWriter = metaindexFile
	bsw.indexWriter = indexFile
	bsw.itemsWriter = itemsFile
	bsw.lensWriter = lensFile
}

// MustClose closes the bsw.
//
// It closes *Writer files passed to Init*.
func (bsw *blockStreamWriter) MustClose() {
	// Flush the remaining data.
	bsw.flushIndexData()

	// Compress and write metaindex.
	bsw.packedMetaindexBuf = encoding.CompressZSTDLevel(bsw.packedMetaindexBuf[:0], bsw.unpackedMetaindexBuf, bsw.compressLevel)
	fs.MustWriteData(bsw.metaindexWriter, bsw.packedMetaindexBuf)

	// Close all the writers.
	bsw.metaindexWriter.MustClose()
	bsw.indexWriter.MustClose()
	bsw.itemsWriter.MustClose()
	bsw.lensWriter.MustClose()

	bsw.reset()
}

// WriteBlock writes ib to bsw.
//
// ib must be sorted.
func (bsw *blockStreamWriter) WriteBlock(ib *inmemoryBlock) {
	bsw.bh.firstItem, bsw.bh.commonPrefix, bsw.bh.itemsCount, bsw.bh.marshalType = ib.MarshalSortedData(&bsw.sb, bsw.bh.firstItem[:0], bsw.bh.commonPrefix[:0], bsw.compressLevel)

	// Write itemsData
	fs.MustWriteData(bsw.itemsWriter, bsw.sb.itemsData)
	bsw.bh.itemsBlockSize = uint32(len(bsw.sb.itemsData))
	bsw.bh.itemsBlockOffset = bsw.itemsBlockOffset
	bsw.itemsBlockOffset += uint64(bsw.bh.itemsBlockSize)

	// Write lensData
	fs.MustWriteData(bsw.lensWriter, bsw.sb.lensData)
	bsw.bh.lensBlockSize = uint32(len(bsw.sb.lensData))
	bsw.bh.lensBlockOffset = bsw.lensBlockOffset
	bsw.lensBlockOffset += uint64(bsw.bh.lensBlockSize)

	// Write blockHeader
	unpackedIndexBlockBufLen := len(bsw.unpackedIndexBlockBuf)
	bsw.unpackedIndexBlockBuf = bsw.bh.Marshal(bsw.unpackedIndexBlockBuf)
	if len(bsw.unpackedIndexBlockBuf) > maxIndexBlockSize {
		bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:unpackedIndexBlockBufLen]
		bsw.flushIndexData()
		bsw.unpackedIndexBlockBuf = bsw.bh.Marshal(bsw.unpackedIndexBlockBuf)
	}

	if !bsw.mrFirstItemCaught {
		bsw.mr.firstItem = append(bsw.mr.firstItem[:0], bsw.bh.firstItem...)
		bsw.mrFirstItemCaught = true
	}
	bsw.bh.Reset()
	bsw.mr.blockHeadersCount++
}

// The maximum size of index block with multiple blockHeaders.
const maxIndexBlockSize = 64 * 1024

func (bsw *blockStreamWriter) flushIndexData() {
	if len(bsw.unpackedIndexBlockBuf) == 0 {
		// Nothing to flush.
		return
	}

	// Write indexBlock.
	bsw.packedIndexBlockBuf = encoding.CompressZSTDLevel(bsw.packedIndexBlockBuf[:0], bsw.unpackedIndexBlockBuf, bsw.compressLevel)
	fs.MustWriteData(bsw.indexWriter, bsw.packedIndexBlockBuf)
	bsw.mr.indexBlockSize = uint32(len(bsw.packedIndexBlockBuf))
	bsw.mr.indexBlockOffset = bsw.indexBlockOffset
	bsw.indexBlockOffset += uint64(bsw.mr.indexBlockSize)
	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]

	// Write metaindexRow.
	bsw.unpackedMetaindexBuf = bsw.mr.Marshal(bsw.unpackedMetaindexBuf)
	bsw.mr.Reset()

	// Notify that the next call to WriteBlock must catch the first item.
	bsw.mrFirstItemCaught = false
}

func getBlockStreamWriter() *blockStreamWriter {
	v := bswPool.Get()
	if v == nil {
		return &blockStreamWriter{}
	}
	return v.(*blockStreamWriter)
}

func putBlockStreamWriter(bsw *blockStreamWriter) {
	bsw.reset()
	bswPool.Put(bsw)
}

var bswPool sync.Pool
