package bytesutil

import (
	"fmt"
	"io"
	"sync"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/filestream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fs"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/slicesutil"
)

var (
	// Verify ByteBuffer implements the given interfaces.
	_ io.Writer           = &ByteBuffer{}
	_ fs.MustReadAtCloser = &ByteBuffer{}
	_ io.ReaderFrom       = &ByteBuffer{}

	// Verify reader implement filestream.ReadCloser interface.
	_ filestream.ReadCloser = &reader{}
)

// ByteBuffer implements a simple byte buffer.
type ByteBuffer struct {
	// B is the underlying byte slice.
	B []byte
}

// Path returns an unique id for bb.
func (bb *ByteBuffer) Path() string {
	return fmt.Sprintf("ByteBuffer/%p/mem", bb)
}

// Reset resets bb.
func (bb *ByteBuffer) Reset() {
	bb.B = bb.B[:0]
}

// Write appends p to bb.
func (bb *ByteBuffer) Write(p []byte) (int, error) {
	bb.B = append(bb.B, p...)
	return len(p), nil
}

// MustReadAt reads len(p) bytes starting from the given offset.
func (bb *ByteBuffer) MustReadAt(p []byte, offset int64) {
	if offset < 0 {
		logger.Panicf("BUG: cannot read at negative offset=%d", offset)
	}
	if offset > int64(len(bb.B)) {
		logger.Panicf("BUG: too big offset=%d; cannot exceed len(bb.B)=%d", offset, len(bb.B))
	}
	if n := copy(p, bb.B[offset:]); n < len(p) {
		logger.Panicf("BUG: EOF occurred after reading %d bytes out of %d bytes at offset %d", n, len(p), offset)
	}
}

// ReadFrom reads all the data from r to bb until EOF.
func (bb *ByteBuffer) ReadFrom(r io.Reader) (int64, error) {
	b := bb.B
	bLen := len(b)
	b = ResizeWithCopyMayOverallocate(b, 4*1024)
	b = b[:cap(b)]
	offset := bLen
	for {
		if free := len(b) - offset; free < offset {
			// grow slice by 30% similar to how Go does this
			// https://go.googlesource.com/go/+/2dda92ff6f9f07eeb110ecbf0fc2d7a0ddd27f9d
			// higher growth rates could consume excessive memory when reading big amounts of data.
			n := 1.3 * float64(len(b))
			b = slicesutil.SetLength(b, int(n))
		}
		n, err := r.Read(b[offset:])
		offset += n
		if err != nil {
			bb.B = b[:offset]
			if err == io.EOF {
				err = nil
			}
			return int64(offset - bLen), err
		}
	}
}

// MustClose closes bb for subsequent re-use.
func (bb *ByteBuffer) MustClose() {
	// Do nothing, since certain code rely on bb reading after MustClose call.
}

// NewReader returns new reader for the given bb.
func (bb *ByteBuffer) NewReader() filestream.ReadCloser {
	return &reader{
		bb: bb,
	}
}

type reader struct {
	bb *ByteBuffer

	// readOffset is the offset in bb.B for read.
	readOffset int
}

// Path returns an unique id for the underlying ByteBuffer.
func (r *reader) Path() string {
	return r.bb.Path()
}

// Read reads up to len(p) bytes from bb.
func (r *reader) Read(p []byte) (int, error) {
	var err error
	n := copy(p, r.bb.B[r.readOffset:])
	if n < len(p) {
		err = io.EOF
	}
	r.readOffset += n
	return n, err
}

// MustClose closes bb for subsequent re-use.
func (r *reader) MustClose() {
	r.bb = nil
	r.readOffset = 0
}

// ByteBufferPool is a pool of ByteBuffers.
type ByteBufferPool struct {
	p sync.Pool
}

// Get obtains a ByteBuffer from bbp.
func (bbp *ByteBufferPool) Get() *ByteBuffer {
	bbv := bbp.p.Get()
	if bbv == nil {
		return &ByteBuffer{}
	}
	return bbv.(*ByteBuffer)
}

// Put puts bb into bbp.
func (bbp *ByteBufferPool) Put(bb *ByteBuffer) {
	bb.Reset()
	bbp.p.Put(bb)
}
