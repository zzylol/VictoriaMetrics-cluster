package storage

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/decimal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
)

// rawRow represents raw timeseries row.
type rawRow struct {
	// TSID is time series id.
	TSID TSID

	// Timestamp is unix timestamp in milliseconds.
	Timestamp int64

	// Value is time series value for the given timestamp.
	Value float64

	// PrecisionBits is the number of the significant bits in the Value
	// to store. Possible values are [1..64].
	// 1 means max. 50% error, 2 - 25%, 3 - 12.5%, 64 means no error, i.e.
	// Value stored without information loss.
	PrecisionBits uint8
}

type rawRowsMarshaler struct {
	bsw blockStreamWriter

	auxTimestamps  []int64
	auxValues      []int64
	auxFloatValues []float64
}

func (rrm *rawRowsMarshaler) reset() {
	rrm.bsw.reset()

	rrm.auxTimestamps = rrm.auxTimestamps[:0]
	rrm.auxValues = rrm.auxValues[:0]
	rrm.auxFloatValues = rrm.auxFloatValues[:0]
}

// Use sort.Interface instead of sort.Slice in order to optimize rows swap.
type rawRowsSort []rawRow

func (rrs *rawRowsSort) Len() int { return len(*rrs) }
func (rrs *rawRowsSort) Less(i, j int) bool {
	x := *rrs
	if i < 0 || j < 0 || i >= len(x) || j >= len(x) {
		// This is no-op for compiler, so it doesn't generate panic code
		// for out of range access on x[i], x[j] below
		return false
	}
	a := &x[i]
	b := &x[j]
	ta := &a.TSID
	tb := &b.TSID

	// Manually inline TSID.Less here, since the compiler doesn't inline it yet :(
	if ta.AccountID != tb.AccountID {
		return ta.AccountID < tb.AccountID
	}
	if ta.ProjectID != tb.ProjectID {
		return ta.ProjectID < tb.ProjectID
	}
	if ta.MetricGroupID != tb.MetricGroupID {
		return ta.MetricGroupID < tb.MetricGroupID
	}
	if ta.JobID != tb.JobID {
		return ta.JobID < tb.JobID
	}
	if ta.InstanceID != tb.InstanceID {
		return ta.InstanceID < tb.InstanceID
	}
	if ta.MetricID != tb.MetricID {
		return ta.MetricID < tb.MetricID
	}
	return a.Timestamp < b.Timestamp
}
func (rrs *rawRowsSort) Swap(i, j int) {
	x := *rrs
	x[i], x[j] = x[j], x[i]
}

func (rrm *rawRowsMarshaler) marshalToInmemoryPart(mp *inmemoryPart, rows []rawRow) {
	if len(rows) == 0 {
		return
	}
	if uint64(len(rows)) >= 1<<32 {
		logger.Panicf("BUG: rows count must be smaller than 2^32; got %d", len(rows))
	}

	// Use the minimum compression level for first-level in-memory blocks,
	// since they are going to be re-compressed during subsequent merges.
	const compressLevel = -5 // See https://github.com/facebook/zstd/releases/tag/v1.3.4
	rrm.bsw.MustInitFromInmemoryPart(mp, compressLevel)

	ph := &mp.ph
	ph.Reset()

	// Sort rows by (TSID, Timestamp) if they aren't sorted yet.
	rrs := rawRowsSort(rows)
	if !sort.IsSorted(&rrs) {
		sort.Sort(&rrs)
	}

	// Group rows into blocks.
	var scale int16
	var rowsMerged atomic.Uint64
	r := &rows[0]
	tsid := &r.TSID
	precisionBits := r.PrecisionBits
	tmpBlock := getBlock()
	defer putBlock(tmpBlock)
	for i := range rows {
		r = &rows[i]
		if r.TSID.MetricID == tsid.MetricID && len(rrm.auxTimestamps) < maxRowsPerBlock {
			rrm.auxTimestamps = append(rrm.auxTimestamps, r.Timestamp)
			rrm.auxFloatValues = append(rrm.auxFloatValues, r.Value)
			continue
		}

		rrm.auxValues, scale = decimal.AppendFloatToDecimal(rrm.auxValues[:0], rrm.auxFloatValues)
		tmpBlock.Init(tsid, rrm.auxTimestamps, rrm.auxValues, scale, precisionBits)
		rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)

		tsid = &r.TSID
		precisionBits = r.PrecisionBits
		rrm.auxTimestamps = append(rrm.auxTimestamps[:0], r.Timestamp)
		rrm.auxFloatValues = append(rrm.auxFloatValues[:0], r.Value)
	}

	rrm.auxValues, scale = decimal.AppendFloatToDecimal(rrm.auxValues[:0], rrm.auxFloatValues)
	tmpBlock.Init(tsid, rrm.auxTimestamps, rrm.auxValues, scale, precisionBits)
	rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)
	if n := rowsMerged.Load(); n != uint64(len(rows)) {
		logger.Panicf("BUG: unexpected rowsMerged; got %d; want %d", n, len(rows))
	}
	rrm.bsw.MustClose()
}

func getRawRowsMarshaler() *rawRowsMarshaler {
	v := rrmPool.Get()
	if v == nil {
		return &rawRowsMarshaler{}
	}
	return v.(*rawRowsMarshaler)
}

func putRawRowsMarshaler(rrm *rawRowsMarshaler) {
	rrm.reset()
	rrmPool.Put(rrm)
}

var rrmPool sync.Pool
