package sketch

import (
	"fmt"
	"strconv"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/slicesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/stringsutil"
)

// Search is a search for time series.
type Search struct {

	// tr contains time range used in the search.
	tr TimeRange

	MetricNameRaw [][]byte

	// deadline in unix timestamp seconds for the current search.
	deadline uint64

	err error

	needClosing bool

	loops int
}

// MetricSketch is a time series sketch instance for a single metric.
type MetricSketch struct {
	// MetricName is metric name for the given Block.
	MetricName []byte

	// SketchCache is a Sketch Cache for the given MetricName
	SketchCache VMSketchSeries
}

// TagFilter represents a single tag filter from SearchQuery.
type TagFilter struct {
	Key        []byte
	Value      []byte
	IsNegative bool
	IsRegexp   bool
}

// String returns string representation of tf.
func (tf *TagFilter) String() string {
	op := tf.getOp()
	value := stringsutil.LimitStringLen(string(tf.Value), 60)
	if len(tf.Key) == 0 {
		return fmt.Sprintf("__name__%s%q", op, value)
	}
	return fmt.Sprintf("%s%s%q", tf.Key, op, value)
}

func (tf *TagFilter) getOp() string {
	if tf.IsNegative {
		if tf.IsRegexp {
			return "!~"
		}
		return "!="
	}
	if tf.IsRegexp {
		return "=~"
	}
	return "="
}

// Marshal appends marshaled tf to dst and returns the result.
func (tf *TagFilter) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, tf.Key)
	dst = encoding.MarshalBytes(dst, tf.Value)

	x := 0
	if tf.IsNegative {
		x = 2
	}
	if tf.IsRegexp {
		x |= 1
	}
	dst = append(dst, byte(x))

	return dst
}

// Unmarshal unmarshals tf from src and returns the tail.
func (tf *TagFilter) Unmarshal(src []byte) ([]byte, error) {
	k, nSize := encoding.UnmarshalBytes(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal Key")
	}
	src = src[nSize:]
	tf.Key = append(tf.Key[:0], k...)

	v, nSize := encoding.UnmarshalBytes(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal Value")
	}
	src = src[nSize:]
	tf.Value = append(tf.Value[:0], v...)

	if len(src) < 1 {
		return src, fmt.Errorf("cannot unmarshal IsNegative+IsRegexp from empty src")
	}
	x := src[0]
	switch x {
	case 0:
		tf.IsNegative = false
		tf.IsRegexp = false
	case 1:
		tf.IsNegative = false
		tf.IsRegexp = true
	case 2:
		tf.IsNegative = true
		tf.IsRegexp = false
	case 3:
		tf.IsNegative = true
		tf.IsRegexp = true
	default:
		return src, fmt.Errorf("unexpected value for IsNegative+IsRegexp: %d; must be in the range [0..3]", x)
	}
	src = src[1:]

	return src, nil
}

// SearchQuery is used for sending search queries to from vmselect to vmsketch.
type SearchQuery struct {

	// The time range for searching time series
	MinTimestamp int64
	MaxTimestamp int64

	MetricNameRaws [][]byte

	FuncNameID uint32

	Args []float64 // rollup function arguments TODO: add to marshal and unmarshal methods

	// The maximum number of time series the search query can return.
	MaxMetrics int
}

// GetTimeRange returns time range for the given sq.
func (sq *SearchQuery) GetTimeRange() TimeRange {
	return TimeRange{
		MinTimestamp: sq.MinTimestamp,
		MaxTimestamp: sq.MaxTimestamp,
	}
}

// String returns string representation of the search query.
func (sq *SearchQuery) String() string {
	a := make([]string, len(sq.MetricNameRaws))
	for i, mn := range sq.MetricNameRaws {
		a[i] = string(mn)
	}
	start := TimestampToHumanReadableFormat(sq.MinTimestamp)
	end := TimestampToHumanReadableFormat(sq.MaxTimestamp)

	return fmt.Sprintf("MetricNameRaws=%s, timeRange=[%s..%s]", a, start, end)
}

// Marshal appends marshaled sq without AccountID/ProjectID to dst and returns the result.
// It is expected that TenantToken is already marshaled to dst.
func (sq *SearchQuery) Marshal(dst []byte) []byte {
	dst = encoding.MarshalVarInt64(dst, sq.MinTimestamp)
	dst = encoding.MarshalVarInt64(dst, sq.MaxTimestamp)

	dst = encoding.MarshalVarUint64(dst, uint64(len(sq.MetricNameRaws)))
	for _, mn := range sq.MetricNameRaws {
		dst = encoding.MarshalBytes(dst, mn)
	}

	dst = encoding.MarshalUint32(dst, sq.FuncNameID)

	dst = encoding.MarshalUint32(dst, uint32(len(sq.Args)))
	for _, arg := range sq.Args {
		dst = encoding.MarshalBytes(dst, []byte(strconv.FormatFloat(arg, 'f', -1, 64)))
		// logger.Infof("in SearchQuery Marhshal: %s, len=%d", []byte(strconv.FormatFloat(arg, 'f', -1, 64)), len([]byte(strconv.FormatFloat(arg, 'f', -1, 64))))
	}

	dst = encoding.MarshalUint32(dst, uint32(sq.MaxMetrics))
	return dst
}

// Unmarshal unmarshals sq from src and returns the tail.
func (sq *SearchQuery) Unmarshal(src []byte) ([]byte, error) {
	minTs, nSize := encoding.UnmarshalVarInt64(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal MinTimestamp from varint")
	}
	src = src[nSize:]
	sq.MinTimestamp = minTs

	maxTs, nSize := encoding.UnmarshalVarInt64(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal MaxTimestamp from varint")
	}
	src = src[nSize:]
	sq.MaxTimestamp = maxTs

	mnsCount, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal the count of MetricNameRaws from uvarint")
	}
	src = src[nSize:]
	sq.MetricNameRaws = slicesutil.SetLength(sq.MetricNameRaws, int(mnsCount))

	for i := 0; i < int(mnsCount); i++ {
		sq.MetricNameRaws[i], nSize = encoding.UnmarshalBytes(src)
		if nSize <= 0 {
			return src, fmt.Errorf("cannot unmarshal MetricNameRaw[%d] from bytes", i)
		}
		src = src[nSize:]
	}

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal FuncNameID from uint32")
	}
	funcNameID := encoding.UnmarshalUint32(src)
	sq.FuncNameID = funcNameID
	src = src[4:]

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal argsCount from uint32")
	}
	argsCount := encoding.UnmarshalUint32(src)
	src = src[4:]
	sq.Args = make([]float64, argsCount)
	var strArg []byte
	for i := 0; i < int(argsCount); i++ {
		strArg, nSize = encoding.UnmarshalBytes(src)
		// sq.Args[i], _ = strconv.ParseFloat(string(strArg), 64)
		if floatArg, err := strconv.ParseFloat(string(strArg), 64); err == nil {
			sq.Args[i] = floatArg
		} else {
			logger.Errorf("strArg=%s, err=%s, byte_size=%d", string(strArg), err, len(strArg))
			return src, fmt.Errorf("cannot unmarshal Args[%d] from string", i)
		}

		if nSize <= 0 {
			return src, fmt.Errorf("cannot unmarshal Args[%d] from bytes", i)
		}
		src = src[nSize:]
	}

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal MaxMetrics: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.MaxMetrics = int(encoding.UnmarshalUint32(src))
	src = src[4:]

	return src, nil
}

// NewSearchQuery creates new search query for the given args.
func NewSearchQuery(start, end int64, MetricNameRaws [][]byte, FuncNameID uint32, Args []float64, maxMetrics int) *SearchQuery {
	if start < 0 {
		// This is needed for https://github.com/zzylol/VictoriaMetrics-cluster/issues/5553
		start = 0
	}
	if maxMetrics <= 0 {
		maxMetrics = 2e9
	}
	return &SearchQuery{
		MinTimestamp:   start,
		MaxTimestamp:   end,
		MetricNameRaws: MetricNameRaws,
		FuncNameID:     FuncNameID,
		Args:           Args,
		MaxMetrics:     maxMetrics,
	}
}
