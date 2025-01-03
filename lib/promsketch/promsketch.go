package promsketch

import (
	"fmt"
	"strings"
	"time"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/slicesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/stringsutil"
)

// TenantToken represents a tenant (accountID, projectID) pair.
type TenantToken struct {
	AccountID uint32
	ProjectID uint32
}

// String returns string representation of t.
func (t *TenantToken) String() string {
	return fmt.Sprintf("{accountID=%d, projectID=%d}", t.AccountID, t.ProjectID)
}

// Marshal appends marshaled t to dst and returns the result.
func (t *TenantToken) Marshal(dst []byte) []byte {
	dst = encoding.MarshalUint32(dst, t.AccountID)
	dst = encoding.MarshalUint32(dst, t.ProjectID)
	return dst
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
	AccountID uint32
	ProjectID uint32

	// TenantTokens and IsMultiTenant is artificial fields
	// they're only exist at runtime and cannot be transferred
	// via network calls for keeping communication protocol compatibility
	// TODO:@f41gh7 introduce breaking change to the protocol later
	// and use TenantTokens instead of AccountID and ProjectID
	TenantTokens  []TenantToken
	IsMultiTenant bool

	// The time range for searching time series
	MinTimestamp int64
	MaxTimestamp int64

	// Tag filters for the search query
	TagFilterss [][]TagFilter

	FuncName string

	// The maximum number of time series the search query can return.
	MaxMetrics int
}

func tagFiltersToString(tfs []TagFilter) string {
	a := make([]string, len(tfs))
	for i, tf := range tfs {
		a[i] = tf.String()
	}
	return "{" + strings.Join(a, ",") + "}"
}

// timestampToTime returns time representation of the given timestamp.
//
// The returned time is in UTC timezone.
func timestampToTime(timestamp int64) time.Time {
	return time.Unix(timestamp/1e3, (timestamp%1e3)*1e6).UTC()
}

// TimestampToHumanReadableFormat converts the given timestamp to human-readable format.
func TimestampToHumanReadableFormat(timestamp int64) string {
	t := timestampToTime(timestamp).UTC()
	return t.Format("2006-01-02T15:04:05.999Z")
}

// String returns string representation of the search query.
func (sq *SearchQuery) String() string {
	a := make([]string, len(sq.TagFilterss))
	for i, tfs := range sq.TagFilterss {
		a[i] = tagFiltersToString(tfs)
	}
	start := TimestampToHumanReadableFormat(sq.MinTimestamp)
	end := TimestampToHumanReadableFormat(sq.MaxTimestamp)
	if !sq.IsMultiTenant {
		return fmt.Sprintf("accountID=%d, projectID=%d, filters=%s, timeRange=[%s..%s]", sq.AccountID, sq.ProjectID, a, start, end)
	}

	tts := make([]string, len(sq.TenantTokens))
	for i, tt := range sq.TenantTokens {
		tts[i] = tt.String()
	}
	return fmt.Sprintf("tenants=[%s], filters=%s, timeRange=[%s..%s]", strings.Join(tts, ","), a, start, end)
}

// MarshaWithoutTenant appends marshaled sq without AccountID/ProjectID to dst and returns the result.
// It is expected that TenantToken is already marshaled to dst.
func (sq *SearchQuery) MarshaWithoutTenant(dst []byte) []byte {
	dst = encoding.MarshalVarInt64(dst, sq.MinTimestamp)
	dst = encoding.MarshalVarInt64(dst, sq.MaxTimestamp)
	dst = encoding.MarshalVarUint64(dst, uint64(len(sq.TagFilterss)))
	for _, tagFilters := range sq.TagFilterss {
		dst = encoding.MarshalVarUint64(dst, uint64(len(tagFilters)))
		for i := range tagFilters {
			dst = tagFilters[i].Marshal(dst)
		}
	}
	dst = encoding.MarshalUint32(dst, uint32(sq.MaxMetrics))
	return dst
}

// Unmarshal unmarshals sq from src and returns the tail.
func (sq *SearchQuery) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal AccountID: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.AccountID = encoding.UnmarshalUint32(src)
	src = src[4:]

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal ProjectID: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.ProjectID = encoding.UnmarshalUint32(src)
	src = src[4:]

	sq.TenantTokens = []TenantToken{{AccountID: sq.AccountID, ProjectID: sq.ProjectID}}
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

	tfssCount, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		return src, fmt.Errorf("cannot unmarshal the count of TagFilterss from uvarint")
	}
	src = src[nSize:]
	sq.TagFilterss = slicesutil.SetLength(sq.TagFilterss, int(tfssCount))

	for i := 0; i < int(tfssCount); i++ {
		tfsCount, nSize := encoding.UnmarshalVarUint64(src)
		if nSize <= 0 {
			return src, fmt.Errorf("cannot unmarshal the count of TagFilters from uvarint")
		}
		src = src[nSize:]

		tagFilters := sq.TagFilterss[i]
		tagFilters = slicesutil.SetLength(tagFilters, int(tfsCount))
		for j := 0; j < int(tfsCount); j++ {
			tail, err := tagFilters[j].Unmarshal(src)
			if err != nil {
				return tail, fmt.Errorf("cannot unmarshal TagFilter #%d: %w", j, err)
			}
			src = tail
		}
		sq.TagFilterss[i] = tagFilters
	}

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal MaxMetrics: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.MaxMetrics = int(encoding.UnmarshalUint32(src))
	src = src[4:]

	return src, nil
}

// NewSearchQuery creates new search query for the given args.
func NewSearchQuery(accountID, projectID uint32, start, end int64, tagFilterss [][]TagFilter, maxMetrics int) *SearchQuery {
	if start < 0 {
		// This is needed for https://github.com/zzylol/VictoriaMetrics-cluster/issues/5553
		start = 0
	}
	if maxMetrics <= 0 {
		maxMetrics = 2e9
	}
	return &SearchQuery{
		MinTimestamp: start,
		MaxTimestamp: end,
		TagFilterss:  tagFilterss,
		MaxMetrics:   maxMetrics,
		TenantTokens: []TenantToken{
			{
				AccountID: accountID,
				ProjectID: projectID,
			},
		},
	}
}

// TimeRange is time range.
type TimeRange struct {
	MinTimestamp int64
	MaxTimestamp int64
}

// GetTimeRange returns time range for the given sq.
func (sq *SearchQuery) GetTimeRange() TimeRange {
	return TimeRange{
		MinTimestamp: sq.MinTimestamp,
		MaxTimestamp: sq.MaxTimestamp,
	}
}
