package sketch

import (
	"fmt"
	"strings"
	"time"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/slicesutil"
)

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

func (tr *TimeRange) String() string {
	start := TimestampToHumanReadableFormat(tr.MinTimestamp)
	end := TimestampToHumanReadableFormat(tr.MaxTimestamp)
	return fmt.Sprintf("[%s..%s]", start, end)
}

// fromPartitionName initializes tr from the given partition name.
func (tr *TimeRange) fromPartitionName(name string) error {
	t, err := time.Parse("2006_01", name)
	if err != nil {
		return fmt.Errorf("cannot parse partition name %q: %w", name, err)
	}
	tr.fromPartitionTime(t)
	return nil
}

// fromPartitionTimestamp initializes tr from the given partition timestamp.
func (tr *TimeRange) fromPartitionTimestamp(timestamp int64) {
	t := timestampToTime(timestamp)
	tr.fromPartitionTime(t)
}

// fromPartitionTime initializes tr from the given partition time t.
func (tr *TimeRange) fromPartitionTime(t time.Time) {
	y, m, _ := t.UTC().Date()
	minTime := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	maxTime := time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC)
	tr.MinTimestamp = minTime.Unix() * 1e3
	tr.MaxTimestamp = maxTime.Unix()*1e3 - 1
}

const msecPerDay = 24 * 3600 * 1000

const msecPerHour = 3600 * 1000

// timestampToPartitionName returns partition name for the given timestamp.
func timestampToPartitionName(timestamp int64) string {
	t := timestampToTime(timestamp)
	return t.Format("2006_01")
}

// timestampFromTime returns timestamp value for the given time.
func timestampFromTime(t time.Time) int64 {
	// There is no need in converting t to UTC, since UnixNano must
	// return the same value for any timezone.
	return t.UnixNano() / 1e6
}

func dateToString(date uint64) string {
	if date == 0 {
		return "1970-01-01"
	}
	t := time.Unix(int64(date*24*3600), 0).UTC()
	return t.Format("2006-01-02")
}
