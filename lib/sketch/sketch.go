package sketch

import (
	"fmt"
	"strings"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/stringsutil"
)

type SketchCacheStatus struct {
	TotalSeries          uint64
	TotalLabelValuePairs uint64
}

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
