package storage

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/slicesutil"
)

// MetricName reperesents a metric name.
type MetricNameNoTenant struct {
	MetricGroup []byte

	// Tags are optional. They must be sorted by tag Key for canonical view.
	// Use SortTags method.
	Tags []Tag
}

// GetMetricName returns a MetricName from pool.
func GetMetricNameNoTenant() *MetricNameNoTenant {
	v := mnPoolNoTenant.Get()
	if v == nil {
		return &MetricNameNoTenant{}
	}
	return v.(*MetricNameNoTenant)
}

// PutMetricName returns mn to the pool.
func PutMetricNameNoTenant(mn *MetricNameNoTenant) {
	mn.Reset()
	mnPoolNoTenant.Put(mn)
}

var mnPoolNoTenant sync.Pool

// Reset resets the mn.
func (mn *MetricNameNoTenant) Reset() {
	mn.MetricGroup = mn.MetricGroup[:0]
	mn.Tags = mn.Tags[:0]
}

// MoveFrom moves src to mn.
//
// The src is reset after the call.
func (mn *MetricNameNoTenant) MoveFrom(src *MetricNameNoTenant) {
	*mn = *src
	*src = MetricNameNoTenant{}
}

// CopyFrom copies src to mn.
func (mn *MetricNameNoTenant) CopyFrom(src *MetricNameNoTenant) {
	if cap(mn.MetricGroup) > 0 {
		mn.MetricGroup = append(mn.MetricGroup[:0], src.MetricGroup...)
		mn.Tags = copyTags(mn.Tags[:0], src.Tags)
		return
	}

	// Pre-allocate a single byte slice for MetricGroup + all the tags.
	// This reduces the number of memory allocations for zero mn.
	size := len(src.MetricGroup)
	for i := range src.Tags {
		tag := &src.Tags[i]
		size += len(tag.Key)
		size += len(tag.Value)
	}
	b := make([]byte, 0, size)

	b = append(b, src.MetricGroup...)
	mn.MetricGroup = b[:len(b):len(b)]

	mn.Tags = make([]Tag, len(src.Tags))
	for i := range src.Tags {
		st := &src.Tags[i]
		dt := &mn.Tags[i]
		b = append(b, st.Key...)
		dt.Key = b[len(b)-len(st.Key) : len(b) : len(b)]
		b = append(b, st.Value...)
		dt.Value = b[len(b)-len(st.Value) : len(b) : len(b)]
	}
}

// AddTag adds new tag to mn with the given key and value.
func (mn *MetricNameNoTenant) AddTag(key, value string) {
	if key == string(metricGroupTagKey) {
		mn.MetricGroup = append(mn.MetricGroup, value...)
		return
	}
	tag := mn.addNextTag()
	tag.Key = append(tag.Key[:0], key...)
	tag.Value = append(tag.Value[:0], value...)
}

// AddTagBytes adds new tag to mn with the given key and value.
func (mn *MetricNameNoTenant) AddTagBytes(key, value []byte) {
	if string(key) == string(metricGroupTagKey) {
		mn.MetricGroup = append(mn.MetricGroup, value...)
		return
	}
	tag := mn.addNextTag()
	tag.Key = append(tag.Key[:0], key...)
	tag.Value = append(tag.Value[:0], value...)
}

func (mn *MetricNameNoTenant) addNextTag() *Tag {
	if len(mn.Tags) < cap(mn.Tags) {
		mn.Tags = mn.Tags[:len(mn.Tags)+1]
	} else {
		mn.Tags = append(mn.Tags, Tag{})
	}
	return &mn.Tags[len(mn.Tags)-1]
}

// ResetMetricGroup resets mn.MetricGroup
func (mn *MetricNameNoTenant) ResetMetricGroup() {
	mn.MetricGroup = mn.MetricGroup[:0]
}

// RemoveTagsOn removes all the tags not included to onTags.
func (mn *MetricNameNoTenant) RemoveTagsOn(onTags []string) {
	if !hasTag(onTags, metricGroupTagKey) {
		mn.ResetMetricGroup()
	}
	tags := mn.Tags
	mn.Tags = mn.Tags[:0]
	if len(onTags) == 0 {
		return
	}
	for i := range tags {
		tag := &tags[i]
		if hasTag(onTags, tag.Key) {
			mn.AddTagBytes(tag.Key, tag.Value)
		}
	}
}

// RemoveTag removes a tag with the given tagKey
func (mn *MetricNameNoTenant) RemoveTag(tagKey string) {
	if tagKey == "__name__" {
		mn.ResetMetricGroup()
		return
	}
	tags := mn.Tags
	mn.Tags = mn.Tags[:0]
	for i := range tags {
		tag := &tags[i]
		if string(tag.Key) != tagKey {
			mn.AddTagBytes(tag.Key, tag.Value)
		}
	}
}

// RemoveTagsIgnoring removes all the tags included in ignoringTags.
func (mn *MetricNameNoTenant) RemoveTagsIgnoring(ignoringTags []string) {
	if len(ignoringTags) == 0 {
		return
	}
	if hasTag(ignoringTags, metricGroupTagKey) {
		mn.ResetMetricGroup()
	}
	tags := mn.Tags
	mn.Tags = mn.Tags[:0]
	for i := range tags {
		tag := &tags[i]
		if !hasTag(ignoringTags, tag.Key) {
			mn.AddTagBytes(tag.Key, tag.Value)
		}
	}
}

// GetTagValue returns tag value for the given tagKey.
func (mn *MetricNameNoTenant) GetTagValue(tagKey string) []byte {
	if tagKey == "__name__" {
		return mn.MetricGroup
	}
	tags := mn.Tags
	for i := range tags {
		tag := &tags[i]
		if string(tag.Key) == tagKey {
			return tag.Value
		}
	}
	return nil
}

// SetTags sets tags from src with keys matching addTags.
//
// It adds prefix to copied label names.
// skipTags contains a list of tags, which must be skipped.
func (mn *MetricNameNoTenant) SetTags(addTags []string, prefix string, skipTags []string, src *MetricNameNoTenant) {
	if len(addTags) == 1 && addTags[0] == "*" {
		// Special case for copying all the tags except of skipTags from src to mn.
		mn.setAllTags(prefix, skipTags, src)
		return
	}
	bb := bbPool.Get()
	for _, tagName := range addTags {
		if containsString(skipTags, tagName) {
			continue
		}
		if tagName == string(metricGroupTagKey) {
			mn.MetricGroup = append(mn.MetricGroup[:0], src.MetricGroup...)
			continue
		}
		var srcTag *Tag
		for i := range src.Tags {
			t := &src.Tags[i]
			if string(t.Key) == tagName {
				srcTag = t
				break
			}
		}
		if srcTag == nil {
			mn.RemoveTag(tagName)
			continue
		}
		bb.B = append(bb.B[:0], prefix...)
		bb.B = append(bb.B, tagName...)
		mn.SetTagBytes(bb.B, srcTag.Value)
	}
	bbPool.Put(bb)
}

// SetTagBytes sets tag with the given key to the given value.
func (mn *MetricNameNoTenant) SetTagBytes(key, value []byte) {
	for i := range mn.Tags {
		t := &mn.Tags[i]
		if string(t.Key) == string(key) {
			t.Value = append(t.Value[:0], value...)
			return
		}
	}
	mn.AddTagBytes(key, value)
}

func (mn *MetricNameNoTenant) setAllTags(prefix string, skipTags []string, src *MetricNameNoTenant) {
	bb := bbPool.Get()
	for _, tag := range src.Tags {
		if containsString(skipTags, bytesutil.ToUnsafeString(tag.Key)) {
			continue
		}
		bb.B = append(bb.B[:0], prefix...)
		bb.B = append(bb.B, tag.Key...)
		mn.SetTagBytes(bb.B, tag.Value)
	}
	bbPool.Put(bb)
}

// String returns user-readable representation of the metric name.
func (mn *MetricNameNoTenant) String() string {
	var mnCopy MetricNameNoTenant
	mnCopy.CopyFrom(mn)
	mnCopy.SortTags()
	var tags []string
	for i := range mnCopy.Tags {
		t := &mnCopy.Tags[i]
		tags = append(tags, fmt.Sprintf("%s=%q", t.Key, t.Value))
	}
	tagsStr := strings.Join(tags, ",")
	return fmt.Sprintf("%s{%s}", mnCopy.MetricGroup, tagsStr)
}

// Marshal appends marshaled mn to dst and returns the result.
//
// mn.SortTags must be called before calling this function
// in order to sort and de-duplcate tags.
func (mn *MetricNameNoTenant) Marshal(dst []byte) []byte {
	// Calculate the required size and pre-allocate space in dst
	dstLen := len(dst)
	requiredSize := len(mn.MetricGroup) + 1
	for i := range mn.Tags {
		tag := &mn.Tags[i]
		requiredSize += len(tag.Key) + len(tag.Value) + 2
	}
	dst = bytesutil.ResizeWithCopyMayOverallocate(dst, requiredSize)[:dstLen]

	// Marshal MetricGroup
	dst = marshalTagValue(dst, mn.MetricGroup)

	// Marshal tags.
	tags := mn.Tags
	for i := range tags {
		t := &tags[i]
		dst = t.Marshal(dst)
	}
	return dst
}

// UnmarshalString unmarshals mn from s
func (mn *MetricNameNoTenant) UnmarshalString(s string) error {
	b := bytesutil.ToUnsafeBytes(s)
	err := mn.Unmarshal(b)
	runtime.KeepAlive(s)
	return err
}

// Unmarshal unmarshals mn from src.
func (mn *MetricNameNoTenant) Unmarshal(src []byte) error {
	// Unmarshal MetricGroup.
	var err error
	src, mn.MetricGroup, err = unmarshalTagValue(mn.MetricGroup[:0], src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal MetricGroup: %w", err)
	}

	mn.Tags = mn.Tags[:0]
	for len(src) > 0 {
		tag := mn.addNextTag()
		var err error
		src, err = tag.Unmarshal(src)
		if err != nil {
			return fmt.Errorf("cannot unmarshal tag: %w", err)
		}
	}

	// There is no need in verifying for identical tag keys,
	// since they must be handled by MetricName.SortTags before calling MetricName.Marshal.

	return nil
}

// MarshalMetricNameRaw marshals labels to dst and returns the result.
//
// The result must be unmarshaled with MetricName.UnmarshalRaw
func MarshalMetricNameRawNoTenant(dst []byte, labels []prompbmarshal.Label) []byte {
	// Calculate the required space for dst.
	dstLen := len(dst)
	dstSize := dstLen
	for i := range labels {
		label := &labels[i]
		if len(label.Value) == 0 {
			// Skip labels without values, since they have no sense in prometheus.
			continue
		}
		if string(label.Name) == "__name__" {
			label.Name = label.Name[:0]
		}
		dstSize += len(label.Name)
		dstSize += len(label.Value)
		dstSize += 4
	}
	dst = bytesutil.ResizeWithCopyMayOverallocate(dst, dstSize)[:dstLen]

	// Marshal labels to dst.
	for i := range labels {
		label := &labels[i]
		if len(label.Value) == 0 {
			// Skip labels without values, since they have no sense in prometheus.
			continue
		}
		dst = MarshalStringFast(dst, label.Name)
		dst = MarshalStringFast(dst, label.Value)
	}
	return dst
}

// marshalRaw marshals mn to dst and returns the result.
//
// The results may be unmarshaled with MetricName.UnmarshalRaw.
//
// This function is for testing purposes. MarshalMetricNameRaw must be used
// in prod instead.
func (mn *MetricNameNoTenant) marshalRaw(dst []byte) []byte {
	dst = marshalBytesFast(dst, nil)
	dst = marshalBytesFast(dst, mn.MetricGroup)

	mn.SortTags()
	for i := range mn.Tags {
		tag := &mn.Tags[i]
		dst = marshalBytesFast(dst, tag.Key)
		dst = marshalBytesFast(dst, tag.Value)
	}
	return dst
}

// UnmarshalRaw unmarshals mn encoded with MarshalMetricNameRaw.
func (mn *MetricNameNoTenant) UnmarshalRaw(src []byte) error {
	mn.Reset()
	for len(src) > 0 {
		tail, key, err := unmarshalBytesFast(src)
		if err != nil {
			return fmt.Errorf("cannot decode key: %w", err)
		}
		src = tail

		tail, value, err := unmarshalBytesFast(src)
		if err != nil {
			return fmt.Errorf("cannot decode value: %w", err)
		}
		src = tail

		if len(key) == 0 {
			mn.MetricGroup = append(mn.MetricGroup[:0], value...)
		} else {
			mn.AddTagBytes(key, value)
		}
	}
	return nil
}

// SortTags sorts tags in mn to canonical form needed for storing in the index.
//
// The SortTags tries moving job-like tag to mn.Tags[0], while instance-like tag to mn.Tags[1].
// See commonTagKeys list for job-like and instance-like tags.
// This guarantees that indexdb entries for the same (job, instance) are located
// close to each other on disk. This reduces disk seeks and disk read IO when metrics
// for a particular job and/or instance are read from the disk.
//
// The function also de-duplicates tags with identical keys in mn. The last tag value
// for duplicate tags wins.
//
// Tags sorting is quite slow, so try avoiding it by caching mn
// with sorted tags.
func (mn *MetricNameNoTenant) SortTags() {
	if len(mn.Tags) == 0 {
		return
	}

	cts := getCanonicalTags()
	cts.tags = slicesutil.SetLength(cts.tags, len(mn.Tags))
	dst := cts.tags
	for i := range mn.Tags {
		tag := &mn.Tags[i]
		ct := &dst[i]
		ct.key = normalizeTagKey(tag.Key)
		ct.tag.copyFrom(tag)
	}
	cts.tags = dst

	// Use sort.Stable instead of sort.Sort in order to preserve the order of tags with duplicate keys.
	// The last tag value wins for tags with duplicate keys.
	// Use sort.Stable instead of sort.SliceStable, since sort.SliceStable allocates a lot.
	sort.Stable(&cts.tags)

	j := 0
	var prevKey []byte
	for i := range cts.tags {
		tag := &cts.tags[i].tag
		if j > 0 && bytes.Equal(tag.Key, prevKey) {
			// Overwrite the previous tag with duplicate key.
			j--
		} else {
			prevKey = tag.Key
		}
		mn.Tags[j].copyFrom(tag)
		j++
	}
	mn.Tags = mn.Tags[:j]

	putCanonicalTags(cts)
}

// Equal returns true if tag equals t
func (mn *MetricNameNoTenant) Equal(other *MetricNameNoTenant) bool {
	if len(mn.MetricGroup) != len(other.MetricGroup) || len(mn.Tags) != len(other.Tags) {
		return false
	}

	if !bytes.Equal(mn.MetricGroup, other.MetricGroup) {
		return false
	}

	if len(mn.Tags) == 0 {
		return true
	}

	for i := range mn.Tags {
		if string(mn.Tags[i].Key) != string(other.Tags[i].Key) || string(mn.Tags[i].Value) != string(other.Tags[i].Value) {
			return false
		}
	}
	return true
}
