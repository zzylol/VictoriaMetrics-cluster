package streamaggr

import (
	"math"
	"sync"
	"time"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fasttime"
	"github.com/VictoriaMetrics/metrics"
)

// histogramBucketAggrState calculates output=histogram_bucket, e.g. VictoriaMetrics histogram over input samples.
type histogramBucketAggrState struct {
	m sync.Map

	stalenessSecs uint64
}

type histogramBucketStateValue struct {
	mu             sync.Mutex
	h              metrics.Histogram
	deleteDeadline uint64
	deleted        bool
}

func newHistogramBucketAggrState(stalenessInterval time.Duration) *histogramBucketAggrState {
	stalenessSecs := roundDurationToSecs(stalenessInterval)
	return &histogramBucketAggrState{
		stalenessSecs: stalenessSecs,
	}
}

func (as *histogramBucketAggrState) pushSamples(samples []pushSample) {
	currentTime := fasttime.UnixTimestamp()
	deleteDeadline := currentTime + as.stalenessSecs
	for i := range samples {
		s := &samples[i]
		outputKey := getOutputKey(s.key)

	again:
		v, ok := as.m.Load(outputKey)
		if !ok {
			// The entry is missing in the map. Try creating it.
			v = &histogramBucketStateValue{}
			outputKey = bytesutil.InternString(outputKey)
			vNew, loaded := as.m.LoadOrStore(outputKey, v)
			if loaded {
				// Use the entry created by a concurrent goroutine.
				v = vNew
			}
		}
		sv := v.(*histogramBucketStateValue)
		sv.mu.Lock()
		deleted := sv.deleted
		if !deleted {
			sv.h.Update(s.value)
			sv.deleteDeadline = deleteDeadline
		}
		sv.mu.Unlock()
		if deleted {
			// The entry has been deleted by the concurrent call to flushState
			// Try obtaining and updating the entry again.
			goto again
		}
	}
}

func (as *histogramBucketAggrState) removeOldEntries(currentTime uint64) {
	m := &as.m
	m.Range(func(k, v any) bool {
		sv := v.(*histogramBucketStateValue)

		sv.mu.Lock()
		deleted := currentTime > sv.deleteDeadline
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		}
		sv.mu.Unlock()

		if deleted {
			m.Delete(k)
		}
		return true
	})
}

func (as *histogramBucketAggrState) flushState(ctx *flushCtx) {
	currentTime := fasttime.UnixTimestamp()

	as.removeOldEntries(currentTime)

	m := &as.m
	m.Range(func(k, v any) bool {
		sv := v.(*histogramBucketStateValue)
		sv.mu.Lock()
		if !sv.deleted {
			key := k.(string)
			sv.h.VisitNonZeroBuckets(func(vmrange string, count uint64) {
				ctx.appendSeriesWithExtraLabel(key, "histogram_bucket", float64(count), "vmrange", vmrange)
			})
		}
		sv.mu.Unlock()
		return true
	})
}

func roundDurationToSecs(d time.Duration) uint64 {
	if d < 0 {
		return 0
	}
	secs := d.Seconds()
	return uint64(math.Ceil(secs))
}
