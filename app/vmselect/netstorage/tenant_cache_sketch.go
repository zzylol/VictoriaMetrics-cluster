package netstorage

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/timeutil"
)

var (
	tenantsCacheDurationSketch = flag.Duration("search.tenantCacheExpireDuration", 5*time.Minute, "The expiry duration for list of tenants for multi-tenant queries.")
)

// TenantsCached returns the list of tenants available in the sketch.
func TenantsCachedSketch(qt *querytracer.Tracer, tr sketch.TimeRange, deadline searchutils.Deadline) ([]sketch.TenantToken, error) {
	qt.Printf("fetching tenants on timeRange=%s", tr.String())

	initTenantsCacheVOnce.Do(func() {
		tenantsCacheVSketch = newTenantsCacheSketch(*tenantsCacheDuration)
	})

	cached := tenantsCacheVSketch.get(tr)
	qt.Printf("fetched %d tenants from cache", len(cached))
	if len(cached) > 0 {
		return cached, nil
	}
	tenants, err := TenantsSketch(qt, tr, deadline)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain tenants: %w", err)
	}

	qt.Printf("fetched %d tenants from storage", len(tenants))

	tt := make([]sketch.TenantToken, len(tenants))
	for i, t := range tenants {
		accountID, projectID, err := auth.ParseToken(t)
		if err != nil {
			return nil, fmt.Errorf("cannot parse tenant token %q: %w", t, err)
		}
		tt[i].AccountID = accountID
		tt[i].ProjectID = projectID
	}

	tenantsCacheVSketch.put(tr, tt)
	qt.Printf("put %d tenants into cache", len(tenants))

	return tt, nil
}

var (
	initTenantsCacheVOnceSketch sync.Once
	tenantsCacheVSketch         *tenantsCacheSketch
)

type tenantsCacheItemSketch struct {
	tenants []sketch.TenantToken
	tr      sketch.TimeRange
	expires time.Time
}

type tenantsCacheSketch struct {
	// items is used for intersection matches lookup
	items []*tenantsCacheItemSketch

	itemExpiration time.Duration

	requests atomic.Uint64
	misses   atomic.Uint64

	mu sync.Mutex
}

func newTenantsCacheSketch(expiration time.Duration) *tenantsCacheSketch {
	tc := &tenantsCacheSketch{
		items:          make([]*tenantsCacheItemSketch, 0),
		itemExpiration: expiration,
	}

	metrics.GetOrCreateGauge(`vm_cache_requests_total{type="multitenancy/tenants"}`, func() float64 {
		return float64(tc.Requests())
	})
	metrics.GetOrCreateGauge(`vm_cache_misses_total{type="multitenancy/tenants"}`, func() float64 {
		return float64(tc.Misses())
	})
	metrics.GetOrCreateGauge(`vm_cache_entries{type="multitenancy/tenants"}`, func() float64 {
		return float64(tc.Len())
	})

	return tc
}

func (tc *tenantsCacheSketch) cleanupLocked() {
	expires := time.Now().Add(tc.itemExpiration)
	itemsTmp := tc.items[:0]
	for _, item := range tc.items {
		if item.expires.After(expires) {
			itemsTmp = append(itemsTmp, item)
		}
	}

	tc.items = itemsTmp
}

func (tc *tenantsCacheSketch) put(tr sketch.TimeRange, tenants []sketch.TenantToken) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	alignTrToDaySketch(&tr)

	exp := time.Now().Add(timeutil.AddJitterToDuration(tc.itemExpiration))

	ci := &tenantsCacheItemSketch{
		tenants: tenants,
		tr:      tr,
		expires: exp,
	}

	tc.items = append(tc.items, ci)
}
func (tc *tenantsCacheSketch) Requests() uint64 {
	return tc.requests.Load()
}

func (tc *tenantsCacheSketch) Misses() uint64 {
	return tc.misses.Load()
}

func (tc *tenantsCacheSketch) Len() uint64 {
	tc.mu.Lock()
	n := len(tc.items)
	tc.mu.Unlock()
	return uint64(n)
}

func (tc *tenantsCacheSketch) get(tr sketch.TimeRange) []sketch.TenantToken {
	tc.requests.Add(1)

	alignTrToDaySketch(&tr)
	return tc.getInternal(tr)
}

func (tc *tenantsCacheSketch) getInternal(tr sketch.TimeRange) []sketch.TenantToken {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if len(tc.items) == 0 {
		return nil
	}
	ct := time.Now()

	result := make(map[sketch.TenantToken]struct{})
	cleanupNeeded := false
	for idx := range tc.items {
		ci := tc.items[idx]
		if ci.expires.Before(ct) {
			cleanupNeeded = true
			continue
		}

		if hasIntersectionSketch(tr, ci.tr) {
			for _, t := range ci.tenants {
				result[t] = struct{}{}
			}
		}
	}

	if cleanupNeeded {
		tc.cleanupLocked()
	}

	tenants := make([]sketch.TenantToken, 0, len(result))
	for t := range result {
		tenants = append(tenants, t)
	}

	return tenants
}

// alignTrToDay aligns the given time range to the day boundaries
// tr.minTimestamp will be set to the start of the day
// tr.maxTimestamp will be set to the end of the day
func alignTrToDaySketch(tr *sketch.TimeRange) {
	tr.MinTimestamp = timeutil.StartOfDay(tr.MinTimestamp)
	tr.MaxTimestamp = timeutil.EndOfDay(tr.MaxTimestamp)
}

// hasIntersection checks if there is any intersection of the given time ranges
func hasIntersectionSketch(a, b sketch.TimeRange) bool {
	return a.MinTimestamp <= b.MaxTimestamp && a.MaxTimestamp >= b.MinTimestamp
}
