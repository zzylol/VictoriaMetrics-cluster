package sketch

import (
	"fmt"
	"sync/atomic"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/syncwg"
)

// ErrDeadlineExceeded is returned when the request times out.
var ErrDeadlineExceeded = fmt.Errorf("deadline exceeded")

type SketchCacheStatus struct {
	TotalSeries uint64
}

// Sketch represents PromSketch instance.
type Sketch struct {
	rowsReceivedTotal atomic.Uint64
	rowsAddedTotal    atomic.Uint64

	tooSmallTimestampRows atomic.Uint64
	tooBigTimestampRows   atomic.Uint64
	invalidRawMetricNames atomic.Uint64

	timeseriesRepopulated atomic.Uint64
	timeseriesPreCreated  atomic.Uint64
	newTimeseriesCreated  atomic.Uint64

	// idbCurr contains the currently used indexdb.
	sketchCache *VMSketches

	// prefetchedMetricIDsDeadline is used for periodic reset of prefetchedMetricIDs in order to limit its size under high rate of creating new series.
	prefetchedMetricIDsDeadline atomic.Uint64

	stopCh chan struct{}

	// isReadOnly is set to true when the storage is in read-only mode.
	isReadOnly atomic.Bool
}

func MustOpenSketchCache() *Sketch {
	return &Sketch{
		sketchCache: NewVMSketches(),
	}
}

// IsReadOnly returns information is storage in read only mode
func (s *Sketch) IsReadOnly() bool {
	return s.isReadOnly.Load()
}

// GetSeriesCount returns the approximate number of unique time series for the given (accountID, projectID).
//
// It includes the deleted series too and may count the same series
func (s *Sketch) GetSeriesCount(accountID, projectID uint32, deadline uint64) (uint64, error) {
	return s.sketchCache.GetSeriesCount(), nil
}

func (s *Sketch) MustClose() {
	s.sketchCache = nil
}

// WG must be incremented before Storage call.
//
// Use syncwg instead of sync, since Add is called from concurrent goroutines.
var WG syncwg.WaitGroup

// AddRows adds mrs to the sketch cache.
//
// The caller should limit the number of concurrent calls to AddRows() in order to limit memory usage.
func (s *Sketch) AddRows(mrs []storage.MetricRow) error {
	WG.Add(1)

	var firstWarn error
	mn := storage.GetMetricName()
	defer storage.PutMetricName(mn)

	for i := range mrs {
		if err := mn.UnmarshalRaw(mrs[i].MetricNameRaw); err != nil {
			if firstWarn != nil {
				firstWarn = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", mrs[i].MetricNameRaw, err)
			}
		}

		err := s.sketchCache.AddRow(mn, mrs[i].Timestamp, mrs[i].Value)
		if err != nil && firstWarn != nil {
			firstWarn = fmt.Errorf("cannot add row to sketch cache MetricNameRaw %q: %w", mrs[i].MetricNameRaw, err)
		}
	}
	WG.Done()
	return firstWarn
}

func (s *Sketch) AddRow(metricNameRaw []byte, timestamp int64, value float64) error {
	mn := storage.GetMetricName()
	defer storage.PutMetricName(mn)
	if err := mn.UnmarshalRaw(metricNameRaw); err != nil {
		return fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
	}

	// fmt.Println(mn, timestamp, value)
	return s.sketchCache.AddRow(mn, timestamp, value)
}

func (s *Sketch) GetSketchCacheStatus(qt *querytracer.Tracer, deadline uint64) (*SketchCacheStatus, error) {
	return &SketchCacheStatus{
		TotalSeries: s.sketchCache.GetSeriesCount(),
	}, nil
}

// RegisterMetricNames registers all the metrics from mrs in the storage.
func (s *Sketch) RegisterMetricNameFuncName(mrs []storage.MetricRow, funcName string, window int64, item_window int64) error {
	WG.Add(1)
	var firstWarn error
	for _, mr := range mrs {
		mn := storage.GetMetricName()
		defer storage.PutMetricName(mn)
		if err := mn.UnmarshalRaw(mr.MetricNameRaw); err != nil {
			// Do not stop adding rows on error - just skip invalid row.
			// This guarantees that invalid rows don't prevent
			// from adding valid rows into the storage.
			if firstWarn == nil {
				firstWarn = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", mr.MetricNameRaw, err)
			}
			continue
		}
		mn.SortTags()
		err := s.sketchCache.NewVMSketchCacheInstance(mn, funcName, window, item_window)
		if err != nil {
			// Do not stop adding rows on error - just skip invalid row.
			// This guarantees that invalid rows don't prevent
			// from adding valid rows into the storage.
			if firstWarn == nil {
				firstWarn = fmt.Errorf("cannot add row to sketch cache MetricNameRaw %q: %w", mr.MetricNameRaw, err)
			}
		}
	}

	WG.Done()
	return firstWarn
}

func (s *Sketch) RegisterMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow) error {
	return s.sketchCache.RegisterMetricNames(mrs)
}

// Result is a single timeseries result.
//
// Search returns Result slice.
type SketchResult struct {
	MetricName *storage.MetricName
	sketchIns  *SketchInstances
}

// Results holds results returned from ProcessSearchQuery.
type SketchResults struct {
	deadline   searchutils.Deadline
	sketchInss []SketchResult
}

func (s *Sketch) SearchTimeSeriesCoverage(start, end int64, mns []string, funcName string, maxMetrics int, deadline searchutils.Deadline) (*SketchResults, bool, error) {
	srs := &SketchResults{
		deadline:   deadline,
		sketchInss: make([]SketchResult, 0),
	}

	for _, mnstr := range mns {
		mn := &storage.MetricName{}
		if err := mn.Unmarshal(bytesutil.ToUnsafeBytes(mnstr)); err != nil {
			return nil, false, fmt.Errorf("cannot unmarshal metricName %q: %w", mnstr, err)
		}
		if deadline.Exceeded() {
			return nil, false, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
		}

		sketchIns, lookup := s.sketchCache.LookupMetricNameFuncNamesTimeRange(mn, funcNames, start, end)
		if sketchIns == nil {
			return nil, false, fmt.Errorf("sketchIns doesn't allocated")
		}
		if !lookup {
			fmt.Println(sketchIns.PrintMinMaxTimeRange(mn, funcNames[0]))
			return nil, false, fmt.Errorf("sketch cache doesn't cover metricName %q", mnstr)
		}
		srs.sketchInss = append(srs.sketchInss, SketchResult{sketchIns: sketchIns, MetricName: mn})
	}
	return srs, true, nil
}

func (s *Sketch) DeleteSeries(qt *querytracer.Tracer, MetricNameRaws [][]byte, deadline uint64) (int, error) {
	WG.Add(1)
	defer WG.Done()

	var total int = 0
	var count int = 0
	var err error
	for _, metricNameRaw := range MetricNameRaws {
		mn := storage.GetMetricName()
		defer storage.PutMetricName(mn)
		if err := mn.UnmarshalRaw(metricNameRaw); err != nil {
			err = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
		}
		mn.SortTags()
		count, err = s.sketchCache.DeleteSeries(mn)
		total += count
	}
	return total, err
}

func (s *Sketch) SearchAndEval(start, end int64, mns []string, funcName string, maxMetrics int, deadline searchutils.Deadline) {
	scs, isCovered, err := s.sketchCache.SearchTimeSeriesCoverage(start, end, mns, funcName, maxMetrics, deadline)

}
