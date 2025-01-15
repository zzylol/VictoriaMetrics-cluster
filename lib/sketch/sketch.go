package sketch

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/cgroup"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
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

	TimeseriesRepopulated atomic.Uint64
	TimeseriesPreCreated  atomic.Uint64
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
	mn := storage.GetMetricNameNoTenant()
	defer storage.PutMetricNameNoTenant(mn)

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
	mn := storage.GetMetricNameNoTenant()
	defer storage.PutMetricNameNoTenant(mn)
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
func (s *Sketch) RegisterSingleMetricNameFuncName(mn *storage.MetricNameNoTenant, funcName string, window int64, item_window int64) error {
	WG.Add(1)
	var firstWarn error

	mn.SortTags()
	err := s.sketchCache.NewVMSketchCacheInstance(mn, funcName, window, item_window)
	if err != nil {
		// Do not stop adding rows on error - just skip invalid row.
		// This guarantees that invalid rows don't prevent
		// from adding valid rows into the storage.
		if firstWarn == nil {
			firstWarn = fmt.Errorf("cannot NewVMSketchCacheInstance to sketch cache with MetricNameRaw %q: %w", mn, err)
		}
	}

	WG.Done()
	return firstWarn
}

// RegisterMetricNames registers all the metrics from mrs in the storage.
func (s *Sketch) RegisterMetricNameFuncName(mrs []storage.MetricRow, funcName string, window int64, item_window int64) error {
	WG.Add(1)
	var firstWarn error
	for _, mr := range mrs {
		mn := storage.GetMetricNameNoTenant()
		defer storage.PutMetricNameNoTenant(mn)
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

func (s *Sketch) RegisterMetricName(qt *querytracer.Tracer, mn *storage.MetricNameNoTenant) error {
	return s.sketchCache.RegisterMetricName(mn)
}

func (s *Sketch) RegisterMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow) error {
	return s.sketchCache.RegisterMetricNames(mrs)
}

// Result is a single Timeseries result.
//
// Search returns Result slice.
type SketchResult struct {
	MetricName *storage.MetricNameNoTenant
	sketchIns  *SketchInstances
}

// Results holds results returned from ProcessSearchQuery.
type SketchResults struct {
	deadline   searchutils.Deadline
	sketchInss []SketchResult
}

func (s *Sketch) DeleteSeries(qt *querytracer.Tracer, MetricNameRaws [][]byte, deadline uint64) (int, error) {
	WG.Add(1)
	defer WG.Done()

	var total int = 0
	var count int = 0
	var err error
	for _, metricNameRaw := range MetricNameRaws {
		mn := storage.GetMetricNameNoTenant()
		defer storage.PutMetricNameNoTenant(mn)
		if err := mn.UnmarshalRaw(metricNameRaw); err != nil {
			err = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
		}
		mn.SortTags()
		count, err = s.sketchCache.DeleteSeries(mn)
		total += count
	}
	return total, err
}

func (s *Sketch) SearchTimeSeriesCoverage(start, end int64, mn *storage.MetricNameNoTenant, funcName string, maxMetrics int) (*SketchResult, bool, error) {
	sketchIns, lookup := s.sketchCache.LookupMetricNameFuncNamesTimeRange(mn, funcName, start, end)
	if sketchIns == nil {
		if err := s.RegisterSingleMetricNameFuncName(mn, funcName, (end-start)*4, (end-start)/100*4); err != nil {
			return nil, false, fmt.Errorf("failed to register metric name and function name with window")
		}
		sketchIns, lookup = s.sketchCache.LookupMetricNameFuncNamesTimeRange(mn, funcName, start, end)
	}

	if sketchIns == nil {
		return nil, false, fmt.Errorf("sketchIns doesn't allocated")
	}

	if !lookup {
		// fmt.Println(sketchIns.PrintMinMaxTimeRange(mn, funcName))
		return nil, false, fmt.Errorf("sketch cache doesn't cover metricName %s, time range: [%d, %d]", mn, start, end)
	}

	return &SketchResult{sketchIns: sketchIns, MetricName: mn}, true, nil
}

func (s *Sketch) SearchAndEval(qt *querytracer.Tracer, MetricNameRaws [][]byte, start, end int64, funcNameID uint32, sargs []float64, maxMetrics int) (results []*Timeseries, isCovered bool, err error) {
	// skip not supported rollup functions
	if funcNameID <= 0 || funcNameID >= 14 {
		return nil, false, nil
	}

	funcName := GetFuncName(funcNameID)

	logger.Errorf("in SearchAndEval, funcNameID=%d, funcName=%s", funcNameID, funcName)
	// logger.Errorf("metricnames =%s", MetricNameRaws)
	logger.Infof("sargs=%s", sargs)

	qt = qt.NewChild("rollup %s() over %d series", funcName, len(MetricNameRaws))
	defer qt.Done()

	if len(MetricNameRaws) == 0 {
		return nil, false, nil
	}

	srs := &SketchResults{}
	srs.sketchInss = make([]SketchResult, 0)

	for _, metricNameRaw := range MetricNameRaws {
		mn := storage.GetMetricNameNoTenant()
		defer storage.PutMetricNameNoTenant(mn)
		err := mn.UnmarshalRaw(metricNameRaw)
		logger.Infof("metricnameraw=%s", metricNameRaw)
		if err != nil {
			logger.Infof("metricname=%s with error", mn)
			fmt.Println("Error:", err)
			err = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
		} else {
			logger.Infof("metricname=%s", mn)
			fmt.Println("Error:", err)
		}

		mn.SortTags()

		sr, isCovered, err := s.SearchTimeSeriesCoverage(start, end, mn, funcName, maxMetrics)
		if err != nil || isCovered == false {
			return nil, isCovered, err
		}
		srs.sketchInss = append(srs.sketchInss, *sr)
	}

	logger.Errorf("Started Sketch Eval()...")

	workers := MaxWorkers()
	if workers > len(MetricNameRaws) {
		workers = len(MetricNameRaws)
	}

	seriesPerWorker := (len(MetricNameRaws) + workers - 1) / workers
	tss := make([]*Timeseries, 0)
	local_tss := make([][]*Timeseries, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			local_tss[i] = make([]*Timeseries, 0)
			startIdx := workerID * seriesPerWorker
			endIdx := startIdx + seriesPerWorker
			if endIdx > len(MetricNameRaws) {
				endIdx = len(MetricNameRaws)
			}
			for i := startIdx; i < endIdx; i++ {
				sr := &srs.sketchInss[i]
				value := sr.Eval(sr.MetricName, funcName, sargs, start, end, end)
				logger.Infof("sr.Eval=%s", value)
				local_tss[i] = append(local_tss[i], &Timeseries{*sr.MetricName, []float64{value}, []int64{end}, true})
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < workers; i++ {
		tss = append(tss, local_tss[i]...)
	}

	logger.Infof("in lib/sketch, tss num=%d", len(tss))
	return tss, true, nil
}

var (
	seriesReadPerQuery = metrics.NewHistogram(`vmsketch_series_read_per_query`)
)

var gomaxprocs = cgroup.AvailableCPUs()

var defaultMaxWorkersPerQuery = func() int {
	// maxWorkersLimit is the maximum number of CPU cores, which can be used in parallel
	// for processing an average query, without significant impact on inter-CPU communications.
	const maxWorkersLimit = 32

	n := gomaxprocs
	if n > maxWorkersLimit {
		n = maxWorkersLimit
	}
	return n
}()

// MaxWorkers returns the maximum number of concurrent goroutines, which can be used by RunParallel()
func MaxWorkers() int {
	n := defaultMaxWorkersPerQuery
	if n > gomaxprocs {
		// There is no sense in running more than gomaxprocs CPU-bound concurrent workers,
		// since this may worsen the query performance.
		n = gomaxprocs
	}
	return n
}

func (sr *SketchResult) Eval(mn *storage.MetricNameNoTenant, funcName string, args []float64, mint, maxt, cur_time int64) float64 {
	return sr.sketchIns.Eval(mn, funcName, args, mint, maxt, cur_time)
}

// Len returns the number of results in srs.
func (srs *SketchResults) Len() int {
	return len(srs.sketchInss)
}

// Cancel cancels srs work.
func (srs *SketchResults) Cancel() {
	srs.mustClose()
}

func (srs *SketchResults) mustClose() {
	// put something to memory pool
}
