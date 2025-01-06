package sketch

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/cgroup"
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

// Result is a single Timeseries result.
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

func (s *Sketch) SearchTimeSeriesCoverage(start, end int64, mn *storage.MetricName, funcName string, maxMetrics int) (*SketchResult, bool, error) {
	sketchIns, lookup := s.sketchCache.LookupMetricNameFuncNamesTimeRange(mn, funcName, start, end)
	if sketchIns == nil {
		return nil, false, fmt.Errorf("sketchIns doesn't allocated")
	}
	if !lookup {
		fmt.Println(sketchIns.PrintMinMaxTimeRange(mn, funcName))
		return nil, false, fmt.Errorf("sketch cache doesn't cover metricName %q", mn)
	}

	return &SketchResult{sketchIns: sketchIns, MetricName: mn}, true, nil
}

func (s *Sketch) SearchAndEval(qt *querytracer.Tracer, MetricNameRaws [][]byte, start, end int64, funcNameID uint32, sargs []float64, maxMetrics int) (results []*Timeseries, isCovered bool, err error) {
	srs := &SketchResults{}

	if len(MetricNameRaws) == 0 {
		return nil, false, nil
	}

	srs.sketchInss = make([]SketchResult, 0)
	funcName := GetFuncName(funcNameID)
	for _, metricNameRaw := range MetricNameRaws {
		mn := storage.GetMetricName()
		defer storage.PutMetricName(mn)
		if err := mn.UnmarshalRaw(metricNameRaw); err != nil {
			err = fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", metricNameRaw, err)
		}
		mn.SortTags()
		sr, isCovered, err := s.SearchTimeSeriesCoverage(start, end, mn, funcName, maxMetrics)
		if err != nil || isCovered == false {
			return nil, isCovered, err
		}
		srs.sketchInss = append(srs.sketchInss, *sr)
	}

	srs.RunParallel(qt) // TODO: implement below to RunParallel
	f := func(sr *SketchResult, workerID uint) error {
		ts_results := make([]*Timeseries, 0)
		for _, sr := range srs.sketchInss {
			value := sr.Eval(sr.MetricName, funcName, sargs, start, end, end)
			ts_results = append(ts_results, &Timeseries{MetricName: *sr.MetricName, Values: []float64{value}, Timestamps: []int64{end}, denyReuse: true})
		}
		return nil
	}
	return ts_results, true, nil
	// TODO: define return value type: each MetricName has a float64 value
}

// These functions don't change physical meaning of input time series,
// so they don't drop metric name
var rollupFuncsKeepMetricName = map[string]bool{
	"avg_over_time":         true,
	"default_rollup":        true,
	"first_over_time":       true,
	"geomean_over_time":     true,
	"hoeffding_bound_lower": true,
	"hoeffding_bound_upper": true,
	"holt_winters":          true,
	"iqr_over_time":         true,
	"last_over_time":        true,
	"max_over_time":         true,
	"median_over_time":      true,
	"min_over_time":         true,
	"mode_over_time":        true,
	"predict_linear":        true,
	"quantile_over_time":    true,
	"quantiles_over_time":   true,
	"rollup":                true,
	"rollup_candlestick":    true,
	"timestamp_with_name":   true,
}

type TimeseriesWithPadding struct {
	tss []*Timeseries

	// The padding prevents false sharing on widespread platforms with
	// 128 mod (cache line size) = 0 .
	_ [128 - unsafe.Sizeof([]*Timeseries{})%128]byte
}

type TimeseriesByWorkerID struct {
	byWorkerID []TimeseriesWithPadding
}

func (tsw *TimeseriesByWorkerID) reset() {
	byWorkerID := tsw.byWorkerID
	for i := range byWorkerID {
		byWorkerID[i].tss = nil
	}
}

func getTimeseriesByWorkerID() *TimeseriesByWorkerID {
	v := timeseriesByWorkerIDPool.Get()
	if v == nil {
		return &TimeseriesByWorkerID{
			byWorkerID: make([]TimeseriesWithPadding, MaxWorkers()),
		}
	}
	return v.(*TimeseriesByWorkerID)
}

func putTimeseriesByWorkerID(tsw *TimeseriesByWorkerID) {
	tsw.reset()
	timeseriesByWorkerIDPool.Put(tsw)
}

var timeseriesByWorkerIDPool sync.Pool

func doRollupForTimeseriesSketch(funcName string, keepMetricNames bool, rargs []interface{}, rc *rollupConfig, tsDst *Timeseries, sr *SketchResult, sharedTimestamps []int64) uint64 {

	tsDst.MetricName.CopyFrom(sr.MetricName)
	if len(rc.TagValue) > 0 {
		tsDst.MetricName.AddTag("rollup", rc.TagValue)
	}
	if !keepMetricNames && !rollupFuncsKeepMetricName[funcName] {
		tsDst.MetricName.ResetMetricGroup()
	}
	var samplesScanned uint64
	tsDst.Values, samplesScanned = rc.DoSketch(tsDst.Values[:0], rargs, sr)
	tsDst.Timestamps = sharedTimestamps
	tsDst.denyReuse = true
	return samplesScanned
}

func (rc *rollupConfig) DoSketch(dstValues []float64, rargs []interface{}, sr *SketchResult) ([]float64, uint64) {
	return rc.doInternalSketch(dstValues, nil, rargs, sr)
}

func (rc *rollupConfig) doInternalSketch(dstValues []float64, tsm *timeseriesMap, rargs []interface{}, sr *SketchResult) ([]float64, uint64) {

	value := sr.Eval(MetricName, funcName, sargs, tStart, tEnd, tEnd)
	// fmt.Println("evaled value=", value)

	dstValues = append(dstValues, value)

	return dstValues, samplesScanned
}

func evalRollupSketchCache(qt *querytracer.Tracer, funcName string, keepMetricNames bool, rargs []interface{}, srs *SketchResults, rcs []*rollupConfig,
	sharedTimestamps []int64) ([]*Timeseries, error) {
	qt = qt.NewChild("rollup %s() over %d series; rollupConfigs=%s", funcName, srs.Len(), rcs)
	defer qt.Done()

	var samplesScannedTotal atomic.Uint64
	tsw := getTimeseriesByWorkerID()
	seriesByWorkerID := tsw.byWorkerID
	seriesLen := srs.Len() // number of Timeseries for querying
	err := srs.RunParallel(qt, func(sr *SketchResult, workerID uint) error {
		for _, rc := range rcs {
			var ts Timeseries
			samplesScanned := doRollupForTimeseriesSketch(funcName, keepMetricNames, rargs, rc, &ts, sr, sharedTimestamps)
			samplesScannedTotal.Add(samplesScanned)
			seriesByWorkerID[workerID].tss = append(seriesByWorkerID[workerID].tss, &ts)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	tss := make([]*Timeseries, 0, seriesLen*len(rcs))
	for i := range seriesByWorkerID {
		tss = append(tss, seriesByWorkerID[i].tss...)
	}
	putTimeseriesByWorkerID(tsw)

	qt.Printf("samplesScanned=%d", samplesScannedTotal.Load())
	return tss, nil
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

func (sr *SketchResult) Eval(mn *storage.MetricName, funcName string, args []float64, mint, maxt, cur_time int64) float64 {
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

type TimeseriesWork struct {
	mustStop *atomic.Bool
	deadline searchutils.Deadline
	sr       *SketchResult
	f        func(sr *SketchResult, workerID uint) error
	err      error
}

func (tsw *TimeseriesWork) do(workerID uint) error {

	if tsw.mustStop.Load() {
		return nil
	}

	if tsw.deadline.Exceeded() {
		tsw.mustStop.Store(true)
		return fmt.Errorf("timeout exceeded during query execution: %s", tsw.deadline.String())
	}

	if err := tsw.f(tsw.sr, workerID); err != nil {
		tsw.mustStop.Store(true)
		return err
	}

	return nil
}

func TimeseriesWorker(qt *querytracer.Tracer, workChs []chan *TimeseriesWork, workerID uint) {

	// Perform own work at first.
	rowsProcessed := 0
	seriesProcessed := 0
	ch := workChs[workerID]
	for tsw := range ch {
		tsw.err = tsw.do(workerID)
		seriesProcessed++
	}
	qt.Printf("own work processed: series=%d, samples=%d", seriesProcessed, rowsProcessed)

	// Then help others with the remaining work.
	rowsProcessed = 0
	seriesProcessed = 0
	for i := uint(1); i < uint(len(workChs)); i++ {
		idx := (i + workerID) % uint(len(workChs))
		ch := workChs[idx]
		for len(ch) > 0 {
			// Do not call runtime.Gosched() here in order to give a chance
			// the real owner of the work to complete it, since it consumes additional CPU
			// and slows down the code on systems with big number of CPU cores.
			// See https://github.com/zzylol/VictoriaMetrics-sketches/issues/3966#issuecomment-1483208419

			// It is expected that every channel in the workChs is already closed,
			// so the next line should return immediately.
			tsw, ok := <-ch
			if !ok {
				break
			}
			tsw.err = tsw.do(workerID)

			seriesProcessed++
		}
	}
	qt.Printf("others work processed: series=%d, samples=%d", seriesProcessed, rowsProcessed)

}

// RunParallel runs f in parallel for all the results from srs.
//
// f shouldn't hold references to rs after returning.
// workerID is the id of the worker goroutine that calls f. The workerID is in the range [0..MaxWorkers()-1].
// Data processing is immediately stopped if f returns non-nil error.
//
// srs becomes unusable after the call to RunParallel.
func (srs *SketchResults) RunParallel(qt *querytracer.Tracer, f func(sr *SketchResult, workerID uint) error) error {
	qt = qt.NewChild("parallel process of fetched sketch instances")
	defer srs.mustClose()

	rowsProcessedTotal, err := srs.runParallel(qt, f)
	seriesProcessedTotal := len(srs.sketchInss)

	seriesReadPerQuery.Update(float64(seriesProcessedTotal))

	qt.Donef("series=%d, samples=%d", seriesProcessedTotal, rowsProcessedTotal)

	return err
}

func (srs *SketchResults) runParallel(qt *querytracer.Tracer, f func(sr *SketchResult, workerID uint) error) (int, error) {
	tswsLen := len(srs.sketchInss)
	if tswsLen == 0 {
		// Nothing to process
		return 0, nil
	}

	var mustStop atomic.Bool
	initTimeseriesWork := func(tsw *TimeseriesWork, sr *SketchResult) {
		tsw.deadline = srs.deadline
		tsw.sr = sr
		tsw.f = f
		tsw.mustStop = &mustStop
	}
	maxWorkers := MaxWorkers()

	if maxWorkers == 1 || tswsLen == 1 {
		// It is faster to process time series in the current goroutine.
		var tsw TimeseriesWork

		rowsProcessedTotal := 0
		var err error
		for i := range srs.sketchInss {
			initTimeseriesWork(&tsw, &srs.sketchInss[i])
			err = tsw.do(0)

			if err != nil {
				break
			}
		}

		return rowsProcessedTotal, err
	}

	// Slow path - spin up multiple local workers for parallel data processing.
	// Do not use global workers pool, since it increases inter-CPU memory ping-poing,
	// which reduces the scalability on systems with many CPU cores.

	// Prepare the work for workers.
	tsws := make([]TimeseriesWork, len(srs.sketchInss))
	for i := range srs.sketchInss {
		initTimeseriesWork(&tsws[i], &srs.sketchInss[i])
	}

	// Prepare worker channels.
	workers := len(tsws)
	if workers > maxWorkers {
		workers = maxWorkers
	}
	itemsPerWorker := (len(tsws) + workers - 1) / workers
	workChs := make([]chan *TimeseriesWork, workers)
	for i := range workChs {
		workChs[i] = make(chan *TimeseriesWork, itemsPerWorker)
	}

	// Spread work among workers.
	for i := range tsws {
		idx := i % len(workChs)
		workChs[idx] <- &tsws[i]
	}
	// Mark worker channels as closed.
	for _, workCh := range workChs {
		close(workCh)
	}

	// Start workers and wait until they finish the work.
	var wg sync.WaitGroup
	for i := range workChs {
		wg.Add(1)
		qtChild := qt.NewChild("worker #%d", i)
		go func(workerID uint) {
			TimeseriesWorker(qtChild, workChs, workerID)
			qtChild.Done()
			wg.Done()
		}(uint(i))
	}
	wg.Wait()

	// Collect results.
	var firstErr error
	rowsProcessedTotal := 0
	for i := range tsws {
		tsw := &tsws[i]
		if tsw.err != nil && firstErr == nil {
			// Return just the first error, since other errors are likely duplicate the first error.
			firstErr = tsw.err
		}
	}
	return rowsProcessedTotal, firstErr
}
