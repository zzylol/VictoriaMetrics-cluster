package servers

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/cgroup"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/memory"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/vmselectsketchapi"
)

var (
	maxUniqueTimeseries = flag.Int("search.maxUniqueTimeseries", 0, "The maximum number of unique time series, which can be scanned during every query. "+
		"This allows protecting against heavy queries, which select unexpectedly high number of series. When set to zero, the limit is automatically calculated based on -search.maxConcurrentRequests (inversely proportional) and memory available to the process (proportional). See also -search.max* command-line flags at vmselect")
	maxTagKeys = flag.Int("search.maxTagKeys", 100e3, "The maximum number of tag keys returned per search. "+
		"See also -search.maxLabelsAPISeries and -search.maxLabelsAPIDuration")
	maxTagValues = flag.Int("search.maxTagValues", 100e3, "The maximum number of tag values returned per search. "+
		"See also -search.maxLabelsAPISeries and -search.maxLabelsAPIDuration")
	maxTagValueSuffixesPerSearch = flag.Int("search.maxTagValueSuffixesPerSearch", 100e3, "The maximum number of tag value suffixes returned from /metrics/find")
	maxConcurrentRequests        = flag.Int("search.maxConcurrentRequests", 2*cgroup.AvailableCPUs(), "The maximum number of concurrent vmselect requests "+
		"the vmstorage can process at -vmselectAddr. It shouldn't be high, since a single request usually saturates a CPU core, and many concurrently executed requests "+
		"may require high amounts of memory. See also -search.maxQueueDuration")
	maxQueueDuration = flag.Duration("search.maxQueueDuration", 10*time.Second, "The maximum time the incoming vmselect request waits for execution "+
		"when -search.maxConcurrentRequests limit is reached")

	disableRPCCompression = flag.Bool("rpc.disableCompression", false, "Whether to disable compression of the data sent from vmstorage to vmselect. "+
		"This reduces CPU usage at the cost of higher network bandwidth usage")
	denyQueriesOutsideRetention = flag.Bool("denyQueriesOutsideRetention", false, "Whether to deny queries outside of the configured -retentionPeriod. "+
		"When set, then /api/v1/query_range would return '503 Service Unavailable' error for queries with 'from' value outside -retentionPeriod. "+
		"This may be useful when multiple data sources with distinct retentions are hidden behind query-tee")
)

var (
	maxUniqueTimeseriesValue     int
	maxUniqueTimeseriesValueOnce sync.Once
)

// NewVMSelectServer starts new server at the given addr, which serves vmselect requests from the given s.
func NewVMSelectServer(addr string, s *sketch.Sketch) (*vmselectsketchapi.Server, error) {
	api := &vmsketchAPI{
		s: s,
	}
	limits := vmselectsketchapi.Limits{
		MaxLabelNames:                 *maxTagKeys,
		MaxLabelValues:                *maxTagValues,
		MaxTagValueSuffixes:           *maxTagValueSuffixesPerSearch,
		MaxConcurrentRequests:         *maxConcurrentRequests,
		MaxConcurrentRequestsFlagName: "search.maxConcurrentRequests",
		MaxQueueDuration:              *maxQueueDuration,
		MaxQueueDurationFlagName:      "search.maxQueueDuration",
	}
	return vmselectsketchapi.NewServer(addr, api, limits, *disableRPCCompression)
}

// vmsketchAPI impelements vmselectsketchapi.API
type vmsketchAPI struct {
	s *sketch.Sketch
}

func (api *vmsketchAPI) SeriesCount(_ *querytracer.Tracer, accountID, projectID uint32, deadline uint64) (uint64, error) {
	return api.s.GetSeriesCount(accountID, projectID, deadline)
}

func (api *vmsketchAPI) SketchCacheStatus(qt *querytracer.Tracer, sq *sketch.SearchQuery, deadline uint64) (*sketch.SketchCacheStatus, error) {
	return api.s.GetSketchCacheStatus(qt, deadline)
}

func (api *vmsketchAPI) DeleteSeries(qt *querytracer.Tracer, sq *sketch.SearchQuery, deadline uint64) (int, error) {
	if len(sq.MetricNameRaws) == 0 {
		return 0, fmt.Errorf("missing metric names")
	}
	return api.s.DeleteSeries(qt, sq.MetricNameRaws, deadline)
}

func (api *vmsketchAPI) RegisterMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow, _ uint64) error {
	qtChild := qt.NewChild("RegisterMetricNames")
	qtChild.Done()
	api.s.RegisterMetricNames(qt, mrs)
	return nil
}

func (api *vmsketchAPI) RegisterMetricNameFuncName(qt *querytracer.Tracer, mrs []storage.MetricRow, funcName string, window int64, item_window int64, deadline uint64) error {
	qtChild := qt.NewChild("RegisterMetricNameFuncName=%q", funcName)
	qtChild.Done()
	return api.s.RegisterMetricNameFuncName(mrs, funcName, window, item_window)
}

func (api *vmsketchAPI) SearchAndEval(qt *querytracer.Tracer, sq *sketch.SearchQuery, _ uint64) ([]*sketch.Timeseries, bool, error) {
	return api.s.SearchAndEval(qt, sq.MetricNameRaws, sq.MinTimestamp, sq.MaxTimestamp, sq.FuncNameID, sq.Args, sq.MaxMetrics)
}

func getMaxMetrics(sq *sketch.SearchQuery) int {
	maxMetrics := sq.MaxMetrics
	maxMetricsLimit := *maxUniqueTimeseries
	if maxMetricsLimit <= 0 {
		maxMetricsLimit = GetMaxUniqueTimeSeries()
	}
	if maxMetrics <= 0 || maxMetrics > maxMetricsLimit {
		maxMetrics = maxMetricsLimit
	}
	return maxMetrics
}

// GetMaxUniqueTimeSeries returns the max metrics limit calculated by available resources.
// The calculation is split into calculateMaxUniqueTimeSeriesForResource for unit testing.
func GetMaxUniqueTimeSeries() int {
	maxUniqueTimeseriesValueOnce.Do(func() {
		maxUniqueTimeseriesValue = *maxUniqueTimeseries
		if maxUniqueTimeseriesValue <= 0 {
			maxUniqueTimeseriesValue = calculateMaxUniqueTimeSeriesForResource(*maxConcurrentRequests, memory.Remaining())
		}
	})
	return maxUniqueTimeseriesValue
}

// calculateMaxUniqueTimeSeriesForResource calculate the max metrics limit calculated by available resources.
func calculateMaxUniqueTimeSeriesForResource(maxConcurrentRequests, remainingMemory int) int {
	if maxConcurrentRequests <= 0 {
		// This line should NOT be reached unless the user has set an incorrect `search.maxConcurrentRequests`.
		// In such cases, fallback to unlimited.
		logger.Warnf("limiting -search.maxUniqueTimeseries to %v because -search.maxConcurrentRequests=%d.", 2e9, maxConcurrentRequests)
		return 2e9
	}

	// Calculate the max metrics limit for a single request in the worst-case concurrent scenario.
	// The approximate size of 1 unique series that could occupy in the vmstorage is 200 bytes.
	mts := remainingMemory / 200 / maxConcurrentRequests
	logger.Infof("limiting -search.maxUniqueTimeseries to %d according to -search.maxConcurrentRequests=%d and remaining memory=%d bytes. To increase the limit, reduce -search.maxConcurrentRequests or increase memory available to the process.", mts, maxConcurrentRequests, remainingMemory)
	return mts
}
