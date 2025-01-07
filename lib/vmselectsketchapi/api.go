package vmselectsketchapi

import (
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
)

// API must implement vmselect API.
type API interface {

	// SeriesCount returns the number of series for the given (accountID, projectID).
	SeriesCount(qt *querytracer.Tracer, accountID, projectID uint32, deadline uint64) (uint64, error)

	// SketchCacheStatus returns sketch cache status for the given sq.
	SketchCacheStatus(qt *querytracer.Tracer, sq *sketch.SearchQuery, deadline uint64) (*sketch.SketchCacheStatus, error)

	// DeleteSeries deletes series matching the given sq.
	DeleteSeries(qt *querytracer.Tracer, sq *sketch.SearchQuery, deadline uint64) (int, error)

	// RegisterMetricNames registers the given mrs in the sketch.
	RegisterMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow, deadline uint64) error

	RegisterMetricNameFuncName(qt *querytracer.Tracer, mrs []storage.MetricRow, funcName string, window int64, item_window int64, deadline uint64) error

	SearchAndEval(qt *querytracer.Tracer, sq *sketch.SearchQuery, deadline uint64) ([]*sketch.Timeseries, bool, error)
}
