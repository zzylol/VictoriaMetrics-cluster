package metricsql

import (
	"strings"
)

var transformFuncs = map[string]bool{
	"":                           true, // empty func is a synonym to union
	"abs":                        true,
	"absent":                     true,
	"acos":                       true,
	"acosh":                      true,
	"asin":                       true,
	"asinh":                      true,
	"atan":                       true,
	"atanh":                      true,
	"bitmap_and":                 true,
	"bitmap_or":                  true,
	"bitmap_xor":                 true,
	"buckets_limit":              true,
	"ceil":                       true,
	"clamp":                      true,
	"clamp_max":                  true,
	"clamp_min":                  true,
	"cos":                        true,
	"cosh":                       true,
	"day_of_month":               true,
	"day_of_week":                true,
	"day_of_year":                true,
	"days_in_month":              true,
	"deg":                        true,
	"drop_common_labels":         true,
	"drop_empty_series":          true,
	"end":                        true,
	"exp":                        true,
	"floor":                      true,
	"histogram_avg":              true,
	"histogram_quantile":         true,
	"histogram_quantiles":        true,
	"histogram_share":            true,
	"histogram_stddev":           true,
	"histogram_stdvar":           true,
	"hour":                       true,
	"interpolate":                true,
	"keep_last_value":            true,
	"keep_next_value":            true,
	"label_copy":                 true,
	"label_del":                  true,
	"label_graphite_group":       true,
	"label_join":                 true,
	"label_keep":                 true,
	"label_lowercase":            true,
	"label_map":                  true,
	"label_match":                true,
	"label_mismatch":             true,
	"label_move":                 true,
	"label_replace":              true,
	"label_set":                  true,
	"label_transform":            true,
	"label_uppercase":            true,
	"label_value":                true,
	"labels_equal":               true,
	"limit_offset":               true,
	"ln":                         true,
	"log2":                       true,
	"log10":                      true,
	"minute":                     true,
	"month":                      true,
	"now":                        true,
	"pi":                         true,
	"prometheus_buckets":         true,
	"rad":                        true,
	"rand":                       true,
	"rand_exponential":           true,
	"rand_normal":                true,
	"range_avg":                  true,
	"range_first":                true,
	"range_last":                 true,
	"range_linear_regression":    true,
	"range_mad":                  true,
	"range_max":                  true,
	"range_min":                  true,
	"range_normalize":            true,
	"range_quantile":             true,
	"range_stddev":               true,
	"range_stdvar":               true,
	"range_sum":                  true,
	"range_trim_outliers":        true,
	"range_trim_spikes":          true,
	"range_trim_zscore":          true,
	"range_zscore":               true,
	"remove_resets":              true,
	"round":                      true,
	"running_avg":                true,
	"running_max":                true,
	"running_min":                true,
	"running_sum":                true,
	"scalar":                     true,
	"sgn":                        true,
	"sin":                        true,
	"sinh":                       true,
	"smooth_exponential":         true,
	"sort":                       true,
	"sort_by_label":              true,
	"sort_by_label_desc":         true,
	"sort_by_label_numeric":      true,
	"sort_by_label_numeric_desc": true,
	"sort_desc":                  true,
	"sqrt":                       true,
	"start":                      true,
	"step":                       true,
	"tan":                        true,
	"tanh":                       true,
	"time":                       true,
	// "timestamp" has been moved to rollup funcs. See https://github.com/zzylol/VictoriaMetrics-cluster/issues/415
	"timezone_offset": true,
	"union":           true,
	"vector":          true,
	"year":            true,
}

// IsTransformFunc returns whether funcName is known transform function.
func IsTransformFunc(funcName string) bool {
	s := strings.ToLower(funcName)
	return transformFuncs[s]

}
