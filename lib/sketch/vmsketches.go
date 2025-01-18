package sketch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
)

type UnivConfig struct {
	TopK_size int
	Row_no    int
	Col_no    int
	Layer     int
}

type EHUnivConfig struct {
	K                int64
	Time_window_size int64
	Univ_config      UnivConfig
}

type EHKLLConfig struct {
	K                int64
	Kll_k            int
	Time_window_size int64
}

type SamplingConfig struct {
	Sampling_rate    float64
	Time_window_size int64
	Max_size         int
}

// SketchConfig bundles sketch configurations for promsketch
type SketchConfig struct {
	// 	CM_config       CMConfig
	// CS_config      CSConfig
	// Univ_config    UnivConfig
	// SH_univ_config SHUnivConfig
	// 	SH_count_config SHCountConfig
	// EH_count_config EHCountConfig
	EH_univ_config EHUnivConfig
	EH_kll_config  EHKLLConfig
	// EH_dd_config    EHDDConfig
	// EffSum_config   EffSumConfig
	// EffSum2_config  EffSum2Config
	Sampling_config SamplingConfig
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

type TSId int

const (
	// DefaultStripeSize is the default number of entries to allocate in the stripeSeries hash map.
	DefaultStripeSize = 1 << 14
)

type SketchType int

const (
	SHUniv SketchType = iota + 1
	EHUniv
	EHCount
	EHKLL
	EHDD
	EffSum
	EffSum2
	USampling
)

// Each series maintain their own sketches
type SketchInstances struct {
	// shuniv *SmoothHistogramUnivMon
	ehuniv *ExpoHistogramUnivOptimized
	ehkll  *ExpoHistogramKLL
	// ehdd   *ExpoHistogramDD
	sampling *UniformSampling
}

var seps = []byte{'\xff'}

const sep = '\xff' // Used between labels in `Bytes` and `Hash`.

// This file implements MetricNames aligned with VictoriaMetrics, but using a hash table as timeseries indexing

type vmMemSeries struct {
	id              TSId
	mn              *storage.MetricNameNoTenant
	sketchInstances *SketchInstances // same as PromSketches
	oldestTimestamp int64
}

type VMSketchSeriesHashMap struct {
	unique    map[uint64]*vmMemSeries
	conflicts map[uint64][]*vmMemSeries
}

func (m *VMSketchSeriesHashMap) get(hash uint64, mn *storage.MetricNameNoTenant) *vmMemSeries {
	if s, found := m.unique[hash]; found {
		if s.mn.Equal(mn) {
			return s
		}
	}
	for _, s := range m.conflicts[hash] {
		if s.mn.Equal(mn) {
			return s
		}
	}
	return nil
}

func (m *VMSketchSeriesHashMap) set(hash uint64, s *vmMemSeries) {
	if existing, found := m.unique[hash]; !found || existing.mn.Equal(s.mn) {
		m.unique[hash] = s
		return
	}
	if m.conflicts == nil {
		m.conflicts = make(map[uint64][]*vmMemSeries)
	}
	l := m.conflicts[hash]
	for i, prev := range l {
		if prev.mn.Equal(s.mn) {
			l[i] = s
			return
		}
	}
	m.conflicts[hash] = append(l, s)
}

func (m *VMSketchSeriesHashMap) del(hash uint64, id TSId) {
	var rem []*vmMemSeries
	unique, found := m.unique[hash]
	switch {
	case !found: // Supplied hash is not stored.
		return
	case unique.id == id:
		conflicts := m.conflicts[hash]
		if len(conflicts) == 0 { // Exactly one series with this hash was stored
			delete(m.unique, hash)
			return
		}
		m.unique[hash] = conflicts[0] // First remaining series goes in 'unique'.
		rem = conflicts[1:]           // Keep the rest.
	default: // The series to delete is somewhere in 'conflicts'. Keep all the ones that don't match.
		for _, s := range m.conflicts[hash] {
			if s.id != id {
				rem = append(rem, s)
			}
		}
	}
	if len(rem) == 0 {
		delete(m.conflicts, hash)
	} else {
		m.conflicts[hash] = rem
	}
}

// sketchSeries holds series by ID and also by hash of their MetricName.
// ID-based lookups via getByID() are preferred over getByHash() for performance reasons.
type VMSketchSeries struct {
	size   int
	id     TSId
	hashes []VMSketchSeriesHashMap
	series []map[TSId]*vmMemSeries
	locks  []stripeLock
}

type VMSketches struct {
	lastSeriesID atomic.Uint64
	numSeries    atomic.Uint64
	series       *VMSketchSeries
}

func (s *VMSketchSeries) getByID(id TSId) *vmMemSeries {
	if s.size == 0 {
		return nil
	}
	i := uint64(id) & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *VMSketchSeries) getByHash(hash uint64, mn *storage.MetricNameNoTenant) *vmMemSeries {
	if s.size == 0 {
		return nil
	}
	i := hash & uint64(s.size-1)
	s.locks[i].RLock()
	series := s.hashes[i].get(hash, mn)
	s.locks[i].RUnlock()

	return series
}

func newVMSlidingHistorgrams(s *vmMemSeries, stype SketchType, sc *SketchConfig) error {
	if s.sketchInstances == nil {
		s.sketchInstances = &SketchInstances{}
	}

	if stype == USampling && s.sketchInstances.sampling == nil {
		s.sketchInstances.sampling = NewUniformSampling(sc.Sampling_config.Time_window_size, sc.Sampling_config.Sampling_rate, int(sc.Sampling_config.Max_size))
	}

	if stype == EHKLL && s.sketchInstances.ehkll == nil {
		s.sketchInstances.ehkll = ExpoInitKLL(sc.EH_kll_config.K, sc.EH_kll_config.Kll_k, sc.EH_kll_config.Time_window_size)
	}

	if stype == EHUniv && s.sketchInstances.ehuniv == nil {
		s.sketchInstances.ehuniv = ExpoInitUnivOptimized(sc.EH_univ_config.K, sc.EH_univ_config.Time_window_size)
	}

	return nil
}

func NewVMSketchSeries(stripeSize int) *VMSketchSeries {
	ss := &VMSketchSeries{ // TODO: use stripeSeries toreduce lock contention later
		size:   stripeSize,
		id:     0,
		hashes: make([]VMSketchSeriesHashMap, stripeSize),
		series: make([]map[TSId]*vmMemSeries, stripeSize),
		locks:  make([]stripeLock, stripeSize),
	}

	for i := range ss.series {
		ss.series[i] = map[TSId]*vmMemSeries{}
	}
	for i := range ss.hashes {
		ss.hashes[i] = VMSketchSeriesHashMap{
			unique:    map[uint64]*vmMemSeries{},
			conflicts: nil,
		}
	}
	return ss
}

func NewVMSketches() *VMSketches {
	ss := NewVMSketchSeries(DefaultStripeSize)
	vs := &VMSketches{
		series: ss,
	}
	vs.lastSeriesID.Store(0)
	vs.numSeries.Store(0)
	return vs
}

func newVMMemSeries(mn *storage.MetricNameNoTenant, id TSId) *vmMemSeries {
	s := &vmMemSeries{
		mn:              &storage.MetricNameNoTenant{},
		id:              id,
		sketchInstances: nil,
		oldestTimestamp: -1,
	}
	s.mn.CopyFrom(mn)
	return s
}

func newVMSketchInstance(series *vmMemSeries, stype SketchType, sc *SketchConfig) error {
	return newVMSlidingHistorgrams(series, stype, sc)
}

func (vs *VMSketches) NewVMSketchCacheInstance(mn *storage.MetricNameNoTenant, funcName string, time_window_size int64, item_window_size int64) error {
	mn.SortTags()
	hash := MetricNameHash(mn)
	series, _, _ := vs.getOrCreate(hash, mn)
	stypes := funcSketchMap[funcName]
	sc := SketchConfig{}

	for _, stype := range stypes {
		switch stype {
		case EHUniv:
			sc.EH_univ_config = EHUnivConfig{K: 20, Time_window_size: time_window_size}
		case USampling:
			sc.Sampling_config = SamplingConfig{Sampling_rate: 0.1, Time_window_size: time_window_size, Max_size: int(float64(item_window_size) * 0.1)}
			// fmt.Println("max_size:", sc.Sampling_config.Max_size)
		case EHKLL:
			sc.EH_kll_config = EHKLLConfig{K: 50, Time_window_size: time_window_size, Kll_k: 256}
		default:
			fmt.Println("[NewSketchCacheInstance] not supported sketch type")
		}
		err := newVMSketchInstance(series, stype, &sc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (vs *VMSketches) LookupAndUpdateWindowMetricNameFuncName(mn *storage.MetricNameNoTenant, funcName string, window int64) bool {
	mn.SortTags()
	hash := MetricNameHash(mn)
	series := vs.series.getByHash(hash, mn)
	if series == nil || series.sketchInstances == nil {
		return false
	}
	stypes := funcSketchMap[funcName]
	for _, stype := range stypes {
		switch stype {
		case EHUniv:
			if series.sketchInstances.ehuniv == nil {
				return false
			} else if series.sketchInstances.ehuniv.time_window_size < window {
				series.sketchInstances.ehuniv.UpdateWindow(window)
			}
		case EHKLL:
			if series.sketchInstances.ehkll == nil {
				return false
			} else if series.sketchInstances.ehkll.time_window_size < window {
				series.sketchInstances.ehkll.UpdateWindow(window)
			}
		case USampling:
			if series.sketchInstances.sampling == nil {
				return false
			} else if series.sketchInstances.sampling.Time_window_size < window {
				series.sketchInstances.sampling.UpdateWindow(window)
			}
		default:
			return false
		}
	}
	return true
}

func (vs *VMSketches) getOrCreate(hash uint64, mn *storage.MetricNameNoTenant) (*vmMemSeries, bool, error) {
	s := vs.series.getByHash(hash, mn)
	if s != nil {
		return s, false, nil
	}

	vs.series.id = vs.series.id + 1
	id := vs.series.id

	series := newVMMemSeries(mn, id)

	i := hash & uint64(vs.series.size-1)
	vs.series.locks[i].Lock()
	vs.series.hashes[i].set(hash, series)
	vs.series.locks[i].Unlock()

	i = uint64(id) & uint64(vs.series.size-1)
	vs.series.locks[i].Lock()
	vs.series.series[i][id] = series
	vs.series.locks[i].Unlock()

	return series, true, nil
}

func (vs *VMSketches) Stop() {

}

// Hash returns a hash value for the label set.
// Note: the result is not guaranteed to be consistent across different runs of Prometheus.
func MetricNameHash(mn *storage.MetricNameNoTenant) uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	for i, tag := range mn.Tags {
		if len(b)+len(tag.Key)+len(tag.Value)+2 >= cap(b) {
			// If MetricName entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range mn.Tags[i:] {
				_, _ = h.Write(tag.Key)
				_, _ = h.Write(seps)
				_, _ = h.Write(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, tag.Key...)
		b = append(b, sep)
		b = append(b, tag.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

func (vs *VMSketches) GetSeriesCount() uint64 {
	return vs.numSeries.Load()
}

func (vs *VMSketches) AddRow(mn *storage.MetricNameNoTenant, t int64, value float64) error {
	mn.SortTags()
	hash := MetricNameHash(mn)
	s := vs.series.getByHash(hash, mn)
	if s == nil || s.sketchInstances == nil {
		return nil // fmt.Errorf("not find timeseries")
	}

	if s.sketchInstances.ehkll != nil {
		if s.oldestTimestamp == -1 {
			s.oldestTimestamp = t
		}
		// logger.Infof("sketch addRow:%s, %d, %f", mn, t, value)
		s.sketchInstances.ehkll.Update(t, value)
	}

	if s.sketchInstances.sampling != nil {
		if s.oldestTimestamp == -1 {
			s.oldestTimestamp = t
		}
		// logger.Infof("sketch addRow:%s, %d, %f", mn, t, value)
		s.sketchInstances.sampling.Insert(t, value)
	}

	if s.sketchInstances.ehuniv != nil {
		if s.oldestTimestamp == -1 {
			s.oldestTimestamp = t
		}
		s.sketchInstances.ehuniv.Update(t, value)
	}

	return nil
}

func (si *SketchInstances) Eval(mn *storage.MetricNameNoTenant, funcName string, args []float64, mint, maxt, cur_time int64) float64 {
	sfunc := VMFunctionCalls[funcName]
	// logger.Infof("funcName=%s, sfunc=%s", funcName, sfunc)
	return sfunc(context.TODO(), si, args, mint, maxt, cur_time)
}

func (s *SketchInstances) PrintMinMaxTimeRange(mn *storage.MetricNameNoTenant, funcName string) (mint, maxt int64) {
	stype := funcSketchMap[funcName]

	switch stype[0] {
	case EHUniv:
		return s.ehuniv.GetMinTime(), s.ehuniv.GetMaxTime()
	case EHKLL:
		return s.ehkll.GetMinTime(), s.ehkll.GetMaxTime()
	case USampling:
		return s.sampling.GetMinTime(), s.sampling.GetMaxTime()
	default:
		return -1, -1
	}
}

func (vs *VMSketches) LookupMetricNameFuncNamesTimeRange(mn *storage.MetricNameNoTenant, funcName string, mint, maxt int64) (*SketchInstances, bool) {
	mn.SortTags()
	hash := MetricNameHash(mn)
	series := vs.series.getByHash(hash, mn)
	if series == nil || series.sketchInstances == nil {
		return nil, false
	}
	stypes := make([]SketchType, 0)

	stypes = append(stypes, funcSketchMap[funcName]...)

	startt := mint
	if series.oldestTimestamp != -1 && mint < series.oldestTimestamp {
		startt = series.oldestTimestamp
	}

	for _, stype := range stypes {
		switch stype {
		case EHUniv:
			if series.sketchInstances.ehuniv == nil {
				return nil, false
			} else if series.sketchInstances.ehuniv.Cover(startt, maxt) == false {
				return series.sketchInstances, false
			}
		case EHKLL:
			if series.sketchInstances.ehkll == nil {
				return nil, false
			} else if series.sketchInstances.ehkll.Cover(startt, maxt) == false {
				// logger.Infof("find EHKLL series, with time range: [%d, %d]", series.sketchInstances.ehkll.GetMinTime(), series.sketchInstances.ehkll.GetMaxTime())
				return series.sketchInstances, false
			} else {
				// logger.Infof("find EHKLL series, with time range: [%d, %d]", series.sketchInstances.ehkll.GetMinTime(), series.sketchInstances.ehkll.GetMaxTime())
			}
		case USampling:
			if series.sketchInstances.sampling == nil {
				return nil, false
			} else if series.sketchInstances.sampling.Cover(startt, maxt) == false {
				// logger.Infof("find Sampling series, with time range: [%d, %d]", series.sketchInstances.sampling.GetMinTime(), series.sketchInstances.sampling.GetMaxTime())
				return series.sketchInstances, false
			} else {
				// logger.Infof("find Sampling series, with time range: [%d, %d]", series.sketchInstances.sampling.GetMinTime(), series.sketchInstances.sampling.GetMaxTime())
			}
		default:
			return nil, false
		}
	}
	return series.sketchInstances, true
}

func (vs *VMSketches) OutputTimeseriesCoverage(mn *storage.MetricNameNoTenant, funcNames []string) {
	mn.SortTags()
	hash := MetricNameHash(mn)
	series := vs.series.getByHash(hash, mn)

	if series == nil || series.sketchInstances == nil {
		fmt.Println("[OutputTimeseriesCoverage] series not found", series)
		return
	}

	stypes := make([]SketchType, 0)
	for _, funcName := range funcNames {
		stypes = append(stypes, funcSketchMap[funcName]...)
	}

	for _, stype := range stypes {
		switch stype {
		case EHUniv:
			if series.sketchInstances.ehuniv == nil {
				fmt.Println("[OutputTimeseriesCoverage] EHuniv not found for series", mn)
			} else {
				fmt.Println("[OutputTimeseriesCoverage] funcName", "EHUniv found for series", mn, series.sketchInstances.ehuniv.time_window_size, series.sketchInstances.ehuniv.GetMaxTime())
			}
		case EHKLL:
			if series.sketchInstances.ehkll == nil {
				fmt.Println("[OutputTimeseriesCoverage] EHKLL not found for series", mn)
			} else {
				fmt.Println("[OutputTimeseriesCoverage] funcName", "EHKLL found for series", mn, series.sketchInstances.ehkll.time_window_size, series.sketchInstances.ehkll.GetMaxTime())
			}
		case USampling:
			if series.sketchInstances.sampling == nil {
				fmt.Println("[OutputTimeseriesCoverage] Sampling not found for series", mn)
			} else {
				fmt.Println("[OutputTimeseriesCoverage] funcName", "Sampling found for series", mn, series.sketchInstances.sampling.Time_window_size, series.sketchInstances.sampling.GetMaxTime())
			}
		default:
			continue
		}
	}
}

func (vs *VMSketches) RegisterMetricName(mn *storage.MetricNameNoTenant) error {
	mn.SortTags()
	_, _, err := vs.getOrCreate(MetricNameHash(mn), mn)
	if err != nil {
		return err
	}

	return nil
}

func (vs *VMSketches) RegisterMetricNames(mrs []storage.MetricRow) error {
	mn := storage.GetMetricNameNoTenant()
	defer storage.PutMetricNameNoTenant(mn)

	for i := range mrs {
		if err := mn.UnmarshalRaw(mrs[i].MetricNameRaw); err != nil {
			return fmt.Errorf("cannot umarshal MetricNameRaw %q: %w", mrs[i].MetricNameRaw, err)
		}
		mn.SortTags()
		_, _, err := vs.getOrCreate(MetricNameHash(mn), mn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (vs *VMSketches) DeleteSeries(mn *storage.MetricNameNoTenant) (int, error) {
	hash := MetricNameHash(mn)

	series := vs.series.getByHash(hash, mn)
	if series == nil {
		return 0, nil
	}
	i := uint64(series.id) & uint64(vs.series.size-1)
	vs.series.locks[i].Lock()
	vs.series.hashes[i].del(hash, series.id)
	delete(vs.series.series[i], series.id)
	vs.series.locks[i].Unlock()
	return 1, nil
}
