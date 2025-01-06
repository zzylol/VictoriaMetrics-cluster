package netstorage

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fasttime"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/handshake"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/httpserver"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/netutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
)

type sketchNodesBucket struct {
	ms  *metrics.Set
	sns []*sketchNode
}

var sketchNodes atomic.Pointer[sketchNodesBucket]

func getSketchNodesBucket() *sketchNodesBucket {
	return sketchNodes.Load()
}

func setSketchNodesBucket(snb *sketchNodesBucket) {
	sketchNodes.Store(snb)
}

func getSketchNodes() []*sketchNode {
	snb := getSketchNodesBucket()
	return snb.sns
}

func newSketchNode(ms *metrics.Set, group *sketchNodesGroup, addr string) *sketchNode {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		// Automatically add missing port.
		addr += ":8401"
	}
	// There is no need in requests compression, since vmselect requests are usually very small.
	connPool := netutil.NewConnPool(ms, "vmselect", addr, handshake.VMSelectClient, 0, *vmstorageDialTimeout, *vmstorageUserTimeout)

	sn := &sketchNode{
		group:    group,
		connPool: connPool,

		concurrentQueries: ms.NewCounter(fmt.Sprintf(`vm_concurrent_queries{name="vmselect", addr=%q}`, addr)),

		registerMetricNamesRequests: ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="registerMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		registerMetricNamesErrors:   ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="registerMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		deleteSeriesRequests:        ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="deleteSeries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		deleteSeriesErrors:          ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="deleteSeries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		labelNamesRequests:          ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="labelNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		labelNamesErrors:            ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="labelNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		labelValuesRequests:         ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="labelValues", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		labelValuesErrors:           ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="labelValues", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tagValueSuffixesRequests:    ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="tagValueSuffixes", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tagValueSuffixesErrors:      ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="tagValueSuffixes", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tsdbStatusRequests:          ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="tsdbStatus", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tsdbStatusErrors:            ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="tsdbStatus", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		seriesCountRequests:         ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="seriesCount", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		seriesCountErrors:           ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="seriesCount", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		searchMetricNamesRequests:   ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="searchMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		searchMetricNamesErrors:     ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="searchMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		searchRequests:              ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="search", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		searchErrors:                ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="search", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tenantsRequests:             ms.NewCounter(fmt.Sprintf(`vm_requests_sketch_total{action="tenants", type="rpcClient", name="vmselect", addr=%q}`, addr)),
		tenantsErrors:               ms.NewCounter(fmt.Sprintf(`sketchNodevm_request_errors_sketch_total{action="tenants", type="rpcClient", name="vmselect", addr=%q}`, addr)),

		metricBlocksRead: ms.NewCounter(fmt.Sprintf(`vm_metric_blocks_read_sketch_total{name="vmselect", addr=%q}`, addr)),
		metricRowsRead:   ms.NewCounter(fmt.Sprintf(`vm_metric_rows_read_sketch_total{name="vmselect", addr=%q}`, addr)),
	}
	return sn
}

func initSketchNodes(addrs []string) *sketchNodesBucket {
	if len(addrs) == 0 {
		logger.Panicf("BUG: addrs must be non-empty")
	}

	groupsMap := initSketchNodeGroups(addrs)

	var snsLock sync.Mutex
	sns := make([]*sketchNode, 0, len(addrs))
	var wg sync.WaitGroup
	ms := metrics.NewSet()
	// initialize connections to sketch nodes in parallel in order speed up the initialization
	// for big number of sketch nodes.
	// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/4364
	for _, addr := range addrs {
		var groupName string
		groupName, addr = netutil.ParseGroupAddr(addr)
		group := groupsMap[groupName]

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			sn := newSketchNode(ms, group, addr)
			snsLock.Lock()
			sns = append(sns, sn)
			snsLock.Unlock()
		}(addr)
	}
	wg.Wait()
	metrics.RegisterSet(ms)
	return &sketchNodesBucket{
		sns: sns,
		ms:  ms,
	}
}

func mustStopSketchNodes(snb *sketchNodesBucket) {
	for _, sn := range snb.sns {
		sn.connPool.MustStop()
	}
	metrics.UnregisterSet(snb.ms, true)
}

var (
	partialSearchResultsSketch = metrics.NewCounter(`vm_partial_results_sketch_total{action="search", name="vmselect"}`)
)

type sketchNodesGroup struct {
	// group name
	name string

	// replicationFactor for the given group
	replicationFactor int

	// the number of nodes in the group
	nodesCount int

	// groupsCount is the number of groups in the list the given group belongs to
	groupsCount int
}

func initSketchNodeGroups(addrs []string) map[string]*sketchNodesGroup {
	m := make(map[string]*sketchNodesGroup)
	for _, addr := range addrs {
		groupName, _ := netutil.ParseGroupAddr(addr)
		g, ok := m[groupName]
		if !ok {
			g = &sketchNodesGroup{
				name:              groupName,
				replicationFactor: replicationFactor.Get(groupName),
			}
			m[groupName] = g
		}
		g.nodesCount++
	}

	groupsCount := len(m)
	for _, g := range m {
		g.groupsCount = groupsCount
	}

	return m
}

type sketchNode struct {
	// The group this sketchNode belongs to.
	group *sketchNodesGroup

	// Connection pool for the given sketchNode.
	connPool *netutil.ConnPool

	// The number of concurrent queries to sketchNode.
	concurrentQueries *metrics.Counter

	// The number of RegisterMetricNames requests to sketchNode.
	registerMetricNamesRequests *metrics.Counter

	// The number of RegisterMetricNames request errors to sketchNode.
	registerMetricNamesErrors *metrics.Counter

	// The number of DeleteSeries requests to sketchNode.
	deleteSeriesRequests *metrics.Counter

	// The number of DeleteSeries request errors to sketchNode.
	deleteSeriesErrors *metrics.Counter

	// The number of requests to labelNames.
	labelNamesRequests *metrics.Counter

	// The number of errors during requests to labelNames.
	labelNamesErrors *metrics.Counter

	// The number of requests to labelValues.
	labelValuesRequests *metrics.Counter

	// The number of errors during requests to labelValuesOnTimeRange.
	labelValuesErrors *metrics.Counter

	// The number of requests to tagValueSuffixes.
	tagValueSuffixesRequests *metrics.Counter

	// The number of errors during requests to tagValueSuffixes.
	tagValueSuffixesErrors *metrics.Counter

	// The number of requests to tsdb status.
	tsdbStatusRequests *metrics.Counter

	// The number of errors during requests to tsdb status.
	tsdbStatusErrors *metrics.Counter

	// The number of requests to seriesCount.
	seriesCountRequests *metrics.Counter

	// The number of errors during requests to seriesCount.
	seriesCountErrors *metrics.Counter

	// The number of searchMetricNames requests to sketchNode.
	searchMetricNamesRequests *metrics.Counter

	// The number of searchMetricNames errors to sketchNode.
	searchMetricNamesErrors *metrics.Counter

	// The number of search requests to sketchNode.
	searchRequests *metrics.Counter

	// The number of search request errors to sketchNode.
	searchErrors *metrics.Counter

	// The number of metric blocks read.
	metricBlocksRead *metrics.Counter

	// The number of read metric rows.
	metricRowsRead *metrics.Counter

	// The number of list tenants requests to sketchNode.
	tenantsRequests *metrics.Counter

	// The number of list tenants errors to sketchNode.
	tenantsErrors *metrics.Counter
}

func (sn *sketchNode) registerMetricNames(qt *querytracer.Tracer, mrs []storage.MetricRow, deadline searchutils.Deadline) error {
	if len(mrs) == 0 {
		return nil
	}
	f := func(bc *handshake.BufferedConn) error {
		return sn.registerMetricNamesOnConn(bc, mrs)
	}
	return sn.execOnConnWithPossibleRetry(qt, "registerMetricNames_v3", f, deadline)
}

func (sn *sketchNode) deleteSeries(qt *querytracer.Tracer, requestData []byte, deadline searchutils.Deadline) (int, error) {
	var deletedCount int
	f := func(bc *handshake.BufferedConn) error {
		n, err := sn.deleteSeriesOnConn(bc, requestData)
		if err != nil {
			return err
		}
		deletedCount = n
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "deleteSeries_v5", f, deadline); err != nil {
		return 0, err
	}
	return deletedCount, nil
}

func (sn *sketchNode) registerMetricNamesOnConn(bc *handshake.BufferedConn, mrs []storage.MetricRow) error {
	// Send the request to sn.
	if err := writeUint64(bc, uint64(len(mrs))); err != nil {
		return fmt.Errorf("cannot send metricsCount to conn: %w", err)
	}
	for i, mr := range mrs {
		if err := writeBytes(bc, mr.MetricNameRaw); err != nil {
			return fmt.Errorf("cannot send MetricNameRaw #%d to conn: %w", i+1, err)
		}
		if err := writeUint64(bc, uint64(mr.Timestamp)); err != nil {
			return fmt.Errorf("cannot send Timestamp #%d to conn: %w", i+1, err)
		}
	}
	if err := bc.Flush(); err != nil {
		return fmt.Errorf("cannot flush registerMetricNames request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return newErrRemote(buf)
	}
	return nil
}

func (sn *sketchNode) deleteSeriesOnConn(bc *handshake.BufferedConn, requestData []byte) (int, error) {
	// Send the request to sn
	if err := writeBytes(bc, requestData); err != nil {
		return 0, fmt.Errorf("cannot send deleteSeries request to conn: %w", err)
	}
	if err := bc.Flush(); err != nil {
		return 0, fmt.Errorf("cannot flush deleteSeries request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return 0, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return 0, newErrRemote(buf)
	}

	// Read deletedCount
	deletedCount, err := readUint64(bc)
	if err != nil {
		return 0, fmt.Errorf("cannot read deletedCount value: %w", err)
	}
	return int(deletedCount), nil
}

func (sn *sketchNode) getLabelNamesOnConn(bc *handshake.BufferedConn, requestData []byte, maxLabelNames int) ([]string, error) {
	// Send the request to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return nil, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := writeLimit(bc, maxLabelNames); err != nil {
		return nil, fmt.Errorf("cannot write maxLabelNames=%d: %w", maxLabelNames, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	var labels []string
	for {
		buf, err = readBytes(buf[:0], bc, maxLabelNameSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read labels: %w", err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return labels, nil
		}
		labels = append(labels, string(buf))
	}
}

func (sn *sketchNode) getLabelValuesOnConn(bc *handshake.BufferedConn, labelName string, requestData []byte, maxLabelValues int) ([]string, error) {
	// Send the request to sn.
	if err := writeBytes(bc, []byte(labelName)); err != nil {
		return nil, fmt.Errorf("cannot send labelName=%q to conn: %w", labelName, err)
	}
	if err := writeBytes(bc, requestData); err != nil {
		return nil, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := writeLimit(bc, maxLabelValues); err != nil {
		return nil, fmt.Errorf("cannot write maxLabelValues=%d: %w", maxLabelValues, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush labelName to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	labelValues, _, err := readLabelValues(buf, bc)
	if err != nil {
		return nil, err
	}
	return labelValues, nil
}

func (sn *sketchNode) getLabelNames(qt *querytracer.Tracer, requestData []byte, maxLabelNames int, deadline searchutils.Deadline) ([]string, error) {
	var labels []string
	f := func(bc *handshake.BufferedConn) error {
		ls, err := sn.getLabelNamesOnConn(bc, requestData, maxLabelNames)
		if err != nil {
			return err
		}
		labels = ls
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "labelNames_v5", f, deadline); err != nil {
		return nil, err
	}
	return labels, nil
}

func (sn *sketchNode) getLabelValues(qt *querytracer.Tracer, labelName string, requestData []byte, maxLabelValues int, deadline searchutils.Deadline) ([]string, error) {
	var labelValues []string
	f := func(bc *handshake.BufferedConn) error {
		lvs, err := sn.getLabelValuesOnConn(bc, labelName, requestData, maxLabelValues)
		if err != nil {
			return err
		}
		labelValues = lvs
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "labelValues_v5", f, deadline); err != nil {
		return nil, err
	}
	return labelValues, nil
}

func writeTimeRangeSketch(bc *handshake.BufferedConn, tr sketch.TimeRange) error {
	if err := writeUint64(bc, uint64(tr.MinTimestamp)); err != nil {
		return fmt.Errorf("cannot send minTimestamp=%d to conn: %w", tr.MinTimestamp, err)
	}
	if err := writeUint64(bc, uint64(tr.MaxTimestamp)); err != nil {
		return fmt.Errorf("cannot send maxTimestamp=%d to conn: %w", tr.MaxTimestamp, err)
	}
	return nil
}

func (sn *sketchNode) getTenantsOnConn(bc *handshake.BufferedConn, tr sketch.TimeRange) ([]string, error) {
	if err := writeTimeRangeSketch(bc, tr); err != nil {
		return nil, err
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	var tenants []string
	for {
		var err error
		buf, err = readBytes(buf[:0], bc, maxTenantValueSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read tenant #%d: %w", len(tenants), err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return tenants, nil
		}
		tenants = append(tenants, string(buf))
	}
}

func (sn *sketchNode) getTagValueSuffixesOnConn(bc *handshake.BufferedConn, accountID, projectID uint32,
	tr storage.TimeRange, tagKey, tagValuePrefix string, delimiter byte, maxSuffixes int,
) ([]string, error) {
	// Send the request to sn.
	if err := sendAccountIDProjectID(bc, accountID, projectID); err != nil {
		return nil, err
	}
	if err := writeTimeRange(bc, tr); err != nil {
		return nil, err
	}
	if err := writeBytes(bc, []byte(tagKey)); err != nil {
		return nil, fmt.Errorf("cannot send tagKey=%q to conn: %w", tagKey, err)
	}
	if err := writeBytes(bc, []byte(tagValuePrefix)); err != nil {
		return nil, fmt.Errorf("cannot send tagValuePrefix=%q to conn: %w", tagValuePrefix, err)
	}
	if err := writeByte(bc, delimiter); err != nil {
		return nil, fmt.Errorf("cannot send delimiter=%c to conn: %w", delimiter, err)
	}
	if err := writeLimit(bc, maxSuffixes); err != nil {
		return nil, fmt.Errorf("cannot send maxSuffixes=%d to conn: %w", maxSuffixes, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response.
	// The response may contain empty suffix, so it is prepended with the number of the following suffixes.
	suffixesCount, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read the number of tag value suffixes: %w", err)
	}
	suffixes := make([]string, 0, suffixesCount)
	for i := 0; i < int(suffixesCount); i++ {
		buf, err = readBytes(buf[:0], bc, maxLabelValueSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read tag value suffix #%d: %w", i+1, err)
		}
		suffixes = append(suffixes, string(buf))
	}
	return suffixes, nil
}

// Tenants returns tenants until the given deadline.
func TenantsSketch(qt *querytracer.Tracer, tr sketch.TimeRange, deadline searchutils.Deadline) ([]string, error) {
	qt = qt.NewChild("get tenants on timeRange=%s", &tr)
	defer qt.Done()
	if deadline.Exceeded() {
		return nil, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
	}

	// Send the query to all the storage nodes in parallel.
	type nodeResult struct {
		tenants []string
		err     error
	}
	sns := getSketchNodes()
	// Deny partial responses when obtaining the list of tenants, since partial tenants have little sense.
	snr := startSketchNodesRequest(qt, sns, true, func(qt *querytracer.Tracer, _ uint, sn *sketchNode) any {
		sn.tenantsRequests.Inc()
		tenants, err := sn.getTenants(qt, tr, deadline)
		if err != nil {
			sn.tenantsErrors.Inc()
			err = fmt.Errorf("cannot get tenants from vmstorage %s: %w", sn.connPool.Addr(), err)
		}
		return &nodeResult{
			tenants: tenants,
			err:     err,
		}
	})

	// Collect results
	var tenants []string
	_, err := snr.collectResults(partialLabelValuesResults, func(result any) error {
		nr := result.(*nodeResult)
		if nr.err != nil {
			return nr.err
		}
		tenants = append(tenants, nr.tenants...)
		return nil
	})
	qt.Printf("get %d non-duplicated tenants", len(tenants))
	if err != nil {
		return nil, fmt.Errorf("cannot fetch tenants from vmstorage nodes: %w", err)
	}

	// Deduplicate tenants
	tenants = deduplicateStrings(tenants)
	qt.Printf("get %d unique tenants after de-duplication", len(tenants))

	sort.Strings(tenants)
	qt.Printf("sort %d tenants", len(tenants))
	return tenants, nil
}

func (sn *sketchNode) getTenants(qt *querytracer.Tracer, tr sketch.TimeRange, deadline searchutils.Deadline) ([]string, error) {
	var tenants []string
	f := func(bc *handshake.BufferedConn) error {
		result, err := sn.getTenantsOnConn(bc, tr)
		if err != nil {
			return err
		}
		tenants = result
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "tenants_v1", f, deadline); err != nil {
		return nil, err
	}
	return tenants, nil
}

func (sn *sketchNode) getTagValueSuffixes(qt *querytracer.Tracer, accountID, projectID uint32, tr storage.TimeRange, tagKey, tagValuePrefix string,
	delimiter byte, maxSuffixes int, deadline searchutils.Deadline,
) ([]string, error) {
	var suffixes []string
	f := func(bc *handshake.BufferedConn) error {
		ss, err := sn.getTagValueSuffixesOnConn(bc, accountID, projectID, tr, tagKey, tagValuePrefix, delimiter, maxSuffixes)
		if err != nil {
			return err
		}
		suffixes = ss
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "tagValueSuffixes_v4", f, deadline); err != nil {
		return nil, err
	}
	return suffixes, nil
}

func (sn *sketchNode) getSketchCacheStatusOnConn(bc *handshake.BufferedConn, requestData []byte, focusLabel string, topN int) (*sketch.SketchCacheStatus, error) {
	// Send the request to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return nil, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := writeBytes(bc, []byte(focusLabel)); err != nil {
		return nil, fmt.Errorf("cannot write focusLabel=%q: %w", focusLabel, err)
	}
	// topN shouldn't exceed 32 bits, so send it as uint32.
	if err := writeUint32(bc, uint32(topN)); err != nil {
		return nil, fmt.Errorf("cannot send topN=%d to conn: %w", topN, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush tsdbStatus args to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	return readSketchCacheStatus(bc)
}

func readSketchCacheStatus(bc *handshake.BufferedConn) (*sketch.SketchCacheStatus, error) {
	totalSeries, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read totalSeries: %w", err)
	}
	totalLabelValuePairs, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read totalLabelValuePairs: %w", err)
	}
	status := &sketch.SketchCacheStatus{
		TotalSeries:          totalSeries,
		TotalLabelValuePairs: totalLabelValuePairs,
	}
	return status, nil
}

func (sn *sketchNode) getSketchCacheStatus(qt *querytracer.Tracer, requestData []byte, focusLabel string, topN int, deadline searchutils.Deadline) (*sketch.SketchCacheStatus, error) {
	var status *sketch.SketchCacheStatus
	f := func(bc *handshake.BufferedConn) error {
		st, err := sn.getSketchCacheStatusOnConn(bc, requestData, focusLabel, topN)
		if err != nil {
			return err
		}
		status = st
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "tsdbStatus_v5", f, deadline); err != nil {
		return nil, err
	}
	return status, nil
}

func (sn *sketchNode) getSeriesCount(qt *querytracer.Tracer, accountID, projectID uint32, deadline searchutils.Deadline) (uint64, error) {
	var n uint64
	f := func(bc *handshake.BufferedConn) error {
		nn, err := sn.getSeriesCountOnConn(bc, accountID, projectID)
		if err != nil {
			return err
		}
		n = nn
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "seriesCount_v4", f, deadline); err != nil {
		return 0, err
	}
	return n, nil
}

func (sn *sketchNode) processSearchMetricNames(qt *querytracer.Tracer, requestData []byte, deadline searchutils.Deadline) ([]string, error) {
	var metricNames []string
	f := func(bc *handshake.BufferedConn) error {
		mns, err := sn.processSearchMetricNamesOnConn(bc, requestData)
		if err != nil {
			return err
		}
		metricNames = mns
		return nil
	}
	if err := sn.execOnConnWithPossibleRetry(qt, "searchMetricNames_v3", f, deadline); err != nil {
		return nil, err
	}
	return metricNames, nil
}

func (sn *sketchNode) processSearchQuery(qt *querytracer.Tracer, requestData []byte, processBlock func(mb *storage.MetricBlock, workerID uint) error,
	workerID uint, deadline searchutils.Deadline,
) error {
	f := func(bc *handshake.BufferedConn) error {
		return sn.processSearchQueryOnConn(bc, requestData, processBlock, workerID)
	}
	return sn.execOnConnWithPossibleRetry(qt, "search_v7", f, deadline)
}

func (sn *sketchNode) execOnConnWithPossibleRetry(qt *querytracer.Tracer, funcName string, f func(bc *handshake.BufferedConn) error, deadline searchutils.Deadline) error {
	qtChild := qt.NewChild("rpc call %s()", funcName)
	err := sn.execOnConn(qtChild, funcName, f, deadline)
	defer qtChild.Done()
	if err == nil {
		return nil
	}
	var er *errRemote
	var ne net.Error
	var le *limitExceededErr
	if errors.As(err, &le) || errors.As(err, &er) || errors.As(err, &ne) && ne.Timeout() || deadline.Exceeded() {
		// There is no sense in repeating the query on the following errors:
		//
		//   - exceeded complexity limits (limitExceededErr)
		//   - induced by vmstorage (errRemote)
		//   - network timeout errors
		//   - request deadline exceeded errors
		return err
	}
	// Repeat the query in the hope the error was temporary.
	qtRetry := qtChild.NewChild("retry rpc call %s() after error", funcName)
	err = sn.execOnConn(qtRetry, funcName, f, deadline)
	qtRetry.Done()
	return err
}

func (sn *sketchNode) execOnConn(qt *querytracer.Tracer, funcName string, f func(bc *handshake.BufferedConn) error, deadline searchutils.Deadline) error {
	sn.concurrentQueries.Inc()
	defer sn.concurrentQueries.Dec()

	d := time.Unix(int64(deadline.Deadline()), 0)
	nowSecs := fasttime.UnixTimestamp()
	currentTime := time.Unix(int64(nowSecs), 0)
	timeout := d.Sub(currentTime)
	if timeout <= 0 {
		return fmt.Errorf("request timeout reached: %s", deadline.String())
	}
	bc, err := sn.connPool.Get()
	if err != nil {
		return fmt.Errorf("cannot obtain connection from a pool: %w", err)
	}
	// Extend the connection deadline by 2 seconds, so the remote storage could return `timeout` error
	// without the need to break the connection.
	connDeadline := d.Add(2 * time.Second)
	if err := bc.SetDeadline(connDeadline); err != nil {
		_ = bc.Close()
		logger.Panicf("FATAL: cannot set connection deadline: %s", err)
	}
	if err := writeBytes(bc, []byte(funcName)); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return fmt.Errorf("cannot send funcName=%q to the server: %w", funcName, err)
	}

	// Send query trace flag
	traceEnabled := qt.Enabled()
	if err := writeBool(bc, traceEnabled); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return fmt.Errorf("cannot send traceEnabled=%v for funcName=%q to the server: %w", traceEnabled, funcName, err)
	}
	// Send the remaining timeout instead of deadline to remote server, since it may have different time.
	timeoutSecs := uint32(timeout.Seconds() + 1)
	if err := writeUint32(bc, timeoutSecs); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return fmt.Errorf("cannot send timeout=%d for funcName=%q to the server: %w", timeout, funcName, err)
	}
	// Execute the rpc function.
	if err := f(bc); err != nil {
		remoteAddr := bc.RemoteAddr()
		var er *errRemote
		if errors.As(err, &er) {
			// Remote error. The connection may be re-used. Return it to the pool.
			_ = readTrace(qt, bc)
			sn.connPool.Put(bc)
		} else {
			// Local error.
			// Close the connection instead of returning it to the pool,
			// since it may be broken.
			_ = bc.Close()
		}
		if deadline.Exceeded() || errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("cannot execute funcName=%q on vmstorage %q with timeout %s: %w", funcName, remoteAddr, deadline.String(), err)
		}
		return fmt.Errorf("cannot execute funcName=%q on vmstorage %q: %w", funcName, remoteAddr, err)
	}

	// Read trace from the response
	if err := readTrace(qt, bc); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return err
	}
	// Return the connection back to the pool, assuming it is healthy.
	sn.connPool.Put(bc)
	return nil
}

func (sn *sketchNode) getSeriesCountOnConn(bc *handshake.BufferedConn, accountID, projectID uint32) (uint64, error) {
	// Send the request to sn.
	if err := sendAccountIDProjectID(bc, accountID, projectID); err != nil {
		return 0, err
	}
	if err := bc.Flush(); err != nil {
		return 0, fmt.Errorf("cannot flush seriesCount args to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return 0, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return 0, newErrRemote(buf)
	}

	// Read response
	n, err := readUint64(bc)
	if err != nil {
		return 0, fmt.Errorf("cannot read series count: %w", err)
	}
	return n, nil
}

func (sn *sketchNode) processSearchQueryOnConn(bc *handshake.BufferedConn, requestData []byte,
	processBlock func(mb *storage.MetricBlock, workerID uint) error, workerID uint,
) error {
	// Send the request to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := bc.Flush(); err != nil {
		return fmt.Errorf("cannot flush requestData to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return newErrRemote(buf)
	}

	// Read response. It may consist of multiple MetricBlocks.
	blocksRead := 0
	var mb sketch.MetricBlock // TODO: What to return from VMSketch?
	for {
		buf, err = readBytes(buf[:0], bc, maxMetricBlockSize)
		if err != nil {
			return fmt.Errorf("cannot read MetricBlock #%d: %w", blocksRead, err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return nil
		}
		tail, err := mb.Unmarshal(buf)
		if err != nil {
			return fmt.Errorf("cannot unmarshal MetricBlock #%d from %d bytes: %w", blocksRead, len(buf), err)
		}
		if len(tail) != 0 {
			return fmt.Errorf("non-empty tail after unmarshaling MetricBlock #%d: (len=%d) %q", blocksRead, len(tail), tail)
		}
		blocksRead++
		sn.metricBlocksRead.Inc()
		sn.metricRowsRead.Add(mb.Block.RowsCount())
		if err := processBlock(&mb, workerID); err != nil {
			return fmt.Errorf("cannot process MetricBlock #%d: %w", blocksRead, err)
		}
	}
}

func (sn *sketchNode) processSearchMetricNamesOnConn(bc *handshake.BufferedConn, requestData []byte) ([]string, error) {
	// Send the requst to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return nil, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush requestData to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read metricNames from response.
	metricNamesCount, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read metricNamesCount: %w", err)
	}
	metricNames := make([]string, metricNamesCount)
	for i := int64(0); i < int64(metricNamesCount); i++ {
		buf, err = readBytes(buf[:0], bc, maxMetricNameSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read metricName #%d: %w", i+1, err)
		}
		metricNames[i] = string(buf)
	}
	return metricNames, nil
}

type sketchNodesRequest struct {
	denyPartialResponse bool
	resultsCh           chan rpcResultSketch
	qts                 map[*querytracer.Tracer]struct{}
	sns                 []*sketchNode
}

type rpcResultSketch struct {
	data  any
	qt    *querytracer.Tracer
	group *sketchNodesGroup
}

func startSketchNodesRequest(qt *querytracer.Tracer, sns []*sketchNode, denyPartialResponse bool,
	f func(qt *querytracer.Tracer, workerID uint, sn *sketchNode) any,
) *sketchNodesRequest {
	resultsCh := make(chan rpcResultSketch, len(sns))
	qts := make(map[*querytracer.Tracer]struct{}, len(sns))
	for idx, sn := range sns {
		qtChild := qt.NewChild("rpc at vmsketch %s", sn.connPool.Addr())
		qts[qtChild] = struct{}{}
		go func(workerID uint, sn *sketchNode) {
			data := f(qtChild, workerID, sn)
			resultsCh <- rpcResultSketch{
				data:  data,
				qt:    qtChild,
				group: sn.group,
			}
		}(uint(idx), sn)
	}
	return &sketchNodesRequest{
		denyPartialResponse: denyPartialResponse,
		resultsCh:           resultsCh,
		qts:                 qts,
		sns:                 sns,
	}
}

func processAndEvalSketchBlocks(qt *querytracer.Tracer, sns []*sketchNode, denyPartialResponse bool, sq *sketch.SearchQuery,
	processBlock func(mb *storage.MetricBlock, workerID uint) error, deadline searchutils.Deadline,
) ([]*sketch.Timeseries, bool, error) {
	// Make sure that processBlock is no longer called after the exit from processBlocks() function.
	// Use per-worker WaitGroup instead of a shared WaitGroup in order to avoid inter-CPU contention,
	// which may significantly slow down the rate of processBlock calls on multi-CPU systems.
	type wgStruct struct {
		// mu prevents from calling processBlock when stop is set to true
		mu sync.Mutex

		// wg is used for waiting until currently executed processBlock calls are finished.
		wg sync.WaitGroup

		// stop must be set to true when no more processBlocks calls should be made.
		stop bool
	}
	type wgWithPadding struct {
		wgStruct
		// The padding prevents false sharing on widespread platforms with
		// 128 mod (cache line size) = 0 .
		_ [128 - unsafe.Sizeof(wgStruct{})%128]byte
	}
	wgs := make([]wgWithPadding, len(sns))
	f := func(mb *storage.MetricBlock, workerID uint) error {
		muwg := &wgs[workerID]
		muwg.mu.Lock()
		if muwg.stop {
			muwg.mu.Unlock()
			return nil
		}
		muwg.wg.Add(1)
		muwg.mu.Unlock()
		err := processBlock(mb, workerID)
		muwg.wg.Done()
		return err
	}

	err := populateSqTenantTokensIfNeededSketch(sq)
	if err != nil {
		return false, err
	}
	// Send the query to all the sketch nodes in parallel.
	snr := startSketchNodesRequest(qt, sns, denyPartialResponse, func(qt *querytracer.Tracer, workerID uint, sn *sketchNode) any {
		// Use a separate variable for each goroutine
		var err error
		res := execSketchSearchQuery(qt, sq, func(qt *querytracer.Tracer, rd []byte, _ sketch.TenantToken) any {
			sn.searchRequests.Inc()
			err = sn.processSearchQuery(qt, rd, f, workerID, deadline)
			if err != nil {
				sn.searchErrors.Inc()
				err = fmt.Errorf("cannot perform search on vmsketch %s: %w", sn.connPool.Addr(), err)
				return &err
			}

			return &err
		})

		for _, e := range res {
			e := e.(*error)
			if *e != nil {
				return e
			}
		}

		return &err
	})

	// Collect results; each timeseries is evaluated results from sketches
	isPartial, err := snr.collectResults(partialSearchResultsSketch, func(result any) error {
		errP := result.(*error)
		return *errP
	})
	// Make sure that processBlock is no longer called after the exit from processBlocks() function.
	for i := range wgs {
		muwg := &wgs[i]
		muwg.mu.Lock()
		muwg.stop = true
		muwg.mu.Unlock()
	}
	for i := range wgs {
		wgs[i].wg.Wait()
	}
	if err != nil {
		return isPartial, fmt.Errorf("cannot fetch query results from vmsketch nodes: %w", err)
	}
	return ts_results, isCovered, nil
}

/*
There will be network connections
*/
func SearchAndEvalSketchCache(qt *querytracer.Tracer, denyPartialResponse bool, sqs *sketch.SearchQuery, maxMetrics int, deadline searchutils.Deadline) ([]*sketch.Timeseries, bool, error) {
	qt = qt.NewChild("try to search adn eval query from sketch cache: %s", sqs)
	defer qt.Done()
	if deadline.Exceeded() {
		return nil, false, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
	}

	// Setup search.
	tr := sketch.TimeRange{
		MinTimestamp: sqs.MinTimestamp,
		MaxTimestamp: sqs.MaxTimestamp,
	}
	sns := getSketchNodes()

	processBlock := func(workerID int) error {

	}

	ts_results, isCovered, err := processAndEvalSketchBlocks(qt, sns, denyPartialResponse, sqs, processBlock, deadline)

	return ts_results, isCovered, err
}

// execSearchQuery calls cb for with marshaled requestData for each tenant in sq.
func execSketchSearchQuery(qt *querytracer.Tracer, sq *sketch.SearchQuery, cb func(qt *querytracer.Tracer, requestData []byte, t sketch.TenantToken) any) []any {
	var requestData []byte
	var results []any

	for i := range sq.TenantTokens {
		requestData = sq.TenantTokens[i].Marshal(requestData)
		requestData = sq.MarshaWithoutTenant(requestData)
		qtL := qt
		if sq.IsMultiTenant && qt.Enabled() {
			qtL = qt.NewChild("query for tenant: %s", sq.TenantTokens[i].String())
		}
		r := cb(qtL, requestData, sq.TenantTokens[i])
		if sq.IsMultiTenant {
			qtL.Done()
		}
		results = append(results, r)
		requestData = requestData[:0]
	}

	return results
}

func (snr *sketchNodesRequest) collectResults(partialResultsCounter *metrics.Counter, f func(result any) error) (bool, error) {
	sns := snr.sns
	if len(sns) == 0 {
		return false, nil
	}
	groupsCount := sns[0].group.groupsCount
	resultsCollectedPerGroup := make(map[*sketchNodesGroup]int, groupsCount)
	errsPartialPerGroup := make(map[*sketchNodesGroup][]error)
	groupsPartial := make(map[*sketchNodesGroup]struct{})
	for range sns {
		// There is no need in timer here, since all the goroutines executing the f function
		// passed to startSketchNodesRequest must be finished until the deadline.
		result := <-snr.resultsCh
		group := result.group
		if err := f(result.data); err != nil {
			snr.finishQueryTracer(result.qt, fmt.Sprintf("error: %s", err))
			var er *errRemote
			if errors.As(err, &er) {
				// Immediately return the error reported by vmstorage to the caller,
				// since such errors usually mean misconfiguration at vmstorage.
				// The misconfiguration must be known by the caller, so it is fixed ASAP.
				snr.finishQueryTracers("cancel request because of error in other vmstorage nodes")
				return false, err
			}
			var limitErr *limitExceededErr
			if errors.As(err, &limitErr) {
				// Immediately return the error, since complexity limits are already exceeded,
				// and we don't need to process the rest of results.
				snr.finishQueryTracers("cancel request because query complexity limit was exceeded")
				return false, err
			}

			errsPartialPerGroup[group] = append(errsPartialPerGroup[group], err)
			if snr.denyPartialResponse && len(errsPartialPerGroup[group]) >= group.replicationFactor {
				groupsPartial[group] = struct{}{}
				if len(groupsPartial) < *globalReplicationFactor {
					// Ignore this error, since the number of groups with partial results is smaller than the globalReplicationFactor.
					continue
				}

				// Return the error to the caller if partial responses are denied
				// and the number of partial responses for the given group reach its replicationFactor,
				// since this means that the response is partial.
				snr.finishQueryTracers(fmt.Sprintf("cancel request because partial responses are denied and replicationFactor=%d vmstorage nodes at group %q failed to return response",
					group.replicationFactor, group.name))

				// Returns 503 status code for partial response, so the caller could retry it if needed.
				err = &httpserver.ErrorWithStatusCode{
					Err:        err,
					StatusCode: http.StatusServiceUnavailable,
				}
				return false, err
			}
			continue
		}
		snr.finishQueryTracer(result.qt, "")
		resultsCollectedPerGroup[group]++
		if *skipSlowReplicas && len(resultsCollectedPerGroup) > groupsCount-*globalReplicationFactor {
			groupsWithFullResult := 0
			for g, n := range resultsCollectedPerGroup {
				if n > g.nodesCount-g.replicationFactor {
					groupsWithFullResult++
				}
			}
			if groupsWithFullResult > groupsCount-*globalReplicationFactor {
				// There is no need in waiting for the remaining results,
				// because the collected results contain all the data according to the given per-group replicationFactor.
				// This should speed up responses when a part of vmstorage nodes are slow and/or temporarily unavailable.
				// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/711
				snr.finishQueryTracers("cancel request because -search.skipSlowReplicas is set and every group returned the needed number of responses according to replicationFactor")
				return false, nil
			}
		}
	}

	// Verify whether the full result can be returned
	failedGroups := 0
	for g, errsPartial := range errsPartialPerGroup {
		if len(errsPartial) >= g.replicationFactor {
			failedGroups++
		}
	}
	if failedGroups < *globalReplicationFactor {
		// Assume that the result is full if the the number of failed groups is smaller than the globalReplicationFactor.
		return false, nil
	}

	// Verify whether at least a single node per each group successfully returned result in order to be able returning partial result.
	missingGroups := 0
	var firstErr error
	for g, errsPartial := range errsPartialPerGroup {
		if len(errsPartial) == g.nodesCount {
			missingGroups++
			if firstErr == nil {
				// Return only the first error, since it has no sense in returning all errors.
				firstErr = errsPartial[0]
			}
		}
		if len(errsPartial) > 0 {
			partialErrorsLogger.Warnf("%d out of %d vmstorage nodes at group %q were unavailable during the query; a sample error: %s", len(errsPartial), len(sns), g.name, errsPartial[0])
		}
	}
	if missingGroups >= *globalReplicationFactor {
		// Too many groups contain all the non-working vmstorage nodes.
		// Returns 503 status code, so the caller could retry it if needed.
		err := &httpserver.ErrorWithStatusCode{
			Err:        firstErr,
			StatusCode: http.StatusServiceUnavailable,
		}
		return false, err
	}

	// Return partial results.
	// This allows continuing returning responses in the case
	// if a part of vmstorage nodes are temporarily unavailable.
	partialResultsCounter.Inc()
	// Do not return the error, since it may spam logs on busy vmselect
	// serving high amounts of requests.
	return true, nil
}

func (snr *sketchNodesRequest) finishQueryTracers(msg string) {
	for qt := range snr.qts {
		snr.finishQueryTracer(qt, msg)
	}
}

func (snr *sketchNodesRequest) finishQueryTracer(qt *querytracer.Tracer, msg string) {
	if msg == "" {
		qt.Done()
	} else {
		qt.Donef("%s", msg)
	}
	delete(snr.qts, qt)
}
