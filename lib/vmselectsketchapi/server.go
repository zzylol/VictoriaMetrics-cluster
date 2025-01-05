package vmselectsketchapi

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/fasttime"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/handshake"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/ingestserver"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/netutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/timerpool"
)

// Server processes vmselect requests.
type Server struct {
	// api contains the implementation of the server API for vmselect requests.
	api API

	// limits contains various limits for the Server.
	limits Limits

	// disableResponseCompression controls whether vmselect server must compress responses.
	disableResponseCompression bool

	// ln is the listener for incoming connections to the server.
	ln net.Listener

	// The channel for limiting the number of concurrently executed requests.
	concurrencyLimitCh chan struct{}

	// connsMap is a map of currently established connections to the server.
	// It is used for closing the connections when MustStop() is called.
	connsMap ingestserver.ConnsMap

	// wg is used for waiting for worker goroutines to stop when MustStop() is called.
	wg sync.WaitGroup

	// stopFlag is set to true when the server needs to stop.
	stopFlag atomic.Bool

	concurrencyLimitReached *metrics.Counter
	concurrencyLimitTimeout *metrics.Counter

	vmselectConns      *metrics.Counter
	vmselectConnErrors *metrics.Counter

	indexSearchDuration *metrics.Histogram

	registerMetricNamesRequests *metrics.Counter
	deleteSeriesRequests        *metrics.Counter
	labelNamesRequests          *metrics.Counter
	labelValuesRequests         *metrics.Counter
	tagValueSuffixesRequests    *metrics.Counter
	seriesCountRequests         *metrics.Counter
	sketchCacheStatusRequests   *metrics.Counter
	searchMetricNamesRequests   *metrics.Counter
	searchRequests              *metrics.Counter

	metricSketchesRead *metrics.Counter
	metricRowsRead     *metrics.Counter
}

// Limits contains various limits for Server.
type Limits struct {
	// MaxLabelNames is the maximum label names, which may be returned from labelNames request.
	MaxLabelNames int

	// MaxLabelValues is the maximum label values, which may be returned from labelValues request.
	MaxLabelValues int

	// MaxTagValueSuffixes is the maximum number of entries, which can be returned from tagValueSuffixes request.
	MaxTagValueSuffixes int

	// MaxConcurrentRequests is the maximum number of concurrent requests a server can process.
	//
	// The remaining requests wait for up to MaxQueueDuration for their execution.
	MaxConcurrentRequests int

	// MaxConcurrentRequestsFlagName is the name for the flag containing the MaxConcurrentRequests value.
	MaxConcurrentRequestsFlagName string

	// MaxQueueDuration is the maximum duration to wait if MaxConcurrentRequests are executed.
	MaxQueueDuration time.Duration

	// MaxQueueDurationFlagName is the name for the flag containing the MaxQueueDuration value.
	MaxQueueDurationFlagName string
}

// NewServer starts new Server at the given addr, which serves the given api with the given limits.
//
// If disableResponseCompression is set to true, then the returned server doesn't compress responses.
func NewServer(addr string, api API, limits Limits, disableResponseCompression bool) (*Server, error) {
	ln, err := netutil.NewTCPListener("vmselect", addr, false, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to listen vmselectAddr %s: %w", addr, err)
	}
	concurrencyLimitCh := make(chan struct{}, limits.MaxConcurrentRequests)
	_ = metrics.NewGauge(`vm_vmselect_sketch_concurrent_requests_capacity`, func() float64 {
		return float64(cap(concurrencyLimitCh))
	})
	_ = metrics.NewGauge(`vm_vmselect_sketch_concurrent_requests_current`, func() float64 {
		return float64(len(concurrencyLimitCh))
	})
	s := &Server{
		api:                        api,
		limits:                     limits,
		disableResponseCompression: disableResponseCompression,
		ln:                         ln,

		concurrencyLimitCh: concurrencyLimitCh,

		concurrencyLimitReached: metrics.NewCounter(fmt.Sprintf(`vm_vmselect_concurrent_requests_limit_reached_total{addr=%q}`, addr)),
		concurrencyLimitTimeout: metrics.NewCounter(fmt.Sprintf(`vm_vmselect_concurrent_requests_limit_timeout_total{addr=%q}`, addr)),

		vmselectConns:      metrics.NewCounter(fmt.Sprintf(`vm_vmselect_conns{addr=%q}`, addr)),
		vmselectConnErrors: metrics.NewCounter(fmt.Sprintf(`vm_vmselect_conn_errors_total{addr=%q}`, addr)),

		indexSearchDuration: metrics.NewHistogram(fmt.Sprintf(`vm_index_search_duration_seconds{addr=%q}`, addr)),

		registerMetricNamesRequests: metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="registerMetricNames",addr=%q}`, addr)),
		deleteSeriesRequests:        metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="deleteSeries",addr=%q}`, addr)),
		labelNamesRequests:          metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="labelNames",addr=%q}`, addr)),
		labelValuesRequests:         metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="labelValues",addr=%q}`, addr)),
		tagValueSuffixesRequests:    metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="tagValueSuffixes",addr=%q}`, addr)),
		seriesCountRequests:         metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="seriesSount",addr=%q}`, addr)),
		sketchCacheStatusRequests:   metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="sketchCacheStatus",addr=%q}`, addr)),
		searchMetricNamesRequests:   metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="searchMetricNames",addr=%q}`, addr)),
		searchRequests:              metrics.NewCounter(fmt.Sprintf(`vm_vmselect_rpc_requests_total{action="search",addr=%q}`, addr)),

		metricSketchesRead: metrics.NewCounter(fmt.Sprintf(`vm_vmselect_metric_sketches_read_total{addr=%q}`, addr)),
		metricRowsRead:     metrics.NewCounter(fmt.Sprintf(`vm_vmselect_metric_rows_read_total{addr=%q}`, addr)),
	}

	s.connsMap.Init("vmselect")
	s.wg.Add(1)
	go func() {
		s.run()
		s.wg.Done()
	}()
	return s, nil
}

func (s *Server) run() {
	logger.Infof("accepting vmselect conns at %s", s.ln.Addr())
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if pe, ok := err.(net.Error); ok && pe.Temporary() {
				continue
			}
			if s.isStopping() {
				return
			}
			logger.Panicf("FATAL: cannot process vmselect conns at %s: %s", s.ln.Addr(), err)
		}
		// Do not log connection accept from vmselect, since this can generate too many lines
		// in the log because vmselect tends to re-establish idle connections.

		if !s.connsMap.Add(c) {
			// The server is closed.
			_ = c.Close()
			return
		}
		s.vmselectConns.Inc()
		s.wg.Add(1)
		go func() {
			defer func() {
				s.connsMap.Delete(c)
				s.vmselectConns.Dec()
				s.wg.Done()
			}()

			// Compress responses to vmselect even if they already contain compressed blocks.
			// Responses contain uncompressed metric names, which should compress well
			// when the response contains high number of time series.
			// Additionally, recently added metric blocks are usually uncompressed, so the compression
			// should save network bandwidth.
			compressionLevel := 1
			if s.disableResponseCompression {
				compressionLevel = 0
			}
			bc, err := handshake.VMSelectServer(c, compressionLevel)
			if err != nil {
				if s.isStopping() {
					// c is closed inside Server.MustStop
					return
				}
				if !errors.Is(err, handshake.ErrIgnoreHealthcheck) {
					logger.Errorf("cannot perform vmselect handshake with client %q: %s", c.RemoteAddr(), err)
				}
				_ = c.Close()
				return
			}

			defer func() {
				_ = bc.Close()
			}()
			if err := s.processConn(bc); err != nil {
				if s.isStopping() {
					return
				}
				s.vmselectConnErrors.Inc()
				logger.Errorf("cannot process vmselect conn %s: %s", c.RemoteAddr(), err)
			}
		}()
	}
}

// MustStop gracefully stops s, so it no longer touches s.api after returning.
func (s *Server) MustStop() {
	// Mark the server as stoping.
	s.setIsStopping()

	// Stop accepting new connections from vmselect.
	if err := s.ln.Close(); err != nil {
		logger.Panicf("FATAL: cannot close vmselect listener: %s", err)
	}

	// Close existing connections from vmselect, so the goroutines
	// processing these connections are finished.
	s.connsMap.CloseAll(0)

	// Wait until all the goroutines processing vmselect conns are finished.
	s.wg.Wait()
}

func (s *Server) setIsStopping() {
	s.stopFlag.Store(true)
}

func (s *Server) isStopping() bool {
	return s.stopFlag.Load()
}

func (s *Server) processConn(bc *handshake.BufferedConn) error {
	ctx := &vmselectRequestCtx{
		bc:      bc,
		sizeBuf: make([]byte, 8),
	}
	for {
		if err := s.processRequest(ctx); err != nil {
			if isExpectedError(err) {
				return nil
			}
			if errors.Is(err, sketch.ErrDeadlineExceeded) {
				return fmt.Errorf("cannot process vmselect request in %d seconds: %w", ctx.timeout, err)
			}
			return fmt.Errorf("cannot process vmselect request: %w", err)
		}
		if err := bc.Flush(); err != nil {
			return fmt.Errorf("cannot flush compressed buffers: %w", err)
		}
	}
}

func isExpectedError(err error) bool {
	if err == io.EOF {
		// Remote client gracefully closed the connection.
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	errStr := err.Error()
	if strings.Contains(errStr, "broken pipe") || strings.Contains(errStr, "connection reset by peer") {
		// The connection has been interrupted abruptly.
		// It could happen due to unexpected network glitch or because connection was
		// interrupted by remote client. In both cases, remote client will notice
		// connection breach and handle it on its own. No need in mirroring the error here.
		return true
	}
	return false
}

type vmselectRequestCtx struct {
	bc      *handshake.BufferedConn
	sizeBuf []byte
	dataBuf []byte

	qt *querytracer.Tracer
	sq sketch.SearchQuery
	ms sketch.MetricSketch

	// timeout in seconds for the current request
	timeout uint64

	// deadline in unix timestamp seconds for the current request.
	deadline uint64
}

func (ctx *vmselectRequestCtx) readTimeRange() (sketch.TimeRange, error) {
	var tr sketch.TimeRange
	minTimestamp, err := ctx.readUint64()
	if err != nil {
		return tr, fmt.Errorf("cannot read minTimestamp: %w", err)
	}
	maxTimestamp, err := ctx.readUint64()
	if err != nil {
		return tr, fmt.Errorf("cannot read maxTimestamp: %w", err)
	}
	tr.MinTimestamp = int64(minTimestamp)
	tr.MaxTimestamp = int64(maxTimestamp)
	return tr, nil
}

func (ctx *vmselectRequestCtx) readLimit() (int, error) {
	n, err := ctx.readUint32()
	if err != nil {
		return 0, fmt.Errorf("cannot read limit: %w", err)
	}
	if n > 1<<31-1 {
		n = 1<<31 - 1
	}
	return int(n), nil
}

func (ctx *vmselectRequestCtx) readUint32() (uint32, error) {
	ctx.sizeBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.sizeBuf, 4)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read uint32: %w", err)
	}
	n := encoding.UnmarshalUint32(ctx.sizeBuf)
	return n, nil
}

func (ctx *vmselectRequestCtx) readUint64() (uint64, error) {
	ctx.sizeBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.sizeBuf, 8)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read uint64: %w", err)
	}
	n := encoding.UnmarshalUint64(ctx.sizeBuf)
	return n, nil
}

func (ctx *vmselectRequestCtx) readAccountIDProjectID() (uint32, uint32, error) {
	accountID, err := ctx.readUint32()
	if err != nil {
		return 0, 0, fmt.Errorf("cannot read accountID: %w", err)
	}
	projectID, err := ctx.readUint32()
	if err != nil {
		return 0, 0, fmt.Errorf("cannot read projectID: %w", err)
	}
	return accountID, projectID, nil
}

// maxSearchQuerySize is the maximum size of SearchQuery packet in bytes.
// see https://github.com/zzylol/VictoriaMetrics-cluster/issues/5154#issuecomment-1757216612
const maxSearchQuerySize = 5 * 1024 * 1024

func (ctx *vmselectRequestCtx) readSearchQuery() error {
	if err := ctx.readDataBufBytes(maxSearchQuerySize); err != nil {
		return fmt.Errorf("cannot read searchQuery: %w", err)
	}
	tail, err := ctx.sq.Unmarshal(ctx.dataBuf)
	if err != nil {
		return fmt.Errorf("cannot unmarshal SearchQuery: %w", err)
	}
	if len(tail) > 0 {
		return fmt.Errorf("unexpected non-zero tail left after unmarshaling SearchQuery: (len=%d) %q", len(tail), tail)
	}
	return nil
}

func (ctx *vmselectRequestCtx) readDataBufBytes(maxDataSize int) error {
	ctx.sizeBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.sizeBuf, 8)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("cannot read data size: %w", err)
	}
	dataSize := encoding.UnmarshalUint64(ctx.sizeBuf)
	if dataSize > uint64(maxDataSize) {
		return fmt.Errorf("too big data size: %d; it mustn't exceed %d bytes", dataSize, maxDataSize)
	}
	ctx.dataBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.dataBuf, int(dataSize))
	if dataSize == 0 {
		return nil
	}
	if n, err := io.ReadFull(ctx.bc, ctx.dataBuf); err != nil {
		return fmt.Errorf("cannot read data with size %d: %w; read only %d bytes", dataSize, err, n)
	}
	return nil
}

func (ctx *vmselectRequestCtx) readBool() (bool, error) {
	ctx.dataBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.dataBuf, 1)
	if _, err := io.ReadFull(ctx.bc, ctx.dataBuf); err != nil {
		if err == io.EOF {
			return false, err
		}
		return false, fmt.Errorf("cannot read bool: %w", err)
	}
	v := ctx.dataBuf[0] != 0
	return v, nil
}

func (ctx *vmselectRequestCtx) readByte() (byte, error) {
	ctx.dataBuf = bytesutil.ResizeNoCopyMayOverallocate(ctx.dataBuf, 1)
	if _, err := io.ReadFull(ctx.bc, ctx.dataBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read byte: %w", err)
	}
	b := ctx.dataBuf[0]
	return b, nil
}

func (ctx *vmselectRequestCtx) writeDataBufBytes() error {
	if err := ctx.writeUint64(uint64(len(ctx.dataBuf))); err != nil {
		return fmt.Errorf("cannot write data size: %w", err)
	}
	if len(ctx.dataBuf) == 0 {
		return nil
	}
	if _, err := ctx.bc.Write(ctx.dataBuf); err != nil {
		return fmt.Errorf("cannot write data with size %d: %w", len(ctx.dataBuf), err)
	}
	return nil
}

// maxErrorMessageSize is the maximum size of error message to send to clients.
const maxErrorMessageSize = 64 * 1024

func (ctx *vmselectRequestCtx) writeErrorMessage(err error) error {
	if errors.Is(err, sketch.ErrDeadlineExceeded) {
		err = fmt.Errorf("cannot execute request in %d seconds: %w", ctx.timeout, err)
	}
	errMsg := err.Error()
	if len(errMsg) > maxErrorMessageSize {
		// Trim too long error message.
		errMsg = errMsg[:maxErrorMessageSize]
	}
	if err := ctx.writeString(errMsg); err != nil {
		return fmt.Errorf("cannot send error message %q to client: %w", errMsg, err)
	}
	return nil
}

func (ctx *vmselectRequestCtx) writeString(s string) error {
	ctx.dataBuf = append(ctx.dataBuf[:0], s...)
	return ctx.writeDataBufBytes()
}

func (ctx *vmselectRequestCtx) writeUint64(n uint64) error {
	ctx.sizeBuf = encoding.MarshalUint64(ctx.sizeBuf[:0], n)
	if _, err := ctx.bc.Write(ctx.sizeBuf); err != nil {
		return fmt.Errorf("cannot write uint64 %d: %w", n, err)
	}
	return nil
}

const maxRPCNameSize = 128

func (s *Server) processRequest(ctx *vmselectRequestCtx) error {
	// Read rpcName
	// Do not set deadline on reading rpcName, since it may take a
	// lot of time for idle connection.
	if err := ctx.readDataBufBytes(maxRPCNameSize); err != nil {
		if err == io.EOF {
			// Remote client gracefully closed the connection.
			return err
		}
		return fmt.Errorf("cannot read rpcName: %w", err)
	}
	rpcName := string(ctx.dataBuf)

	// Initialize query tracing.
	traceEnabled, err := ctx.readBool()
	if err != nil {
		return fmt.Errorf("cannot read traceEnabled: %w", err)
	}
	ctx.qt = querytracer.New(traceEnabled, "rpc call %s() at vmstorage", rpcName)

	// Limit the time required for reading request args.
	if err := ctx.bc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("cannot set read deadline for reading request args: %w", err)
	}
	defer func() {
		_ = ctx.bc.SetReadDeadline(time.Time{})
	}()

	// Read the timeout for request execution.
	timeout, err := ctx.readUint32()
	if err != nil {
		return fmt.Errorf("cannot read timeout for the request %q: %w", rpcName, err)
	}
	ctx.timeout = uint64(timeout)
	ctx.deadline = fasttime.UnixTimestamp() + uint64(timeout)

	// Process the rpcName call.
	if err := s.processRPC(ctx, rpcName); err != nil {
		return fmt.Errorf("cannot execute %q: %w", rpcName, err)
	}

	// Finish query trace.
	ctx.qt.Done()
	traceJSON := ctx.qt.ToJSON()
	if err := ctx.writeString(traceJSON); err != nil {
		return fmt.Errorf("cannot send trace with length %d bytes to vmselect: %w", len(traceJSON), err)
	}
	return nil
}

func (s *Server) beginConcurrentRequest(ctx *vmselectRequestCtx) error {
	select {
	case s.concurrencyLimitCh <- struct{}{}:
		return nil
	default:
		d := time.Duration(ctx.timeout) * time.Second
		if d > s.limits.MaxQueueDuration {
			d = s.limits.MaxQueueDuration
		}
		t := timerpool.Get(d)
		s.concurrencyLimitReached.Inc()
		select {
		case s.concurrencyLimitCh <- struct{}{}:
			timerpool.Put(t)
			ctx.qt.Printf("wait in queue because -%s=%d concurrent requests are executed", s.limits.MaxConcurrentRequestsFlagName, s.limits.MaxConcurrentRequests)
			return nil
		case <-t.C:
			timerpool.Put(t)
			s.concurrencyLimitTimeout.Inc()
			return fmt.Errorf("couldn't start executing the request in %.3f seconds, since -%s=%d concurrent requests "+
				"are already executed. Possible solutions: to reduce the query load; to add more compute resources to the server; "+
				"to increase -%s=%d; to increase -%s",
				d.Seconds(), s.limits.MaxConcurrentRequestsFlagName, s.limits.MaxConcurrentRequests,
				s.limits.MaxQueueDurationFlagName, s.limits.MaxQueueDuration, s.limits.MaxConcurrentRequestsFlagName)
		}
	}
}

func (s *Server) endConcurrentRequest() {
	<-s.concurrencyLimitCh
}

func (s *Server) processRPC(ctx *vmselectRequestCtx, rpcName string) error {
	switch rpcName {
	case "searchAndEval_v1":
		return s.processSearchAndEval(ctx)
	case "seriesCount_v4":
		return s.processSeriesCount(ctx)
	case "sketchCachetatus_v5":
		return s.processSketchCacheStatus(ctx)
	case "deleteSeries_v5":
		return s.processDeleteSeries(ctx)
	case "registerMetricNames_v3":
		return s.processRegisterMetricNames(ctx)
	case "registerMetricNameFuncName_v1":
		return s.processRegisterMetricNameFuncName(ctx)
	default:
		return fmt.Errorf("unsupported rpcName: %q", rpcName)
	}
}

const maxMetricNameRawSize = 1024 * 1024
const maxMetricNamesPerRequest = 1024 * 1024

func (s *Server) processRegisterMetricNames(ctx *vmselectRequestCtx) error {
	s.registerMetricNamesRequests.Inc()

	// Read request
	metricsCount, err := ctx.readUint64()
	if err != nil {
		return fmt.Errorf("cannot read metricsCount: %w", err)
	}
	if metricsCount > maxMetricNamesPerRequest {
		return fmt.Errorf("too many metric names in a single request; got %d; mustn't exceed %d", metricsCount, maxMetricNamesPerRequest)
	}
	mrs := make([]storage.MetricRow, metricsCount)
	for i := 0; i < int(metricsCount); i++ {
		if err := ctx.readDataBufBytes(maxMetricNameRawSize); err != nil {
			return fmt.Errorf("cannot read metricNameRaw: %w", err)
		}
		mr := &mrs[i]
		mr.MetricNameRaw = append(mr.MetricNameRaw[:0], ctx.dataBuf...)
		n, err := ctx.readUint64()
		if err != nil {
			return fmt.Errorf("cannot read timestamp: %w", err)
		}
		mr.Timestamp = int64(n)
	}

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	// Register metric names from mrs.
	if err := s.api.RegisterMetricNames(ctx.qt, mrs, ctx.deadline); err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}
	return nil
}

func (s *Server) processRegisterMetricNameFuncName(ctx *vmselectRequestCtx) error {
	s.registerMetricNamesRequests.Inc()

	// Read request
	metricsCount, err := ctx.readUint64()
	if err != nil {
		return fmt.Errorf("cannot read metricsCount: %w", err)
	}
	if metricsCount > maxMetricNamesPerRequest {
		return fmt.Errorf("too many metric names in a single request; got %d; mustn't exceed %d", metricsCount, maxMetricNamesPerRequest)
	}
	mrs := make([]storage.MetricRow, metricsCount)
	for i := 0; i < int(metricsCount); i++ {
		if err := ctx.readDataBufBytes(maxMetricNameRawSize); err != nil {
			return fmt.Errorf("cannot read metricNameRaw: %w", err)
		}
		mr := &mrs[i]
		mr.MetricNameRaw = append(mr.MetricNameRaw[:0], ctx.dataBuf...)
	}

	window, err := ctx.readUint64()
	if err != nil {
		return fmt.Errorf("cannot read window: %w", err)
	}

	item_window, err := ctx.readUint64()
	if err != nil {
		return fmt.Errorf("cannot read item_window: %w", err)
	}

	funcNameID, err := ctx.readUint32()
	if err != nil {
		return fmt.Errorf("cannot read funcNameID: %w", err)
	}
	funcName := sketch.GetFuncName(funcNameID)

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	if err := s.api.RegisterMetricNameFuncName(ctx.qt, mrs, funcName, int64(window), int64(item_window), ctx.deadline); err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}
	return nil
}

func (s *Server) processDeleteSeries(ctx *vmselectRequestCtx) error {
	s.deleteSeriesRequests.Inc()

	// Read request
	if err := ctx.readSearchQuery(); err != nil {
		return err
	}

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	// Execute the request.
	deletedCount, err := s.api.DeleteSeries(ctx.qt, &ctx.sq, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}
	// Send deletedCount to vmselect.
	if err := ctx.writeUint64(uint64(deletedCount)); err != nil {
		return fmt.Errorf("cannot send deletedCount=%d: %w", deletedCount, err)
	}
	return nil
}

func (s *Server) processSeriesCount(ctx *vmselectRequestCtx) error {
	s.seriesCountRequests.Inc()

	// Read request
	accountID, projectID, err := ctx.readAccountIDProjectID()
	if err != nil {
		return err
	}

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	// Execute the request
	n, err := s.api.SeriesCount(ctx.qt, accountID, projectID, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	// Send series count to vmselect.
	if err := ctx.writeUint64(n); err != nil {
		return fmt.Errorf("cannot write series count to vmselect: %w", err)
	}
	return nil
}

func (s *Server) processSketchCacheStatus(ctx *vmselectRequestCtx) error {
	s.sketchCacheStatusRequests.Inc()

	// Read request
	if err := ctx.readSearchQuery(); err != nil {
		return err
	}

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	// Execute the request
	status, err := s.api.SketchCacheStatus(ctx.qt, &ctx.sq, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	// Send status to vmselect.
	return writeSketchCacheStatus(ctx, status)
}

func writeSketchCacheStatus(ctx *vmselectRequestCtx, status *sketch.SketchCacheStatus) error {
	if err := ctx.writeUint64(status.TotalSeries); err != nil {
		return fmt.Errorf("cannot write totalSeries to vmselect: %w", err)
	}
	return nil
}

func (s *Server) processSearchAndEval(ctx *vmselectRequestCtx) error {
	s.searchRequests.Inc()

	// Read request.
	if err := ctx.readSearchQuery(); err != nil {
		return err
	}

	if err := s.beginConcurrentRequest(ctx); err != nil {
		return ctx.writeErrorMessage(err)
	}
	defer s.endConcurrentRequest()

	// Evaluate and send the result to vmselect.
	s.api.SearchAndEval(ctx.qt, mrs, tr, funcName, ctx.deadline)

	// Send 'end of response' marker
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send 'end of response' marker")
	}
	return nil
}
