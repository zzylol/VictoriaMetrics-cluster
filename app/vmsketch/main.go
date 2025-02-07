package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmsketch/servers"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/buildinfo"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/envflag"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/flagutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/httpserver"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/procutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/pushmetrics"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/stringsutil"
)

var (
	throughput_test_threshold = flag.Int64("throughput_test_threshold", 2160000*10000, "The sample threshold for throughput test")
	testWindowSize   = flag.Int("testWindowSize", 10000, "Window size")
	testAlgo         = flag.String("testAlgo", "sampling", "promsketch algorithm tested")
	httpListenAddrs  = flagutil.NewArrayString("httpListenAddr", "Address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol")
	useProxyProtocol = flagutil.NewArrayBool("httpListenAddr.useProxyProtocol", "Whether to use proxy protocol for connections accepted at the given -httpListenAddr . "+
		"See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . "+
		"With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing")
	vminsertAddr      = flag.String("vminsertAddr", ":8500", "TCP address to accept connections from vminsert services")
	vmselectAddr      = flag.String("vmselectAddr", ":8501", "TCP address to accept connections from vmselect services")
	snapshotAuthKey   = flagutil.NewPassword("snapshotAuthKey", "authKey, which must be passed in query string to /snapshot* pages")
	forceMergeAuthKey = flagutil.NewPassword("forceMergeAuthKey", "authKey, which must be passed in query string to /internal/force_merge pages")
	forceFlushAuthKey = flagutil.NewPassword("forceFlushAuthKey", "authKey, which must be passed in query string to /internal/force_flush pages")
	snapshotsMaxAge   = flagutil.NewRetentionDuration("snapshotsMaxAge", "0", "Automatically delete snapshots older than -snapshotsMaxAge if it is set to non-zero duration. Make sure that backup process has enough time to finish the backup before the corresponding snapshot is automatically deleted")
	_                 = flag.Duration("snapshotCreateTimeout", 0, "Deprecated: this flag does nothing")

	_ = flag.Duration("finalMergeDelay", 0, "Deprecated: this flag does nothing")
	_ = flag.Int("bigMergeConcurrency", 0, "Deprecated: this flag does nothing")
	_ = flag.Int("smallMergeConcurrency", 0, "Deprecated: this flag does nothing")

	retentionTimezoneOffset = flag.Duration("retentionTimezoneOffset", 0, "The offset for performing indexdb rotation. "+
		"If set to 0, then the indexdb rotation is performed at 4am UTC time per each -retentionPeriod. "+
		"If set to 2h, then the indexdb rotation is performed at 4am EET time (the timezone with +2h offset)")
	minScrapeInterval = flag.Duration("dedup.minScrapeInterval", 0, "Leave only the last sample in every time series per each discrete interval "+
		"equal to -dedup.minScrapeInterval > 0. See https://docs.victoriametrics.com/#deduplication for details")
	inmemoryDataFlushInterval = flag.Duration("inmemoryDataFlushInterval", 5*time.Second, "The interval for guaranteed saving of in-memory data to disk. "+
		"The saved data survives unclean shutdowns such as OOM crash, hardware reset, SIGKILL, etc. "+
		"Bigger intervals may help increase the lifetime of flash storage with limited write cycles (e.g. Raspberry PI). "+
		"Smaller intervals increase disk IO load. Minimum supported value is 1s")

	logNewSeries = flag.Bool("logNewSeries", false, "Whether to log new series. This option is for debug purposes only. It can lead to performance issues "+
		"when big number of new series are ingested into VictoriaMetrics")
	maxHourlySeries = flag.Int("storage.maxHourlySeries", 0, "The maximum number of unique series can be added to the storage during the last hour. "+
		"Excess series are logged and dropped. This can be useful for limiting series cardinality. See https://docs.victoriametrics.com/#cardinality-limiter . "+
		"See also -storage.maxDailySeries")
	maxDailySeries = flag.Int("storage.maxDailySeries", 0, "The maximum number of unique series can be added to the storage during the last 24 hours. "+
		"Excess series are logged and dropped. This can be useful for limiting series churn rate. See https://docs.victoriametrics.com/#cardinality-limiter . "+
		"See also -storage.maxHourlySeries")

	minFreeDiskSpaceBytes = flagutil.NewBytes("storage.minFreeDiskSpaceBytes", 10e6, "The minimum free disk space at -storageDataPath after which the storage stops accepting new data")

	cacheSizeStorageTSID = flagutil.NewBytes("storage.cacheSizeStorageTSID", 0, "Overrides max size for storage/tsid cache. "+
		"See https://docs.victoriametrics.com/single-server-victoriametrics/#cache-tuning")
	cacheSizeIndexDBIndexBlocks = flagutil.NewBytes("storage.cacheSizeIndexDBIndexBlocks", 0, "Overrides max size for indexdb/indexBlocks cache. "+
		"See https://docs.victoriametrics.com/single-server-victoriametrics/#cache-tuning")
	cacheSizeIndexDBDataBlocks = flagutil.NewBytes("storage.cacheSizeIndexDBDataBlocks", 0, "Overrides max size for indexdb/dataBlocks cache. "+
		"See https://docs.victoriametrics.com/single-server-victoriametrics/#cache-tuning")
	cacheSizeIndexDBTagFilters = flagutil.NewBytes("storage.cacheSizeIndexDBTagFilters", 0, "Overrides max size for indexdb/tagFiltersToMetricIDs cache. "+
		"See https://docs.victoriametrics.com/single-server-victoriametrics/#cache-tuning")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	logger.Infof("opening sketch in memory")
	startTime := time.Now()
	sketch := sketch.MustOpenSketchCache(*testWindowSize, *testAlgo)

	logger.Infof("successfully opened sketch in %.3f seconds;", time.Since(startTime).Seconds())

	// register sketch metrics
	sketchMetrics := metrics.NewSet()
	sketchMetrics.RegisterMetricsWriter(func(w io.Writer) {
		writeSketchMetrics(w, sketch)
	})
	metrics.RegisterSet(sketchMetrics)

	common.StartUnmarshalWorkers()

	servers.GetMaxUniqueTimeSeries() // for init and logging only.
	vminsertSrv, err := servers.NewVMInsertServer(*vminsertAddr, sketch)
	if err != nil {
		logger.Fatalf("cannot create a server with -vminsertAddr=%s: %s", *vminsertAddr, err)
	}
	vmselectSrv, err := servers.NewVMSelectServer(*vmselectAddr, sketch)
	if err != nil {
		logger.Fatalf("cannot create a server with -vmselectAddr=%s: %s", *vmselectAddr, err)
	}

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":8582"}
	}

	vminsertSrv.Throughput_test_threshold = *throughput_test_threshold
	vminsertSrv.Throughput_start_time = time.Now()

	requestHandler := newRequestHandler(sketch)
	go httpserver.Serve(listenAddrs, useProxyProtocol, requestHandler)

	pushmetrics.Init()
	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)
	pushmetrics.Stop()

	logger.Infof("gracefully shutting down http service at %q", listenAddrs)
	startTime = time.Now()
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Fatalf("cannot stop http service: %s", err)
	}
	logger.Infof("successfully shut down http service in %.3f seconds", time.Since(startTime).Seconds())

	logger.Infof("gracefully shutting down the service")
	startTime = time.Now()

	// deregister sketch metrics
	metrics.UnregisterSet(sketchMetrics, true)
	sketchMetrics = nil

	vmselectSrv.MustStop()
	vminsertSrv.MustStop()
	common.StopUnmarshalWorkers()
	logger.Infof("successfully shut down the service in %.3f seconds", time.Since(startTime).Seconds())

	logger.Infof("gracefully closing the sketch")
	startTime = time.Now()
	sketch.MustClose()
	logger.Infof("successfully closed the sketch in %.3f seconds", time.Since(startTime).Seconds())

	logger.Infof("the vmsketch has been stopped")
}

func newRequestHandler(sketch *sketch.Sketch) httpserver.RequestHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/" {
			if r.Method != http.MethodGet {
				return false
			}
			w.Header().Add("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, `vmsketch - a component of VictoriaMetrics cluster<br/>
			<a href="https://docs.victoriametrics.com/cluster-victoriametrics/">docs</a><br>
`)
			return true
		}
		return requestHandler(w, r, sketch)
	}
}

func requestHandler(w http.ResponseWriter, r *http.Request, sketch *sketch.Sketch) bool {
	path := r.URL.Path
	switch path {
	// TODO
	default:
		return false
	}
}

func writeSketchMetrics(w io.Writer, sketch *sketch.Sketch) {

	isReadOnly := 0
	if sketch.IsReadOnly() {
		isReadOnly = 1
	}
	metrics.WriteGaugeUint64(w, fmt.Sprintf(`vm_sketch_is_read_only`), uint64(isReadOnly))

}

func jsonResponseError(w http.ResponseWriter, err error) {
	logger.Errorf("%s", err)
	w.WriteHeader(http.StatusInternalServerError)
	errStr := err.Error()
	fmt.Fprintf(w, `{"status":"error","msg":%s}`, stringsutil.JSONString(errStr))
}

func usage() {
	const s = `vmsketch caches time series intermediate results based on query statistics and returns the queried results to vmselect.`
	flagutil.Usage(s)
}
