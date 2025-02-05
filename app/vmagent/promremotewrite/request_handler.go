package promremotewrite

import (
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmagent/common"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmagent/remotewrite"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompb"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/promremotewrite/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vmagent_rows_inserted_total{type="promremotewrite"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vmagent_tenant_inserted_rows_total{type="promremotewrite"}`)
	rowsPerInsert      = metrics.NewHistogram(`vmagent_rows_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isVMRemoteWrite := req.Header.Get("Content-Encoding") == "zstd"
	return stream.Parse(req.Body, isVMRemoteWrite, func(tss []prompb.TimeSeries) error {
		return insertRows(at, tss, extraLabels)
	})
}

func insertRows(at *auth.Token, timeseries []prompb.TimeSeries, extraLabels []prompbmarshal.Label) error {
	ctx := common.GetPushCtx()
	defer common.PutPushCtx(ctx)

	rowsTotal := 0
	tssDst := ctx.WriteRequest.Timeseries[:0]
	labels := ctx.Labels[:0]
	samples := ctx.Samples[:0]
	for i := range timeseries {
		ts := &timeseries[i]
		rowsTotal += len(ts.Samples)
		labelsLen := len(labels)
		for i := range ts.Labels {
			label := &ts.Labels[i]
			labels = append(labels, prompbmarshal.Label{
				Name:  label.Name,
				Value: label.Value,
			})
		}
		labels = append(labels, extraLabels...)
		samplesLen := len(samples)
		for i := range ts.Samples {
			sample := &ts.Samples[i]
			samples = append(samples, prompbmarshal.Sample{
				Value:     sample.Value,
				Timestamp: sample.Timestamp,
			})
		}
		tssDst = append(tssDst, prompbmarshal.TimeSeries{
			Labels:  labels[labelsLen:],
			Samples: samples[samplesLen:],
		})
	}
	ctx.WriteRequest.Timeseries = tssDst
	ctx.Labels = labels
	ctx.Samples = samples
	if !remotewrite.TryPush(at, &ctx.WriteRequest) {
		return remotewrite.ErrQueueFullHTTPRetry
	}
	rowsInserted.Add(rowsTotal)
	if at != nil {
		rowsTenantInserted.Get(at).Add(rowsTotal)
	}
	rowsPerInsert.Update(float64(rowsTotal))
	return nil
}

func InsertTest(testTimeseriesNum, testStartSeriesID, testInsertNodeNum, testSampleLength int) (float64, float64, float64) {

	timeseries := make([]prompb.TimeSeries, testTimeseriesNum)
	total_time_length := testSampleLength
	var total_ops atomic.Int64
	total_ops.Store(0)

	for j := 0; j < testTimeseriesNum; j++ {
		fakeMetric := "machine" + strconv.Itoa(testStartSeriesID+j)
		timeseries[j].Labels = make([]prompb.Label, 1)
		timeseries[j].Labels[0].Name = "fake_metric"
		timeseries[j].Labels[0].Value = fakeMetric
		timeseries[j].Samples = make([]prompb.Sample, 0) // fake, will fill later
		// TODO: call register sketch instance here, now just register in vmsketch through args
	}

	timeDelta := int64(100)
	scrapeCountBatch := 100
	lbl_batch := 100
	start := time.Now()
	time_base := time.Now().Unix()
	for time_idx := 0; time_idx < total_time_length; time_idx += scrapeCountBatch {
		var wg sync.WaitGroup
		tss := timeseries
		for len(tss) > 0 {
			l := lbl_batch
			if len(tss) < lbl_batch {
				l = len(tss)
			}

			batch := tss[:l]
			tss = tss[l:]

			wg.Add(1)
			go func() {
				defer wg.Done()
				var s float64 = 1.01
				var v float64 = 1
				var RAND *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
				z := rand.NewZipf(RAND, s, v, uint64(100000))

				ctx := common.GetPushCtx()
				defer common.PutPushCtx(ctx)

				rowsTotal := 0
				tssDst := ctx.WriteRequest.Timeseries[:0]
				labels := ctx.Labels[:0]
				samples := ctx.Samples[:0]

				for i := 0; i < len(batch); i++ {
					// create labels
					labelsLen := len(labels)
					labels = append(labels, prompbmarshal.Label(batch[i].Labels[0]))
					// create samples
					ts := timeDelta*int64(time_idx) + time_base
					samplesLen := len(samples)
					for j := 0; j < scrapeCountBatch; j++ {
						ts += timeDelta
						rowsTotal += 1
						samples = append(samples, prompbmarshal.Sample{
							Timestamp: ts,
							Value:     float64(z.Uint64()),
						})
					}
					tssDst = append(tssDst, prompbmarshal.TimeSeries{
						Labels:  labels[labelsLen:],
						Samples: samples[samplesLen:],
					})
				}

				ctx.WriteRequest.Timeseries = tssDst
				ctx.Labels = labels
				ctx.Samples = samples
				if remotewrite.TryPush(nil, &ctx.WriteRequest) {
					total_ops.Add(int64(rowsTotal))
				}

			}()
		}
		wg.Wait()
	}
	duration := float64(time.Since(start).Seconds())
	throughput := float64(total_ops.Load()) / duration
	return float64(total_ops.Load()), duration, throughput
}
