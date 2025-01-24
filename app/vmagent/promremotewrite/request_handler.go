package promremotewrite

import (
	"net/http"

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

func InsertTest(testTimeseriesNum int) error {
	ctx := common.GetPushCtx()
	defer common.PutPushCtx(ctx)

	timeseries := make([]prompb.TimeSeries, testTimeseriesNum)
	total_time_length := 2160000

	for i := 0; i < total_time_length; i += 100 {

	}

	tssDst := ctx.WriteRequest.Timeseries[:0]
	labels := ctx.Labels[:0]
	samples := ctx.Samples[:0]
	for i := range timeseries {
		ts := &timeseries[i]

		labelsLen := len(labels)
		for i := range ts.Labels {
			label := &ts.Labels[i]
			labels = append(labels, prompbmarshal.Label{
				Name:  label.Name,
				Value: label.Value,
			})
		}
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
	if !remotewrite.TryPush(nil, &ctx.WriteRequest) {
		return remotewrite.ErrQueueFullHTTPRetry
	}

	return nil
}
