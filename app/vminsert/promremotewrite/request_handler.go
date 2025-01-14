package promremotewrite

import (
	"net/http"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompb"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/promremotewrite/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="promremotewrite"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="promremotewrite"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="promremotewrite"}`)
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
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx_sketch := netstorage.GetInsertCtxSketch()
	defer netstorage.PutInsertCtxSketch(ctx_sketch)

	ctx.Reset() // This line is required for initializing ctx internals.
	ctx_sketch.Reset()
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range timeseries {
		ts := &timeseries[i]
		rowsTotal += len(ts.Samples)
		ctx.Labels = ctx.Labels[:0]
		ctx_sketch.Labels = ctx_sketch.Labels[:0]

		srcLabels := ts.Labels
		for _, srcLabel := range srcLabels {
			ctx.AddLabel(srcLabel.Name, srcLabel.Value)
			ctx_sketch.AddLabel(srcLabel.Name, srcLabel.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
			ctx_sketch.AddLabel(label.Name, label.Value)
		}

		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)

		// sketchNodeIdx := ctx_sketch.GetSketchNodeIdx(atLocal, ctx.Labels)
		storageNodeIdx := ctx.GetStorageNodeIdx(atLocal, ctx.Labels)
		ctx.MetricNameBuf = ctx.MetricNameBuf[:0]
		// ctx_sketch.MetricNameBuf = ctx_sketch.MetricNameBuf[:0]
		samples := ts.Samples
		for i := range samples {
			r := &samples[i]
			if len(ctx.MetricNameBuf) == 0 {
				ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ctx.Labels)
				// ctx_sketch.MetricNameBuf = storage.MarshalMetricNameRaw(ctx_sketch.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ctx_sketch.Labels)
			}
			if err := ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, r.Timestamp, r.Value); err != nil {
				return err
			}
			// if err := ctx_sketch.WriteDataPointExtSketch(sketchNodeIdx, ctx_sketch.MetricNameBuf, r.Timestamp, r.Value); err != nil {
			// 	return err
			// }
		}
		perTenantRows[*atLocal] += len(ts.Samples)
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
