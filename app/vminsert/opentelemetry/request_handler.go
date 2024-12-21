package opentelemetry

import (
	"fmt"
	"net/http"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/opentelemetry/firehose"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/opentelemetry/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="opentelemetry"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="opentelemetry"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="opentelemetry"}`)
)

// InsertHandler processes opentelemetry metrics.
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	var processBody func([]byte) ([]byte, error)
	if req.Header.Get("Content-Type") == "application/json" {
		if req.Header.Get("X-Amz-Firehose-Protocol-Version") != "" {
			processBody = firehose.ProcessRequestBody
		} else {
			return fmt.Errorf("json encoding isn't supported for opentelemetry format. Use protobuf encoding")
		}
	}
	return stream.ParseStream(req.Body, isGzipped, processBody, func(tss []prompbmarshal.TimeSeries) error {
		return insertRows(at, tss, extraLabels)
	})
}

func insertRows(at *auth.Token, tss []prompbmarshal.TimeSeries, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range tss {
		ts := &tss[i]
		rowsTotal += len(ts.Samples)
		ctx.Labels = ctx.Labels[:0]
		for _, label := range ts.Labels {
			ctx.AddLabel(label.Name, label.Value)
		}
		for _, label := range extraLabels {
			ctx.AddLabel(label.Name, label.Value)
		}
		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)
		storageNodeIdx := ctx.GetStorageNodeIdx(atLocal, ctx.Labels)
		ctx.MetricNameBuf = ctx.MetricNameBuf[:0]
		samples := ts.Samples
		for i := range samples {
			r := &samples[i]
			if len(ctx.MetricNameBuf) == 0 {
				ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ctx.Labels)
			}
			if err := ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, r.Timestamp, r.Value); err != nil {
				return err
			}
		}
		perTenantRows[*atLocal] += len(ts.Samples)
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
