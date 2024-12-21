package vmimport

import (
	"net/http"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/logger"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	parser "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/vmimport"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/vmimport/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="vmimport"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="vmimport"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="vmimport"}`)
)

// InsertHandler processes `/api/v1/import` request.
//
// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/6
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	return stream.Parse(req.Body, isGzipped, func(rows []parser.Row) error {
		return insertRows(at, rows, extraLabels)
	})
}

func insertRows(at *auth.Token, rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		rowsTotal += len(r.Values)
		ctx.Labels = ctx.Labels[:0]
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabelBytes(tag.Key, tag.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)
		ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ctx.Labels)
		storageNodeIdx := ctx.GetStorageNodeIdx(atLocal, ctx.Labels)
		values := r.Values
		timestamps := r.Timestamps
		if len(timestamps) != len(values) {
			logger.Panicf("BUG: len(timestamps)=%d must match len(values)=%d", len(timestamps), len(values))
		}
		for j, value := range values {
			timestamp := timestamps[j]
			if err := ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, timestamp, value); err != nil {
				return err
			}
		}
		perTenantRows[*atLocal] += len(r.Values)
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
