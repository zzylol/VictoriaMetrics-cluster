package datadogv1

import (
	"net/http"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/datadogutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/datadogv1"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/datadogv1/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="datadogv1"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="datadogv1"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="datadogv1"}`)
)

// InsertHandlerForHTTP processes remote write for DataDog POST /api/v1/series request.
func InsertHandlerForHTTP(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	ce := req.Header.Get("Content-Encoding")
	return stream.Parse(req.Body, ce, func(series []datadogv1.Series) error {
		return insertRows(at, series, extraLabels)
	})
}

func insertRows(at *auth.Token, series []datadogv1.Series, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset()
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range series {
		ss := &series[i]
		rowsTotal += len(ss.Points)
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel("", ss.Metric)
		if ss.Host != "" {
			ctx.AddLabel("host", ss.Host)
		}
		if ss.Device != "" {
			ctx.AddLabel("device", ss.Device)
		}
		for _, tag := range ss.Tags {
			name, value := datadogutils.SplitTag(tag)
			if name == "host" {
				name = "exported_host"
			}
			ctx.AddLabel(name, value)
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
		for _, pt := range ss.Points {
			timestamp := pt.Timestamp()
			value := pt.Value()
			if err := ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, timestamp, value); err != nil {
				return err
			}
		}
		perTenantRows[*atLocal] += len(ss.Points)
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
