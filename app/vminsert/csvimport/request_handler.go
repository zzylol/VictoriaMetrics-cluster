package csvimport

import (
	"net/http"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/common"
	parser "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/csvimport"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/csvimport/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="csvimport"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="csvimport"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="csvimport"}`)
)

// InsertHandler processes /api/v1/import/csv requests.
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	return stream.Parse(req, func(rows []parser.Row) error {
		return insertRows(at, rows, extraLabels)
	})
}

func insertRows(at *auth.Token, rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel("", r.Metric)
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)
		if err := ctx.WriteDataPoint(atLocal, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}
		perTenantRows[*atLocal]++
	}
	rowsInserted.Add(len(rows))
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
