package opentsdb

import (
	"io"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	parser "github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/opentsdb"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/opentsdb/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="opentsdb"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="opentsdb"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="opentsdb"}`)
)

// InsertHandler processes remote write for OpenTSDB put protocol.
//
// See http://opentsdb.net/docs/build/html/api_telnet/put.html
func InsertHandler(at *auth.Token, r io.Reader) error {
	return stream.Parse(r, func(rows []parser.Row) error {
		return insertRows(at, rows)
	})
}

func insertRows(at *auth.Token, rows []parser.Row) error {
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
