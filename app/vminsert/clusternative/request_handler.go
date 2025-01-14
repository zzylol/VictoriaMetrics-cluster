package clusternative

import (
	"errors"
	"fmt"
	"net"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/netstorage"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/handshake"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/clusternative/stream"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/tenantmetrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="clusternative"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="clusternative"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="clusternative"}`)
)

// InsertHandler processes data from vminsert nodes.
func InsertHandler(c net.Conn) error {
	// There is no need in response compression, since
	// lower-level vminsert sends only small packets to upper-level vminsert.
	bc, err := handshake.VMInsertServer(c, 0)
	if err != nil {
		if errors.Is(err, handshake.ErrIgnoreHealthcheck) {
			return nil
		}
		return fmt.Errorf("cannot perform vminsert handshake with client %q: %w", c.RemoteAddr(), err)
	}
	return stream.Parse(bc, func(rows []storage.MetricRow) error {
		return insertRows(rows)
	}, nil)
}

func insertRows(rows []storage.MetricRow) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	// ctx_sketch := netstorage.GetInsertCtxSketch()
	// defer netstorage.PutInsertCtxSketch(ctx_sketch)

	ctx.Reset() // This line is required for initializing ctx internals.
	// ctx_sketch.Reset()

	hasRelabeling := relabel.HasRelabeling()
	var at auth.Token
	var rowsPerTenant *metrics.Counter
	var mn storage.MetricName
	for i := range rows {
		mr := &rows[i]
		if err := mn.UnmarshalRaw(mr.MetricNameRaw); err != nil {
			return fmt.Errorf("cannot unmarshal MetricNameRaw: %w", err)
		}
		if rowsPerTenant == nil || mn.AccountID != at.AccountID || mn.ProjectID != at.ProjectID {
			at.AccountID = mn.AccountID
			at.ProjectID = mn.ProjectID
			rowsPerTenant = rowsTenantInserted.Get(&at)
		}
		ctx.Labels = ctx.Labels[:0]
		// ctx_sketch.Labels = ctx_sketch.Labels[:0]
		ctx.AddLabelBytes(nil, mn.MetricGroup)
		// ctx_sketch.AddLabelBytes(nil, mn.MetricGroup)
		for j := range mn.Tags {
			tag := &mn.Tags[j]
			ctx.AddLabelBytes(tag.Key, tag.Value)
			// ctx_sketch.AddLabelBytes(tag.Key, tag.Value)
		}
		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		if err := ctx.WriteDataPoint(&at, ctx.Labels, mr.Timestamp, mr.Value); err != nil {
			return err
		}
		// if err := ctx_sketch.WriteDataPointSketch(&at, ctx_sketch.Labels, mr.Timestamp, mr.Value); err != nil {
		// 	return err
		// }
		rowsPerTenant.Inc()
	}
	rowsInserted.Add(len(rows))
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
