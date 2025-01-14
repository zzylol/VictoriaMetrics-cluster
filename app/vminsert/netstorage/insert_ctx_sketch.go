package netstorage

import (
	"fmt"
	"net/http"

	"github.com/cespare/xxhash/v2"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vminsert/relabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/httpserver"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
)

// InsertCtx is a generic context for inserting data.
//
// InsertCtx.Reset must be called before the first usage.
type InsertCtxSketch struct {
	sknb          *sketchNodesBucket
	Labels        sortedLabels
	MetricNameBuf []byte

	bufRowss []bufRows

	labelsBuf []byte

	relabelCtx relabel.Ctx

	at auth.Token
}

// Reset resets ctx.
func (ctx *InsertCtxSketch) Reset() {
	ctx.sknb = getSketchNodesBucket()

	labels := ctx.Labels
	for i := range labels {
		labels[i] = prompbmarshal.Label{}
	}
	ctx.Labels = labels[:0]

	ctx.MetricNameBuf = ctx.MetricNameBuf[:0]

	if ctx.bufRowss == nil || len(ctx.bufRowss) != len(ctx.sknb.sns) {
		ctx.bufRowss = make([]bufRows, len(ctx.sknb.sns))
	}
	for i := range ctx.bufRowss {
		ctx.bufRowss[i].reset()
	}
	ctx.labelsBuf = ctx.labelsBuf[:0]
	ctx.relabelCtx.Reset()
	ctx.at.Set(0, 0)
}

// GetSketchNodeIdx returns promstorage node index for the given at and labels.
// The returned index must be passed to WriteDataPoint.
func (ctx *InsertCtxSketch) GetSketchNodeIdx(at *auth.Token, labels []prompbmarshal.Label) int {
	if len(ctx.sknb.sns) == 1 {
		// Fast path - only a single storage node.
		return 0
	}

	buf := ctx.labelsBuf[:0]
	buf = encoding.MarshalUint32(buf, at.AccountID)
	buf = encoding.MarshalUint32(buf, at.ProjectID)
	for i := range labels {
		label := &labels[i]
		buf = marshalStringFast(buf, label.Name)
		buf = marshalStringFast(buf, label.Value)
	}
	h := xxhash.Sum64(buf)
	ctx.labelsBuf = buf

	// Do not exclude unavailable storage nodes in order to properly account for rerouted rows in storageNode.push().
	idx := ctx.sknb.nodesHash.getNodeIdx(h, nil)
	return idx
}

// AddLabel adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtxSketch) AddLabel(name, value string) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.Labels = append(ctx.Labels, prompbmarshal.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  name,
		Value: value,
	})
}

// AddLabelBytes adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtxSketch) AddLabelBytes(name, value []byte) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/zzylol/VictoriaMetrics-cluster/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.Labels = append(ctx.Labels, prompbmarshal.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  bytesutil.ToUnsafeString(name),
		Value: bytesutil.ToUnsafeString(value),
	})
}

// WriteDataPoint writes (timestamp, value) data point with the given at and labels to ctx buffer.
//
// caller must invoke TryPrepareLabels before using this function
func (ctx *InsertCtxSketch) WriteDataPointSketch(at *auth.Token, labels []prompbmarshal.Label, timestamp int64, value float64) error {
	ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], at.AccountID, at.ProjectID, labels)
	storageNodeIdx := ctx.GetSketchNodeIdx(at, labels)
	return ctx.WriteDataPointExtSketch(storageNodeIdx, ctx.MetricNameBuf, timestamp, value)
}

// WriteDataPointExt writes the given metricNameRaw with (timestmap, value) to ctx buffer with the given storageNodeIdx.
//
// caller must invoke TryPrepareLabels before using this function
func (ctx *InsertCtxSketch) WriteDataPointExtSketch(sketchNodeIdx int, metricNameRaw []byte, timestamp int64, value float64) error {
	br := &ctx.bufRowss[sketchNodeIdx]
	sknb := ctx.sknb
	sn := sknb.sns[sketchNodeIdx]
	bufNew := storage.MarshalMetricRow(br.buf, metricNameRaw, timestamp, value)
	if len(bufNew) >= maxBufSizePerStorageNode {
		// Send buf to sn, since it is too big.
		if err := br.pushToSketch(sknb, sn); err != nil {
			return err
		}
		br.buf = storage.MarshalMetricRow(bufNew[:0], metricNameRaw, timestamp, value)
	} else {
		br.buf = bufNew
	}
	br.rows++
	return nil
}

func (br *bufRows) pushToSketch(snb *sketchNodesBucket, sn *sketchNode) error {
	bufLen := len(br.buf)
	err := sn.push(snb, br.buf, br.rows)
	br.reset()
	if err != nil {
		return &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("cannot send %d bytes to sketchNode %q: %w", bufLen, sn.dialer.Addr(), err),
			StatusCode: http.StatusServiceUnavailable,
		}
	}
	return nil
}
