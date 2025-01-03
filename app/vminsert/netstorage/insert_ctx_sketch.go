package netstorage

import (
	"github.com/cespare/xxhash/v2"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/auth"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
)

// GetSketchNodeIdx returns promstorage node index for the given at and labels.
// The returned index must be passed to WriteDataPoint.
func (ctx *InsertCtx) GetSketchNodeIdx(at *auth.Token, labels []prompbmarshal.Label) int {
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

// WriteDataPoint writes (timestamp, value) data point with the given at and labels to ctx buffer.
//
// caller must invoke TryPrepareLabels before using this function
func (ctx *InsertCtx) WriteDataPointSketch(at *auth.Token, labels []prompbmarshal.Label, timestamp int64, value float64) error {
	ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], at.AccountID, at.ProjectID, labels)
	storageNodeIdx := ctx.GetStorageNodeIdx(at, labels)
	return ctx.WriteDataPointExtSketch(storageNodeIdx, ctx.MetricNameBuf, timestamp, value)
}

// WriteDataPointExt writes the given metricNameRaw with (timestmap, value) to ctx buffer with the given storageNodeIdx.
//
// caller must invoke TryPrepareLabels before using this function
func (ctx *InsertCtx) WriteDataPointExtSketch(sketchNodeIdx int, metricNameRaw []byte, timestamp int64, value float64) error {
	br := &ctx.bufRowss[sketchNodeIdx]
	sknb := ctx.sknb
	sn := sknb.sns[sketchNodeIdx]
	bufNew := storage.MarshalMetricRow(br.buf, metricNameRaw, timestamp, value)
	if len(bufNew) >= maxBufSizePerStorageNode {
		// Send buf to sn, since it is too big.
		if err := br.pushTo(sknb, sn); err != nil {
			return err
		}
		br.buf = storage.MarshalMetricRow(bufNew[:0], metricNameRaw, timestamp, value)
	} else {
		br.buf = bufNew
	}
	br.rows++
	return nil
}
