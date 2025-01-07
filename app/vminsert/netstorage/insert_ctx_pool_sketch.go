package netstorage

import (
	"sync"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/cgroup"
)

// GetInsertCtxSketch returns InsertCtxSketch from the pool.
//
// Call PutInsertCtxSketch for returning it to the pool.
func GetInsertCtxSketch() *InsertCtxSketch {
	select {
	case ctx := <-insertCtxPoolChSketch:
		return ctx
	default:
		if v := insertCtxPool.Get(); v != nil {
			return v.(*InsertCtxSketch)
		}
		return &InsertCtxSketch{}
	}
}

// PutInsertCtxSketch returns ctx to the pool.
//
// ctx cannot be used after the call.
func PutInsertCtxSketch(ctx *InsertCtxSketch) {
	ctx.Reset()
	select {
	case insertCtxPoolChSketch <- ctx:
	default:
		insertCtxPool.Put(ctx)
	}
}

var (
	insertCtxPoolSketch   sync.Pool
	insertCtxPoolChSketch = make(chan *InsertCtxSketch, cgroup.AvailableCPUs())
)
