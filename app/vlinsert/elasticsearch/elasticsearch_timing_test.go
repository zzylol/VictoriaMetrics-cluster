package elasticsearch

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/insertutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
)

func BenchmarkReadBulkRequest(b *testing.B) {
	b.Run("gzip:off", func(b *testing.B) {
		benchmarkReadBulkRequest(b, false)
	})
	b.Run("gzip:on", func(b *testing.B) {
		benchmarkReadBulkRequest(b, true)
	})
}

func benchmarkReadBulkRequest(b *testing.B, isGzip bool) {
	data := `{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:11.735Z","log":{"offset":71770,"file":{"path":"/var/log/auth.log"}},"message":"foobar"}
{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:12.735Z","message":"baz"}
{"create":{"_index":"filebeat-8.8.0"}}
{"message":"xyz","@timestamp":"2023-06-06T04:48:13.735Z","x":"y"}
`
	if isGzip {
		data = compressData(data)
	}
	dataBytes := bytesutil.ToUnsafeBytes(data)

	timeField := "@timestamp"
	msgFields := []string{"message"}
	blp := &insertutils.BenchmarkLogMessageProcessor{}

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.RunParallel(func(pb *testing.PB) {
		r := &bytes.Reader{}
		for pb.Next() {
			r.Reset(dataBytes)
			_, err := readBulkRequest("test", r, isGzip, timeField, msgFields, blp)
			if err != nil {
				panic(fmt.Errorf("unexpected error: %w", err))
			}
		}
	})
}
