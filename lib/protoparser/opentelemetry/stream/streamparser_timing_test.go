package stream

import (
	"bytes"
	"testing"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/protoparser/opentelemetry/pb"
)

func BenchmarkParseStream(b *testing.B) {
	samples := []*pb.Metric{
		generateGauge("my-gauge", ""),
		generateHistogram("my-histogram", ""),
		generateSum("my-sum", "", false),
		generateSummary("my-summary", ""),
	}
	b.SetBytes(1)
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		pbRequest := pb.ExportMetricsServiceRequest{
			ResourceMetrics: []*pb.ResourceMetrics{generateOTLPSamples(samples)},
		}
		data := pbRequest.MarshalProtobuf(nil)

		for p.Next() {
			err := ParseStream(bytes.NewBuffer(data), false, nil, func(_ []prompbmarshal.TimeSeries) error {
				return nil
			})
			if err != nil {
				b.Fatalf("cannot parse stream: %s", err)
			}
		}
	})
}
