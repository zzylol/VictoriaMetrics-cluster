// Code generated by qtc from "label_values_response.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmselect/prometheus/label_values_response.qtpl:3
package prometheus

//line app/vmselect/prometheus/label_values_response.qtpl:3
import (
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
)

// LabelValuesResponse generates response for /api/v1/label/<labelName>/values .See https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values

//line app/vmselect/prometheus/label_values_response.qtpl:9
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/label_values_response.qtpl:9
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmselect/prometheus/label_values_response.qtpl:9
func StreamLabelValuesResponse(qw422016 *qt422016.Writer, isPartial bool, labelValues []string, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/label_values_response.qtpl:9
	qw422016.N().S(`{"status":"success","isPartial":`)
//line app/vmselect/prometheus/label_values_response.qtpl:12
	if isPartial {
//line app/vmselect/prometheus/label_values_response.qtpl:12
		qw422016.N().S(`true`)
//line app/vmselect/prometheus/label_values_response.qtpl:12
	} else {
//line app/vmselect/prometheus/label_values_response.qtpl:12
		qw422016.N().S(`false`)
//line app/vmselect/prometheus/label_values_response.qtpl:12
	}
//line app/vmselect/prometheus/label_values_response.qtpl:12
	qw422016.N().S(`,"data":[`)
//line app/vmselect/prometheus/label_values_response.qtpl:14
	for i, labelValue := range labelValues {
//line app/vmselect/prometheus/label_values_response.qtpl:15
		qw422016.N().Q(labelValue)
//line app/vmselect/prometheus/label_values_response.qtpl:16
		if i+1 < len(labelValues) {
//line app/vmselect/prometheus/label_values_response.qtpl:16
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/label_values_response.qtpl:16
		}
//line app/vmselect/prometheus/label_values_response.qtpl:17
	}
//line app/vmselect/prometheus/label_values_response.qtpl:17
	qw422016.N().S(`]`)
//line app/vmselect/prometheus/label_values_response.qtpl:20
	qt.Printf("generate response for %d label values", len(labelValues))
	qt.Done()

//line app/vmselect/prometheus/label_values_response.qtpl:23
	streamdumpQueryTrace(qw422016, qt)
//line app/vmselect/prometheus/label_values_response.qtpl:23
	qw422016.N().S(`}`)
//line app/vmselect/prometheus/label_values_response.qtpl:25
}

//line app/vmselect/prometheus/label_values_response.qtpl:25
func WriteLabelValuesResponse(qq422016 qtio422016.Writer, isPartial bool, labelValues []string, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/label_values_response.qtpl:25
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/label_values_response.qtpl:25
	StreamLabelValuesResponse(qw422016, isPartial, labelValues, qt)
//line app/vmselect/prometheus/label_values_response.qtpl:25
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/label_values_response.qtpl:25
}

//line app/vmselect/prometheus/label_values_response.qtpl:25
func LabelValuesResponse(isPartial bool, labelValues []string, qt *querytracer.Tracer) string {
//line app/vmselect/prometheus/label_values_response.qtpl:25
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/label_values_response.qtpl:25
	WriteLabelValuesResponse(qb422016, isPartial, labelValues, qt)
//line app/vmselect/prometheus/label_values_response.qtpl:25
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/label_values_response.qtpl:25
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/label_values_response.qtpl:25
	return qs422016
//line app/vmselect/prometheus/label_values_response.qtpl:25
}
