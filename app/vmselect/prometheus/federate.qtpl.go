// Code generated by qtc from "federate.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmselect/prometheus/federate.qtpl:1
package prometheus

//line app/vmselect/prometheus/federate.qtpl:1
import (
	"math"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/netstorage"
)

// Federate writes rs in /federate format.// See https://prometheus.io/docs/prometheus/latest/federation/

//line app/vmselect/prometheus/federate.qtpl:11
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/federate.qtpl:11
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmselect/prometheus/federate.qtpl:11
func StreamFederate(qw422016 *qt422016.Writer, rs *netstorage.Result) {
//line app/vmselect/prometheus/federate.qtpl:13
	values := rs.Values
	timestamps := rs.Timestamps

//line app/vmselect/prometheus/federate.qtpl:16
	if len(timestamps) == 0 || len(values) == 0 {
//line app/vmselect/prometheus/federate.qtpl:16
		return
//line app/vmselect/prometheus/federate.qtpl:16
	}
//line app/vmselect/prometheus/federate.qtpl:18
	lastValue := values[len(values)-1]

//line app/vmselect/prometheus/federate.qtpl:20
	if math.IsNaN(lastValue) {
//line app/vmselect/prometheus/federate.qtpl:26
		return
//line app/vmselect/prometheus/federate.qtpl:27
	}
//line app/vmselect/prometheus/federate.qtpl:28
	streamprometheusMetricName(qw422016, &rs.MetricName)
//line app/vmselect/prometheus/federate.qtpl:28
	qw422016.N().S(` `)
//line app/vmselect/prometheus/federate.qtpl:29
	qw422016.N().F(lastValue)
//line app/vmselect/prometheus/federate.qtpl:29
	qw422016.N().S(` `)
//line app/vmselect/prometheus/federate.qtpl:30
	qw422016.N().DL(timestamps[len(timestamps)-1])
//line app/vmselect/prometheus/federate.qtpl:30
	qw422016.N().S(`
`)
//line app/vmselect/prometheus/federate.qtpl:31
}

//line app/vmselect/prometheus/federate.qtpl:31
func WriteFederate(qq422016 qtio422016.Writer, rs *netstorage.Result) {
//line app/vmselect/prometheus/federate.qtpl:31
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/federate.qtpl:31
	StreamFederate(qw422016, rs)
//line app/vmselect/prometheus/federate.qtpl:31
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/federate.qtpl:31
}

//line app/vmselect/prometheus/federate.qtpl:31
func Federate(rs *netstorage.Result) string {
//line app/vmselect/prometheus/federate.qtpl:31
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/federate.qtpl:31
	WriteFederate(qb422016, rs)
//line app/vmselect/prometheus/federate.qtpl:31
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/federate.qtpl:31
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/federate.qtpl:31
	return qs422016
//line app/vmselect/prometheus/federate.qtpl:31
}
