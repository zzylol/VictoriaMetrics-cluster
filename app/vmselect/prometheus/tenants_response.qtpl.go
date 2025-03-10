// Code generated by qtc from "tenants_response.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmselect/prometheus/tenants_response.qtpl:3
package prometheus

//line app/vmselect/prometheus/tenants_response.qtpl:3
import (
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
)

// TenantsResponse generates response for /admin/tenants .

//line app/vmselect/prometheus/tenants_response.qtpl:8
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/tenants_response.qtpl:8
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmselect/prometheus/tenants_response.qtpl:8
func StreamTenantsResponse(qw422016 *qt422016.Writer, tenants []string, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/tenants_response.qtpl:8
	qw422016.N().S(`{"status":"success","data":[`)
//line app/vmselect/prometheus/tenants_response.qtpl:12
	for i, tenant := range tenants {
//line app/vmselect/prometheus/tenants_response.qtpl:13
		qw422016.N().Q(tenant)
//line app/vmselect/prometheus/tenants_response.qtpl:14
		if i+1 < len(tenants) {
//line app/vmselect/prometheus/tenants_response.qtpl:14
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/tenants_response.qtpl:14
		}
//line app/vmselect/prometheus/tenants_response.qtpl:15
	}
//line app/vmselect/prometheus/tenants_response.qtpl:15
	qw422016.N().S(`]`)
//line app/vmselect/prometheus/tenants_response.qtpl:18
	qt.Printf("generate response for %d tenants", len(tenants))
	qt.Done()

//line app/vmselect/prometheus/tenants_response.qtpl:21
	streamdumpQueryTrace(qw422016, qt)
//line app/vmselect/prometheus/tenants_response.qtpl:21
	qw422016.N().S(`}`)
//line app/vmselect/prometheus/tenants_response.qtpl:23
}

//line app/vmselect/prometheus/tenants_response.qtpl:23
func WriteTenantsResponse(qq422016 qtio422016.Writer, tenants []string, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/tenants_response.qtpl:23
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/tenants_response.qtpl:23
	StreamTenantsResponse(qw422016, tenants, qt)
//line app/vmselect/prometheus/tenants_response.qtpl:23
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/tenants_response.qtpl:23
}

//line app/vmselect/prometheus/tenants_response.qtpl:23
func TenantsResponse(tenants []string, qt *querytracer.Tracer) string {
//line app/vmselect/prometheus/tenants_response.qtpl:23
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/tenants_response.qtpl:23
	WriteTenantsResponse(qb422016, tenants, qt)
//line app/vmselect/prometheus/tenants_response.qtpl:23
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/tenants_response.qtpl:23
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/tenants_response.qtpl:23
	return qs422016
//line app/vmselect/prometheus/tenants_response.qtpl:23
}
