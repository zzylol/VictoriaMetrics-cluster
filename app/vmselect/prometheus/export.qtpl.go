// Code generated by qtc from "export.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmselect/prometheus/export.qtpl:1
package prometheus

//line app/vmselect/prometheus/export.qtpl:1
import (
	"bytes"
	"math"
	"strings"
	"time"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/storage"
	"github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/export.qtpl:14
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmselect/prometheus/export.qtpl:14
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmselect/prometheus/export.qtpl:14
func StreamExportCSVLine(qw422016 *qt422016.Writer, xb *exportBlock, fieldNames []string) {
//line app/vmselect/prometheus/export.qtpl:15
	if len(xb.timestamps) == 0 || len(fieldNames) == 0 {
//line app/vmselect/prometheus/export.qtpl:15
		return
//line app/vmselect/prometheus/export.qtpl:15
	}
//line app/vmselect/prometheus/export.qtpl:16
	for i, timestamp := range xb.timestamps {
//line app/vmselect/prometheus/export.qtpl:17
		value := xb.values[i]

//line app/vmselect/prometheus/export.qtpl:18
		streamexportCSVField(qw422016, xb.mn, fieldNames[0], timestamp, value)
//line app/vmselect/prometheus/export.qtpl:19
		for _, fieldName := range fieldNames[1:] {
//line app/vmselect/prometheus/export.qtpl:19
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/export.qtpl:21
			streamexportCSVField(qw422016, xb.mn, fieldName, timestamp, value)
//line app/vmselect/prometheus/export.qtpl:22
		}
//line app/vmselect/prometheus/export.qtpl:23
		qw422016.N().S(`
`)
//line app/vmselect/prometheus/export.qtpl:24
	}
//line app/vmselect/prometheus/export.qtpl:25
}

//line app/vmselect/prometheus/export.qtpl:25
func WriteExportCSVLine(qq422016 qtio422016.Writer, xb *exportBlock, fieldNames []string) {
//line app/vmselect/prometheus/export.qtpl:25
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:25
	StreamExportCSVLine(qw422016, xb, fieldNames)
//line app/vmselect/prometheus/export.qtpl:25
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:25
}

//line app/vmselect/prometheus/export.qtpl:25
func ExportCSVLine(xb *exportBlock, fieldNames []string) string {
//line app/vmselect/prometheus/export.qtpl:25
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:25
	WriteExportCSVLine(qb422016, xb, fieldNames)
//line app/vmselect/prometheus/export.qtpl:25
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:25
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:25
	return qs422016
//line app/vmselect/prometheus/export.qtpl:25
}

//line app/vmselect/prometheus/export.qtpl:27
const rfc3339Milli = "2006-01-02T15:04:05.999Z07:00"

//line app/vmselect/prometheus/export.qtpl:28
func streamexportCSVField(qw422016 *qt422016.Writer, mn *storage.MetricName, fieldName string, timestamp int64, value float64) {
//line app/vmselect/prometheus/export.qtpl:29
	if fieldName == "__value__" {
//line app/vmselect/prometheus/export.qtpl:30
		qw422016.N().F(value)
//line app/vmselect/prometheus/export.qtpl:31
		return
//line app/vmselect/prometheus/export.qtpl:32
	}
//line app/vmselect/prometheus/export.qtpl:33
	if fieldName == "__timestamp__" {
//line app/vmselect/prometheus/export.qtpl:34
		qw422016.N().DL(timestamp)
//line app/vmselect/prometheus/export.qtpl:35
		return
//line app/vmselect/prometheus/export.qtpl:36
	}
//line app/vmselect/prometheus/export.qtpl:37
	if strings.HasPrefix(fieldName, "__timestamp__:") {
//line app/vmselect/prometheus/export.qtpl:38
		timeFormat := fieldName[len("__timestamp__:"):]

//line app/vmselect/prometheus/export.qtpl:39
		switch timeFormat {
//line app/vmselect/prometheus/export.qtpl:40
		case "unix_s":
//line app/vmselect/prometheus/export.qtpl:41
			qw422016.N().DL(timestamp / 1000)
//line app/vmselect/prometheus/export.qtpl:42
		case "unix_ms":
//line app/vmselect/prometheus/export.qtpl:43
			qw422016.N().DL(timestamp)
//line app/vmselect/prometheus/export.qtpl:44
		case "unix_ns":
//line app/vmselect/prometheus/export.qtpl:45
			qw422016.N().DL(timestamp * 1e6)
//line app/vmselect/prometheus/export.qtpl:46
		case "rfc3339":
//line app/vmselect/prometheus/export.qtpl:48
			bb := quicktemplate.AcquireByteBuffer()
			bb.B = time.Unix(timestamp/1000, (timestamp%1000)*1e6).AppendFormat(bb.B[:0], rfc3339Milli)

//line app/vmselect/prometheus/export.qtpl:51
			qw422016.N().Z(bb.B)
//line app/vmselect/prometheus/export.qtpl:53
			quicktemplate.ReleaseByteBuffer(bb)

//line app/vmselect/prometheus/export.qtpl:55
		default:
//line app/vmselect/prometheus/export.qtpl:56
			if strings.HasPrefix(timeFormat, "custom:") {
//line app/vmselect/prometheus/export.qtpl:58
				layout := timeFormat[len("custom:"):]
				bb := quicktemplate.AcquireByteBuffer()
				bb.B = time.Unix(timestamp/1000, (timestamp%1000)*1e6).AppendFormat(bb.B[:0], layout)

//line app/vmselect/prometheus/export.qtpl:62
				if bytes.ContainsAny(bb.B, `"`+",\n") {
//line app/vmselect/prometheus/export.qtpl:63
					qw422016.E().QZ(bb.B)
//line app/vmselect/prometheus/export.qtpl:64
				} else {
//line app/vmselect/prometheus/export.qtpl:65
					qw422016.N().Z(bb.B)
//line app/vmselect/prometheus/export.qtpl:66
				}
//line app/vmselect/prometheus/export.qtpl:68
				quicktemplate.ReleaseByteBuffer(bb)

//line app/vmselect/prometheus/export.qtpl:70
			} else {
//line app/vmselect/prometheus/export.qtpl:70
				qw422016.N().S(`Unsupported timeFormat=`)
//line app/vmselect/prometheus/export.qtpl:71
				qw422016.N().S(timeFormat)
//line app/vmselect/prometheus/export.qtpl:72
			}
//line app/vmselect/prometheus/export.qtpl:73
		}
//line app/vmselect/prometheus/export.qtpl:74
		return
//line app/vmselect/prometheus/export.qtpl:75
	}
//line app/vmselect/prometheus/export.qtpl:76
	v := mn.GetTagValue(fieldName)

//line app/vmselect/prometheus/export.qtpl:77
	if bytes.ContainsAny(v, `"`+",\n") {
//line app/vmselect/prometheus/export.qtpl:78
		qw422016.N().QZ(v)
//line app/vmselect/prometheus/export.qtpl:79
	} else {
//line app/vmselect/prometheus/export.qtpl:80
		qw422016.N().Z(v)
//line app/vmselect/prometheus/export.qtpl:81
	}
//line app/vmselect/prometheus/export.qtpl:82
}

//line app/vmselect/prometheus/export.qtpl:82
func writeexportCSVField(qq422016 qtio422016.Writer, mn *storage.MetricName, fieldName string, timestamp int64, value float64) {
//line app/vmselect/prometheus/export.qtpl:82
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:82
	streamexportCSVField(qw422016, mn, fieldName, timestamp, value)
//line app/vmselect/prometheus/export.qtpl:82
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:82
}

//line app/vmselect/prometheus/export.qtpl:82
func exportCSVField(mn *storage.MetricName, fieldName string, timestamp int64, value float64) string {
//line app/vmselect/prometheus/export.qtpl:82
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:82
	writeexportCSVField(qb422016, mn, fieldName, timestamp, value)
//line app/vmselect/prometheus/export.qtpl:82
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:82
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:82
	return qs422016
//line app/vmselect/prometheus/export.qtpl:82
}

//line app/vmselect/prometheus/export.qtpl:84
func StreamExportPrometheusLine(qw422016 *qt422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:85
	if len(xb.timestamps) == 0 {
//line app/vmselect/prometheus/export.qtpl:85
		return
//line app/vmselect/prometheus/export.qtpl:85
	}
//line app/vmselect/prometheus/export.qtpl:86
	bb := quicktemplate.AcquireByteBuffer()

//line app/vmselect/prometheus/export.qtpl:87
	writeprometheusMetricName(bb, xb.mn)

//line app/vmselect/prometheus/export.qtpl:88
	for i, ts := range xb.timestamps {
//line app/vmselect/prometheus/export.qtpl:89
		qw422016.N().Z(bb.B)
//line app/vmselect/prometheus/export.qtpl:89
		qw422016.N().S(` `)
//line app/vmselect/prometheus/export.qtpl:90
		qw422016.N().F(xb.values[i])
//line app/vmselect/prometheus/export.qtpl:90
		qw422016.N().S(` `)
//line app/vmselect/prometheus/export.qtpl:91
		qw422016.N().DL(ts)
//line app/vmselect/prometheus/export.qtpl:91
		qw422016.N().S(`
`)
//line app/vmselect/prometheus/export.qtpl:92
	}
//line app/vmselect/prometheus/export.qtpl:93
	quicktemplate.ReleaseByteBuffer(bb)

//line app/vmselect/prometheus/export.qtpl:94
}

//line app/vmselect/prometheus/export.qtpl:94
func WriteExportPrometheusLine(qq422016 qtio422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:94
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:94
	StreamExportPrometheusLine(qw422016, xb)
//line app/vmselect/prometheus/export.qtpl:94
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:94
}

//line app/vmselect/prometheus/export.qtpl:94
func ExportPrometheusLine(xb *exportBlock) string {
//line app/vmselect/prometheus/export.qtpl:94
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:94
	WriteExportPrometheusLine(qb422016, xb)
//line app/vmselect/prometheus/export.qtpl:94
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:94
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:94
	return qs422016
//line app/vmselect/prometheus/export.qtpl:94
}

//line app/vmselect/prometheus/export.qtpl:96
func StreamExportJSONLine(qw422016 *qt422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:97
	if len(xb.timestamps) == 0 {
//line app/vmselect/prometheus/export.qtpl:97
		return
//line app/vmselect/prometheus/export.qtpl:97
	}
//line app/vmselect/prometheus/export.qtpl:97
	qw422016.N().S(`{"metric":`)
//line app/vmselect/prometheus/export.qtpl:99
	streammetricNameObject(qw422016, xb.mn)
//line app/vmselect/prometheus/export.qtpl:99
	qw422016.N().S(`,"values":[`)
//line app/vmselect/prometheus/export.qtpl:101
	if len(xb.values) > 0 {
//line app/vmselect/prometheus/export.qtpl:102
		values := xb.values

//line app/vmselect/prometheus/export.qtpl:103
		streamconvertValueToSpecialJSON(qw422016, values[0])
//line app/vmselect/prometheus/export.qtpl:104
		values = values[1:]

//line app/vmselect/prometheus/export.qtpl:105
		for _, v := range values {
//line app/vmselect/prometheus/export.qtpl:105
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/export.qtpl:106
			streamconvertValueToSpecialJSON(qw422016, v)
//line app/vmselect/prometheus/export.qtpl:107
		}
//line app/vmselect/prometheus/export.qtpl:108
	}
//line app/vmselect/prometheus/export.qtpl:108
	qw422016.N().S(`],"timestamps":[`)
//line app/vmselect/prometheus/export.qtpl:111
	if len(xb.timestamps) > 0 {
//line app/vmselect/prometheus/export.qtpl:112
		timestamps := xb.timestamps

//line app/vmselect/prometheus/export.qtpl:113
		qw422016.N().DL(timestamps[0])
//line app/vmselect/prometheus/export.qtpl:114
		timestamps = timestamps[1:]

//line app/vmselect/prometheus/export.qtpl:115
		for _, ts := range timestamps {
//line app/vmselect/prometheus/export.qtpl:115
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/export.qtpl:116
			qw422016.N().DL(ts)
//line app/vmselect/prometheus/export.qtpl:117
		}
//line app/vmselect/prometheus/export.qtpl:118
	}
//line app/vmselect/prometheus/export.qtpl:118
	qw422016.N().S(`]}`)
//line app/vmselect/prometheus/export.qtpl:120
	qw422016.N().S(`
`)
//line app/vmselect/prometheus/export.qtpl:121
}

//line app/vmselect/prometheus/export.qtpl:121
func WriteExportJSONLine(qq422016 qtio422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:121
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:121
	StreamExportJSONLine(qw422016, xb)
//line app/vmselect/prometheus/export.qtpl:121
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:121
}

//line app/vmselect/prometheus/export.qtpl:121
func ExportJSONLine(xb *exportBlock) string {
//line app/vmselect/prometheus/export.qtpl:121
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:121
	WriteExportJSONLine(qb422016, xb)
//line app/vmselect/prometheus/export.qtpl:121
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:121
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:121
	return qs422016
//line app/vmselect/prometheus/export.qtpl:121
}

//line app/vmselect/prometheus/export.qtpl:123
func StreamExportPromAPILine(qw422016 *qt422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:123
	qw422016.N().S(`{"metric":`)
//line app/vmselect/prometheus/export.qtpl:125
	streammetricNameObject(qw422016, xb.mn)
//line app/vmselect/prometheus/export.qtpl:125
	qw422016.N().S(`,"values":`)
//line app/vmselect/prometheus/export.qtpl:126
	streamvaluesWithTimestamps(qw422016, xb.values, xb.timestamps)
//line app/vmselect/prometheus/export.qtpl:126
	qw422016.N().S(`}`)
//line app/vmselect/prometheus/export.qtpl:128
}

//line app/vmselect/prometheus/export.qtpl:128
func WriteExportPromAPILine(qq422016 qtio422016.Writer, xb *exportBlock) {
//line app/vmselect/prometheus/export.qtpl:128
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:128
	StreamExportPromAPILine(qw422016, xb)
//line app/vmselect/prometheus/export.qtpl:128
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:128
}

//line app/vmselect/prometheus/export.qtpl:128
func ExportPromAPILine(xb *exportBlock) string {
//line app/vmselect/prometheus/export.qtpl:128
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:128
	WriteExportPromAPILine(qb422016, xb)
//line app/vmselect/prometheus/export.qtpl:128
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:128
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:128
	return qs422016
//line app/vmselect/prometheus/export.qtpl:128
}

//line app/vmselect/prometheus/export.qtpl:130
func StreamExportPromAPIHeader(qw422016 *qt422016.Writer) {
//line app/vmselect/prometheus/export.qtpl:130
	qw422016.N().S(`{"status":"success","data":{"resultType":"matrix","result":[`)
//line app/vmselect/prometheus/export.qtpl:136
}

//line app/vmselect/prometheus/export.qtpl:136
func WriteExportPromAPIHeader(qq422016 qtio422016.Writer) {
//line app/vmselect/prometheus/export.qtpl:136
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:136
	StreamExportPromAPIHeader(qw422016)
//line app/vmselect/prometheus/export.qtpl:136
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:136
}

//line app/vmselect/prometheus/export.qtpl:136
func ExportPromAPIHeader() string {
//line app/vmselect/prometheus/export.qtpl:136
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:136
	WriteExportPromAPIHeader(qb422016)
//line app/vmselect/prometheus/export.qtpl:136
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:136
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:136
	return qs422016
//line app/vmselect/prometheus/export.qtpl:136
}

//line app/vmselect/prometheus/export.qtpl:138
func StreamExportPromAPIFooter(qw422016 *qt422016.Writer, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/export.qtpl:138
	qw422016.N().S(`]}`)
//line app/vmselect/prometheus/export.qtpl:142
	qt.Donef("export format=promapi")

//line app/vmselect/prometheus/export.qtpl:144
	streamdumpQueryTrace(qw422016, qt)
//line app/vmselect/prometheus/export.qtpl:144
	qw422016.N().S(`}`)
//line app/vmselect/prometheus/export.qtpl:146
}

//line app/vmselect/prometheus/export.qtpl:146
func WriteExportPromAPIFooter(qq422016 qtio422016.Writer, qt *querytracer.Tracer) {
//line app/vmselect/prometheus/export.qtpl:146
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:146
	StreamExportPromAPIFooter(qw422016, qt)
//line app/vmselect/prometheus/export.qtpl:146
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:146
}

//line app/vmselect/prometheus/export.qtpl:146
func ExportPromAPIFooter(qt *querytracer.Tracer) string {
//line app/vmselect/prometheus/export.qtpl:146
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:146
	WriteExportPromAPIFooter(qb422016, qt)
//line app/vmselect/prometheus/export.qtpl:146
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:146
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:146
	return qs422016
//line app/vmselect/prometheus/export.qtpl:146
}

//line app/vmselect/prometheus/export.qtpl:148
func streamprometheusMetricName(qw422016 *qt422016.Writer, mn *storage.MetricName) {
//line app/vmselect/prometheus/export.qtpl:149
	qw422016.N().Z(mn.MetricGroup)
//line app/vmselect/prometheus/export.qtpl:150
	if len(mn.Tags) > 0 {
//line app/vmselect/prometheus/export.qtpl:150
		qw422016.N().S(`{`)
//line app/vmselect/prometheus/export.qtpl:152
		tags := mn.Tags

//line app/vmselect/prometheus/export.qtpl:153
		qw422016.N().Z(tags[0].Key)
//line app/vmselect/prometheus/export.qtpl:153
		qw422016.N().S(`=`)
//line app/vmselect/prometheus/export.qtpl:153
		streamescapePrometheusLabel(qw422016, tags[0].Value)
//line app/vmselect/prometheus/export.qtpl:154
		tags = tags[1:]

//line app/vmselect/prometheus/export.qtpl:155
		for i := range tags {
//line app/vmselect/prometheus/export.qtpl:156
			tag := &tags[i]

//line app/vmselect/prometheus/export.qtpl:156
			qw422016.N().S(`,`)
//line app/vmselect/prometheus/export.qtpl:157
			qw422016.N().Z(tag.Key)
//line app/vmselect/prometheus/export.qtpl:157
			qw422016.N().S(`=`)
//line app/vmselect/prometheus/export.qtpl:157
			streamescapePrometheusLabel(qw422016, tag.Value)
//line app/vmselect/prometheus/export.qtpl:158
		}
//line app/vmselect/prometheus/export.qtpl:158
		qw422016.N().S(`}`)
//line app/vmselect/prometheus/export.qtpl:160
	}
//line app/vmselect/prometheus/export.qtpl:161
}

//line app/vmselect/prometheus/export.qtpl:161
func writeprometheusMetricName(qq422016 qtio422016.Writer, mn *storage.MetricName) {
//line app/vmselect/prometheus/export.qtpl:161
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:161
	streamprometheusMetricName(qw422016, mn)
//line app/vmselect/prometheus/export.qtpl:161
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:161
}

//line app/vmselect/prometheus/export.qtpl:161
func prometheusMetricName(mn *storage.MetricName) string {
//line app/vmselect/prometheus/export.qtpl:161
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:161
	writeprometheusMetricName(qb422016, mn)
//line app/vmselect/prometheus/export.qtpl:161
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:161
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:161
	return qs422016
//line app/vmselect/prometheus/export.qtpl:161
}

//line app/vmselect/prometheus/export.qtpl:163
func streamconvertValueToSpecialJSON(qw422016 *qt422016.Writer, v float64) {
//line app/vmselect/prometheus/export.qtpl:164
	if math.IsNaN(v) {
//line app/vmselect/prometheus/export.qtpl:164
		qw422016.N().S(`null`)
//line app/vmselect/prometheus/export.qtpl:166
	} else if math.IsInf(v, 0) {
//line app/vmselect/prometheus/export.qtpl:167
		if v > 0 {
//line app/vmselect/prometheus/export.qtpl:167
			qw422016.N().S(`"Infinity"`)
//line app/vmselect/prometheus/export.qtpl:169
		} else {
//line app/vmselect/prometheus/export.qtpl:169
			qw422016.N().S(`"-Infinity"`)
//line app/vmselect/prometheus/export.qtpl:171
		}
//line app/vmselect/prometheus/export.qtpl:172
	} else {
//line app/vmselect/prometheus/export.qtpl:173
		qw422016.N().F(v)
//line app/vmselect/prometheus/export.qtpl:174
	}
//line app/vmselect/prometheus/export.qtpl:175
}

//line app/vmselect/prometheus/export.qtpl:175
func writeconvertValueToSpecialJSON(qq422016 qtio422016.Writer, v float64) {
//line app/vmselect/prometheus/export.qtpl:175
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:175
	streamconvertValueToSpecialJSON(qw422016, v)
//line app/vmselect/prometheus/export.qtpl:175
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:175
}

//line app/vmselect/prometheus/export.qtpl:175
func convertValueToSpecialJSON(v float64) string {
//line app/vmselect/prometheus/export.qtpl:175
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:175
	writeconvertValueToSpecialJSON(qb422016, v)
//line app/vmselect/prometheus/export.qtpl:175
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:175
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:175
	return qs422016
//line app/vmselect/prometheus/export.qtpl:175
}

//line app/vmselect/prometheus/export.qtpl:177
func streamescapePrometheusLabel(qw422016 *qt422016.Writer, b []byte) {
//line app/vmselect/prometheus/export.qtpl:177
	qw422016.N().S(`"`)
//line app/vmselect/prometheus/export.qtpl:179
	for len(b) > 0 {
//line app/vmselect/prometheus/export.qtpl:180
		n := bytes.IndexAny(b, "\\\n\"")

//line app/vmselect/prometheus/export.qtpl:181
		if n < 0 {
//line app/vmselect/prometheus/export.qtpl:182
			qw422016.N().Z(b)
//line app/vmselect/prometheus/export.qtpl:183
			break
//line app/vmselect/prometheus/export.qtpl:184
		}
//line app/vmselect/prometheus/export.qtpl:185
		qw422016.N().Z(b[:n])
//line app/vmselect/prometheus/export.qtpl:186
		switch b[n] {
//line app/vmselect/prometheus/export.qtpl:187
		case '\\':
//line app/vmselect/prometheus/export.qtpl:187
			qw422016.N().S(`\\`)
//line app/vmselect/prometheus/export.qtpl:189
		case '\n':
//line app/vmselect/prometheus/export.qtpl:189
			qw422016.N().S(`\n`)
//line app/vmselect/prometheus/export.qtpl:191
		case '"':
//line app/vmselect/prometheus/export.qtpl:191
			qw422016.N().S(`\"`)
//line app/vmselect/prometheus/export.qtpl:193
		}
//line app/vmselect/prometheus/export.qtpl:194
		b = b[n+1:]

//line app/vmselect/prometheus/export.qtpl:195
	}
//line app/vmselect/prometheus/export.qtpl:195
	qw422016.N().S(`"`)
//line app/vmselect/prometheus/export.qtpl:197
}

//line app/vmselect/prometheus/export.qtpl:197
func writeescapePrometheusLabel(qq422016 qtio422016.Writer, b []byte) {
//line app/vmselect/prometheus/export.qtpl:197
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmselect/prometheus/export.qtpl:197
	streamescapePrometheusLabel(qw422016, b)
//line app/vmselect/prometheus/export.qtpl:197
	qt422016.ReleaseWriter(qw422016)
//line app/vmselect/prometheus/export.qtpl:197
}

//line app/vmselect/prometheus/export.qtpl:197
func escapePrometheusLabel(b []byte) string {
//line app/vmselect/prometheus/export.qtpl:197
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmselect/prometheus/export.qtpl:197
	writeescapePrometheusLabel(qb422016, b)
//line app/vmselect/prometheus/export.qtpl:197
	qs422016 := string(qb422016.B)
//line app/vmselect/prometheus/export.qtpl:197
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmselect/prometheus/export.qtpl:197
	return qs422016
//line app/vmselect/prometheus/export.qtpl:197
}
