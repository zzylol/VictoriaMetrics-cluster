package vlinsert

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/datadog"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/elasticsearch"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/journald"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/jsonline"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/loki"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/opentelemetry"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vlinsert/syslog"
)

// Init initializes vlinsert
func Init() {
	syslog.MustInit()
}

// Stop stops vlinsert
func Stop() {
	syslog.MustStop()
}

// RequestHandler handles insert requests for VictoriaLogs
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	if !strings.HasPrefix(path, "/insert/") {
		// Skip requests, which do not start with /insert/, since these aren't our requests.
		return false
	}
	path = strings.TrimPrefix(path, "/insert")
	path = strings.ReplaceAll(path, "//", "/")

	switch path {
	case "/jsonline":
		jsonline.RequestHandler(w, r)
		return true
	case "/ready":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		fmt.Fprintf(w, `{"status":"ok"}`)
		return true
	}
	switch {
	case strings.HasPrefix(path, "/elasticsearch/"):
		path = strings.TrimPrefix(path, "/elasticsearch")
		return elasticsearch.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/loki/"):
		path = strings.TrimPrefix(path, "/loki")
		return loki.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/opentelemetry/"):
		path = strings.TrimPrefix(path, "/opentelemetry")
		return opentelemetry.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/journald/"):
		path = strings.TrimPrefix(path, "/journald")
		return journald.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/datadog/"):
		path = strings.TrimPrefix(path, "/datadog")
		return datadog.RequestHandler(path, w, r)
	default:
		return false
	}
}
