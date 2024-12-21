// Code generated by qtc from "header.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmalert/tpl/header.qtpl:1
package tpl

//line app/vmalert/tpl/header.qtpl:1
import (
	"net/http"
	"net/url"
	"path"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/utils"
)

//line app/vmalert/tpl/header.qtpl:9
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmalert/tpl/header.qtpl:9
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmalert/tpl/header.qtpl:9
func StreamHeader(qw422016 *qt422016.Writer, r *http.Request, navItems []NavItem, title string, userErr error) {
//line app/vmalert/tpl/header.qtpl:9
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:10
	prefix := utils.Prefix(r.URL.Path)

//line app/vmalert/tpl/header.qtpl:10
	qw422016.N().S(`
<!DOCTYPE html>
<html lang="en">
<head>
    <title>vmalert`)
//line app/vmalert/tpl/header.qtpl:14
	if title != "" {
//line app/vmalert/tpl/header.qtpl:14
		qw422016.N().S(` - `)
//line app/vmalert/tpl/header.qtpl:14
		qw422016.E().S(title)
//line app/vmalert/tpl/header.qtpl:14
	}
//line app/vmalert/tpl/header.qtpl:14
	qw422016.N().S(`</title>
    <link href="`)
//line app/vmalert/tpl/header.qtpl:15
	qw422016.E().S(prefix)
//line app/vmalert/tpl/header.qtpl:15
	qw422016.N().S(`static/css/bootstrap.min.css" rel="stylesheet" />
    <style>
        body{
          min-height: 75rem;
          padding-top: 4.5rem;
        }
        .group-heading {
            cursor: pointer;
            padding: 5px;
            margin-top: 5px;
            position: relative;
        }
        .group-heading .anchor {
            position:absolute;
            top:-60px;
        }
        .group-heading span {
            float: right;
            margin-left: 5px;
            margin-right: 5px;
        }
         .group-heading:hover {
            background-color: #f8f9fa!important;
        }
        .table {
            table-layout: fixed;
        }
        .table .error-cell{
            word-break: break-word;
            font-size: 14px;
        }
        pre {
            overflow: scroll;
            min-height: 30px;
            max-width: 100%;
        }
        pre::-webkit-scrollbar {
          -webkit-appearance: none;
          width: 0px;
          height: 5px;
        }
        pre::-webkit-scrollbar-thumb {
          border-radius: 5px;
          background-color: rgba(0,0,0,.5);
          -webkit-box-shadow: 0 0 1px rgba(255,255,255,.5);
        }
        textarea.curl-area{
            width: 100%;
            line-height: 1;
            font-size: 12px;
            border: none;
            margin: 0;
            padding: 0;
            overflow: scroll;
        }
    </style>
</head>
<body>
    `)
//line app/vmalert/tpl/header.qtpl:73
	streamprintNavItems(qw422016, r, title, navItems, userErr)
//line app/vmalert/tpl/header.qtpl:73
	qw422016.N().S(`
    <main class="px-2">
    `)
//line app/vmalert/tpl/header.qtpl:75
	streamerrorBody(qw422016, userErr)
//line app/vmalert/tpl/header.qtpl:75
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:76
}

//line app/vmalert/tpl/header.qtpl:76
func WriteHeader(qq422016 qtio422016.Writer, r *http.Request, navItems []NavItem, title string, userErr error) {
//line app/vmalert/tpl/header.qtpl:76
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/tpl/header.qtpl:76
	StreamHeader(qw422016, r, navItems, title, userErr)
//line app/vmalert/tpl/header.qtpl:76
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/tpl/header.qtpl:76
}

//line app/vmalert/tpl/header.qtpl:76
func Header(r *http.Request, navItems []NavItem, title string, userErr error) string {
//line app/vmalert/tpl/header.qtpl:76
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/tpl/header.qtpl:76
	WriteHeader(qb422016, r, navItems, title, userErr)
//line app/vmalert/tpl/header.qtpl:76
	qs422016 := string(qb422016.B)
//line app/vmalert/tpl/header.qtpl:76
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/tpl/header.qtpl:76
	return qs422016
//line app/vmalert/tpl/header.qtpl:76
}

//line app/vmalert/tpl/header.qtpl:80
type NavItem struct {
	Name string
	Url  string
}

//line app/vmalert/tpl/header.qtpl:86
func streamprintNavItems(qw422016 *qt422016.Writer, r *http.Request, current string, items []NavItem, userErr error) {
//line app/vmalert/tpl/header.qtpl:86
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:88
	prefix := utils.Prefix(r.URL.Path)

//line app/vmalert/tpl/header.qtpl:89
	qw422016.N().S(`
<nav class="navbar navbar-expand-md navbar-dark fixed-top bg-dark">
  <div class="container-fluid">
    <div class="collapse navbar-collapse" id="navbarCollapse">
        <ul class="navbar-nav me-auto mb-2 mb-md-0">
            `)
//line app/vmalert/tpl/header.qtpl:94
	for _, item := range items {
//line app/vmalert/tpl/header.qtpl:94
		qw422016.N().S(`
                <li class="nav-item">
                    `)
//line app/vmalert/tpl/header.qtpl:97
		u, _ := url.Parse(item.Url)

//line app/vmalert/tpl/header.qtpl:98
		qw422016.N().S(`
                    <a class="nav-link`)
//line app/vmalert/tpl/header.qtpl:99
		if current == item.Name {
//line app/vmalert/tpl/header.qtpl:99
			qw422016.N().S(` active`)
//line app/vmalert/tpl/header.qtpl:99
		}
//line app/vmalert/tpl/header.qtpl:99
		qw422016.N().S(`"
                       href="`)
//line app/vmalert/tpl/header.qtpl:100
		if u.IsAbs() {
//line app/vmalert/tpl/header.qtpl:100
			qw422016.E().S(item.Url)
//line app/vmalert/tpl/header.qtpl:100
		} else {
//line app/vmalert/tpl/header.qtpl:100
			qw422016.E().S(path.Join(prefix, item.Url))
//line app/vmalert/tpl/header.qtpl:100
		}
//line app/vmalert/tpl/header.qtpl:100
		qw422016.N().S(`">
                        `)
//line app/vmalert/tpl/header.qtpl:101
		qw422016.E().S(item.Name)
//line app/vmalert/tpl/header.qtpl:101
		qw422016.N().S(`
                    </a>
                </li>
            `)
//line app/vmalert/tpl/header.qtpl:104
	}
//line app/vmalert/tpl/header.qtpl:104
	qw422016.N().S(`
        </ul>
  </div>
  `)
//line app/vmalert/tpl/header.qtpl:107
	streamerrorIcon(qw422016, userErr)
//line app/vmalert/tpl/header.qtpl:107
	qw422016.N().S(`
</nav>
`)
//line app/vmalert/tpl/header.qtpl:109
}

//line app/vmalert/tpl/header.qtpl:109
func writeprintNavItems(qq422016 qtio422016.Writer, r *http.Request, current string, items []NavItem, userErr error) {
//line app/vmalert/tpl/header.qtpl:109
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/tpl/header.qtpl:109
	streamprintNavItems(qw422016, r, current, items, userErr)
//line app/vmalert/tpl/header.qtpl:109
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/tpl/header.qtpl:109
}

//line app/vmalert/tpl/header.qtpl:109
func printNavItems(r *http.Request, current string, items []NavItem, userErr error) string {
//line app/vmalert/tpl/header.qtpl:109
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/tpl/header.qtpl:109
	writeprintNavItems(qb422016, r, current, items, userErr)
//line app/vmalert/tpl/header.qtpl:109
	qs422016 := string(qb422016.B)
//line app/vmalert/tpl/header.qtpl:109
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/tpl/header.qtpl:109
	return qs422016
//line app/vmalert/tpl/header.qtpl:109
}

//line app/vmalert/tpl/header.qtpl:111
func streamerrorIcon(qw422016 *qt422016.Writer, err error) {
//line app/vmalert/tpl/header.qtpl:111
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:112
	if err != nil {
//line app/vmalert/tpl/header.qtpl:112
		qw422016.N().S(`
<div class="d-flex" data-bs-toggle="tooltip" data-bs-placement="left" title="Configuration file failed to reload! Click to see more details.">
  <a type="button" data-bs-toggle="collapse" href="#reload-groups-error">
      <span class="text-danger">
          <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-exclamation-triangle-fill" viewBox="0 0 16 16">
              <path d="M8.982 1.566a1.13 1.13 0 0 0-1.96 0L.165 13.233c-.457.778.091 1.767.98 1.767h13.713c.889 0 1.438-.99.98-1.767L8.982 1.566zM8 5c.535 0 .954.462.9.995l-.35 3.507a.552.552 0 0 1-1.1 0L7.1 5.995A.905.905 0 0 1 8 5zm.002 6a1 1 0 1 1 0 2 1 1 0 0 1 0-2z"/>
          </svg>
      </span>
  </a>
</div>
`)
//line app/vmalert/tpl/header.qtpl:122
	}
//line app/vmalert/tpl/header.qtpl:122
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:123
}

//line app/vmalert/tpl/header.qtpl:123
func writeerrorIcon(qq422016 qtio422016.Writer, err error) {
//line app/vmalert/tpl/header.qtpl:123
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/tpl/header.qtpl:123
	streamerrorIcon(qw422016, err)
//line app/vmalert/tpl/header.qtpl:123
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/tpl/header.qtpl:123
}

//line app/vmalert/tpl/header.qtpl:123
func errorIcon(err error) string {
//line app/vmalert/tpl/header.qtpl:123
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/tpl/header.qtpl:123
	writeerrorIcon(qb422016, err)
//line app/vmalert/tpl/header.qtpl:123
	qs422016 := string(qb422016.B)
//line app/vmalert/tpl/header.qtpl:123
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/tpl/header.qtpl:123
	return qs422016
//line app/vmalert/tpl/header.qtpl:123
}

//line app/vmalert/tpl/header.qtpl:125
func streamerrorBody(qw422016 *qt422016.Writer, err error) {
//line app/vmalert/tpl/header.qtpl:125
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:126
	if err != nil {
//line app/vmalert/tpl/header.qtpl:126
		qw422016.N().S(`
<div class="collapse mt-2 mb-2" id="reload-groups-error">
  <div class="card card-body">
    `)
//line app/vmalert/tpl/header.qtpl:129
		qw422016.E().S(err.Error())
//line app/vmalert/tpl/header.qtpl:129
		qw422016.N().S(`
  </div>
</div>
`)
//line app/vmalert/tpl/header.qtpl:132
	}
//line app/vmalert/tpl/header.qtpl:132
	qw422016.N().S(`
`)
//line app/vmalert/tpl/header.qtpl:133
}

//line app/vmalert/tpl/header.qtpl:133
func writeerrorBody(qq422016 qtio422016.Writer, err error) {
//line app/vmalert/tpl/header.qtpl:133
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/tpl/header.qtpl:133
	streamerrorBody(qw422016, err)
//line app/vmalert/tpl/header.qtpl:133
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/tpl/header.qtpl:133
}

//line app/vmalert/tpl/header.qtpl:133
func errorBody(err error) string {
//line app/vmalert/tpl/header.qtpl:133
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/tpl/header.qtpl:133
	writeerrorBody(qb422016, err)
//line app/vmalert/tpl/header.qtpl:133
	qs422016 := string(qb422016.B)
//line app/vmalert/tpl/header.qtpl:133
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/tpl/header.qtpl:133
	return qs422016
//line app/vmalert/tpl/header.qtpl:133
}
