package netstorage

import (
	"flag"
	"sort"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
)

var sortLabels = flag.Bool("sortLabels", false, `Whether to sort labels for incoming samples before writing them to storage. `+
	`This may be needed for reducing memory usage at storage when the order of labels in incoming samples is random. `+
	`For example, if m{k1="v1",k2="v2"} may be sent as m{k2="v2",k1="v1"}. `+
	`Enabled sorting for labels can slow down ingestion performance a bit`)

// sortLabelsIfNeeded sorts labels if -sortLabels command-line flag is set
func (ctx *InsertCtx) sortLabelsIfNeeded() {
	if *sortLabels {
		sort.Sort(&ctx.Labels)
	}
}

type sortedLabels []prompbmarshal.Label

func (sl *sortedLabels) Len() int { return len(*sl) }
func (sl *sortedLabels) Less(i, j int) bool {
	a := *sl
	return string(a[i].Name) < string(a[j].Name)
}
func (sl *sortedLabels) Swap(i, j int) {
	a := *sl
	a[i], a[j] = a[j], a[i]
}
