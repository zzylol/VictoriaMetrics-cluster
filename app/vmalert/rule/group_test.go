package rule

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/config"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/datasource"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/notifier"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/remotewrite"
	"github.com/zzylol/VictoriaMetrics-cluster/app/vmalert/templates"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/promutils"
)

func init() {
	// Disable rand sleep on group start during tests in order to speed up test execution.
	// Rand sleep is needed only in prod code.
	SkipRandSleepOnGroupStart = true
}

func TestMain(m *testing.M) {
	if err := templates.Load([]string{}, url.URL{}); err != nil {
		fmt.Println("failed to load template for test")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestUpdateWith(t *testing.T) {
	f := func(currentRules, newRules []config.Rule) {
		t.Helper()

		g := &Group{
			Name: "test",
		}
		qb := &datasource.FakeQuerier{}
		for _, r := range currentRules {
			r.ID = config.HashRule(r)
			g.Rules = append(g.Rules, g.newRule(qb, r))
		}

		ng := &Group{
			Name: "test",
		}
		for _, r := range newRules {
			r.ID = config.HashRule(r)
			ng.Rules = append(ng.Rules, ng.newRule(qb, r))
		}

		err := g.updateWith(ng)
		if err != nil {
			t.Fatalf("cannot update rule: %s", err)
		}

		if len(g.Rules) != len(newRules) {
			t.Fatalf("expected to have %d rules; got: %d", len(g.Rules), len(newRules))
		}
		sort.Slice(g.Rules, func(i, j int) bool {
			return g.Rules[i].ID() < g.Rules[j].ID()
		})
		sort.Slice(ng.Rules, func(i, j int) bool {
			return ng.Rules[i].ID() < ng.Rules[j].ID()
		})
		for i, r := range g.Rules {
			got, want := r, ng.Rules[i]
			if got.ID() != want.ID() {
				t.Fatalf("expected to have rule %q; got %q", want, got)
			}
			if err := CompareRules(t, got, want); err != nil {
				t.Fatalf("comparison error: %s", err)
			}
		}
	}

	// new rule
	f(nil, []config.Rule{
		{Alert: "bar"},
	})

	// update alerting rule
	f([]config.Rule{
		{
			Alert: "foo",
			Expr:  "up > 0",
			For:   promutils.NewDuration(time.Second),
			Labels: map[string]string{
				"bar": "baz",
			},
			Annotations: map[string]string{
				"summary":     "{{ $value|humanize }}",
				"description": "{{$labels}}",
			},
		},
		{
			Alert: "bar",
			Expr:  "up > 0",
			For:   promutils.NewDuration(time.Second),
			Labels: map[string]string{
				"bar": "baz",
			},
		},
	}, []config.Rule{
		{
			Alert: "foo",
			Expr:  "up > 10",
			For:   promutils.NewDuration(time.Second),
			Labels: map[string]string{
				"baz": "bar",
			},
			Annotations: map[string]string{
				"summary": "none",
			},
		},
		{
			Alert:         "bar",
			Expr:          "up > 0",
			For:           promutils.NewDuration(2 * time.Second),
			KeepFiringFor: promutils.NewDuration(time.Minute),
			Labels: map[string]string{
				"bar": "baz",
			},
		},
	})

	// update recording rule
	f([]config.Rule{{
		Record: "foo",
		Expr:   "max(up)",
		Labels: map[string]string{
			"bar": "baz",
		},
	}}, []config.Rule{{
		Record: "foo",
		Expr:   "min(up)",
		Labels: map[string]string{
			"baz": "bar",
		},
	}})

	// empty rule
	f([]config.Rule{{Alert: "foo"}, {Record: "bar"}}, nil)

	// multiple rules
	f([]config.Rule{
		{Alert: "bar"},
		{Alert: "baz"},
		{Alert: "foo"},
	}, []config.Rule{
		{Alert: "baz"},
		{Record: "foo"},
	})

	// replace rule
	f([]config.Rule{{Alert: "foo1"}}, []config.Rule{{Alert: "foo2"}})

	// replace multiple rules
	f([]config.Rule{
		{Alert: "foo1"},
		{Record: "foo2"},
		{Alert: "foo3"},
	}, []config.Rule{
		{Alert: "foo3"},
		{Alert: "foo4"},
		{Record: "foo5"},
	})
}

func TestUpdateDuringRandSleep(t *testing.T) {
	// enable rand sleep to test group update during sleep
	SkipRandSleepOnGroupStart = false
	defer func() {
		SkipRandSleepOnGroupStart = true
	}()
	rule := AlertingRule{
		Name: "jobDown",
		Expr: "up==0",
		Labels: map[string]string{
			"foo": "bar",
		},
	}
	g := &Group{
		Name: "test",
		Rules: []Rule{
			&rule,
		},
		// big interval ensures big enough randSleep during start process
		Interval: 100 * time.Hour,
		updateCh: make(chan *Group),
	}
	go g.Start(context.Background(), nil, nil, nil)

	rule1 := AlertingRule{
		Name: "jobDown",
		Expr: "up{job=\"vmagent\"}==0",
		Labels: map[string]string{
			"foo": "bar",
		},
	}
	g1 := &Group{
		Rules: []Rule{
			&rule1,
		},
	}
	g.updateCh <- g1
	time.Sleep(10 * time.Millisecond)
	g.mu.RLock()
	if g.Rules[0].(*AlertingRule).Expr != "up{job=\"vmagent\"}==0" {
		t.Fatalf("expected to have updated rule expr")
	}
	g.mu.RUnlock()

	rule2 := AlertingRule{
		Name: "jobDown",
		Expr: "up{job=\"vmagent\"}==0",
		Labels: map[string]string{
			"foo": "bar",
			"baz": "qux",
		},
	}
	g2 := &Group{
		Rules: []Rule{
			&rule2,
		},
	}
	g.updateCh <- g2
	time.Sleep(10 * time.Millisecond)
	g.mu.RLock()
	if len(g.Rules[0].(*AlertingRule).Labels) != 2 {
		t.Fatalf("expected to have updated labels")
	}
	g.mu.RUnlock()

	g.Close()
}

func TestGroupStart(t *testing.T) {
	const (
		rules = `
  - name: groupTest
    rules:
      - alert: VMRows
        for: 1ms
        expr: vm_rows > 0
        labels:
          label: bar
          host: "{{ $labels.instance }}"
        annotations:
          summary: "{{ $value }}"
`
	)
	var groups []config.Group
	err := yaml.Unmarshal([]byte(rules), &groups)
	if err != nil {
		t.Fatalf("failed to parse rules: %s", err)
	}

	fs := &datasource.FakeQuerier{}
	fn := &notifier.FakeNotifier{}

	const evalInterval = time.Millisecond
	g := NewGroup(groups[0], fs, evalInterval, map[string]string{"cluster": "east-1"})

	const inst1, inst2, job = "foo", "bar", "baz"
	m1 := metricWithLabels(t, "instance", inst1, "job", job)
	m2 := metricWithLabels(t, "instance", inst2, "job", job)

	r := g.Rules[0].(*AlertingRule)
	alert1 := r.newAlert(m1, time.Now(), nil, nil)
	alert1.State = notifier.StateFiring
	// add annotations
	alert1.Annotations["summary"] = "1"
	// add external label
	alert1.Labels["cluster"] = "east-1"
	// add labels from response
	alert1.Labels["job"] = job
	alert1.Labels["instance"] = inst1
	// add rule labels
	alert1.Labels["label"] = "bar"
	alert1.Labels["host"] = inst1
	// add service labels
	alert1.Labels[alertNameLabel] = alert1.Name
	alert1.Labels[alertGroupNameLabel] = g.Name
	alert1.ID = hash(alert1.Labels)

	alert2 := r.newAlert(m2, time.Now(), nil, nil)
	alert2.State = notifier.StateFiring
	// add annotations
	alert2.Annotations["summary"] = "1"
	// add external label
	alert2.Labels["cluster"] = "east-1"
	// add labels from response
	alert2.Labels["job"] = job
	alert2.Labels["instance"] = inst2
	// add rule labels
	alert2.Labels["label"] = "bar"
	alert2.Labels["host"] = inst2
	// add service labels
	alert2.Labels[alertNameLabel] = alert2.Name
	alert2.Labels[alertGroupNameLabel] = g.Name
	alert2.ID = hash(alert2.Labels)

	finished := make(chan struct{})
	fs.Add(m1)
	fs.Add(m2)
	go func() {
		g.Start(context.Background(), func() []notifier.Notifier { return []notifier.Notifier{fn} }, nil, fs)
		close(finished)
	}()

	waitForIterations := func(n int, interval time.Duration) {
		t.Helper()

		var cur uint64
		prev := g.metrics.iterationTotal.Get()
		for i := 0; ; i++ {
			if i > 40 {
				t.Fatalf("group wasn't able to perform %d evaluations during %d eval intervals", n, i)
			}
			cur = g.metrics.iterationTotal.Get()
			if int(cur-prev) >= n {
				return
			}
			time.Sleep(interval)
		}
	}

	// wait for multiple evaluation iterations
	waitForIterations(4, evalInterval)

	gotAlerts := fn.GetAlerts()
	expectedAlerts := []notifier.Alert{*alert1, *alert2}
	compareAlerts(t, expectedAlerts, gotAlerts)

	gotAlertsNum := fn.GetCounter()
	if gotAlertsNum < len(expectedAlerts)*2 {
		t.Fatalf("expected to receive at least %d alerts; got %d instead",
			len(expectedAlerts)*2, gotAlertsNum)
	}

	// reset previous data
	fs.Reset()
	// and set only one datapoint for response
	fs.Add(m1)

	// wait for multiple evaluation iterations
	waitForIterations(4, evalInterval)

	gotAlerts = fn.GetAlerts()
	alert2.State = notifier.StateInactive
	expectedAlerts = []notifier.Alert{*alert1, *alert2}
	compareAlerts(t, expectedAlerts, gotAlerts)

	g.Close()
	<-finished
}

func TestGetResolveDuration(t *testing.T) {
	f := func(groupInterval, maxDuration, resendDelay, resultExpected time.Duration) {
		t.Helper()

		result := getResolveDuration(groupInterval, resendDelay, maxDuration)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %s; want %s", result, resultExpected)
		}
	}

	f(0, 0, 0, 0)
	f(time.Minute, 0, 0, 4*time.Minute)
	f(time.Minute, 0, 2*time.Minute, 8*time.Minute)
	f(time.Minute, 4*time.Minute, 4*time.Minute, 4*time.Minute)
	f(2*time.Minute, time.Minute, 2*time.Minute, time.Minute)
	f(time.Minute, 2*time.Minute, 1*time.Minute, 2*time.Minute)
	f(2*time.Minute, 0, 1*time.Minute, 8*time.Minute)
}

func TestFaultyNotifier(t *testing.T) {
	fq := &datasource.FakeQuerier{}
	fq.Add(metricWithValueAndLabels(t, 1, "__name__", "foo", "job", "bar"))

	r := newTestAlertingRule("instant", 0)
	r.q = fq

	fn := &notifier.FakeNotifier{}
	e := &executor{
		Notifiers: func() []notifier.Notifier {
			return []notifier.Notifier{
				&notifier.FaultyNotifier{},
				fn,
			}
		},
	}
	delay := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), delay)
	defer cancel()

	go func() {
		_ = e.exec(ctx, r, time.Now(), 0, 10)
	}()

	tn := time.Now()
	deadline := tn.Add(delay / 2)
	for {
		if fn.GetCounter() > 0 {
			return
		}
		if tn.After(deadline) {
			break
		}
		tn = time.Now()
		time.Sleep(time.Millisecond * 100)
	}
	t.Fatalf("alive notifier didn't receive notification by %v", deadline)
}

func TestFaultyRW(t *testing.T) {
	fq := &datasource.FakeQuerier{}
	fq.Add(metricWithValueAndLabels(t, 1, "__name__", "foo", "job", "bar"))

	r := &RecordingRule{
		Name:  "test",
		q:     fq,
		state: &ruleState{entries: make([]StateEntry, 10)},
	}

	e := &executor{
		Rw: &remotewrite.Client{},
	}

	err := e.exec(context.Background(), r, time.Now(), 0, 10)
	if err == nil {
		t.Fatalf("expected to get an error from faulty RW client, got nil instead")
	}
}

func TestCloseWithEvalInterruption(t *testing.T) {
	const (
		rules = `
  - name: groupTest
    rules:
      - alert: VMRows
        for: 1ms
        expr: vm_rows > 0
        labels:
          label: bar
          host: "{{ $labels.instance }}"
        annotations:
          summary: "{{ $value }}"
`
	)
	var groups []config.Group
	err := yaml.Unmarshal([]byte(rules), &groups)
	if err != nil {
		t.Fatalf("failed to parse rules: %s", err)
	}

	const delay = time.Second * 2
	fq := &datasource.FakeQuerierWithDelay{Delay: delay}

	const evalInterval = time.Millisecond
	g := NewGroup(groups[0], fq, evalInterval, nil)

	go g.Start(context.Background(), nil, nil, nil)

	time.Sleep(evalInterval * 20)

	go func() {
		g.Close()
	}()

	deadline := time.Tick(delay / 2)
	select {
	case <-deadline:
		t.Fatalf("deadline for close exceeded")
	case <-g.finishedCh:
	}
}

func TestGroupStartDelay(t *testing.T) {
	g := &Group{}
	// interval of 5min and key generate a static delay of 30s
	g.Interval = time.Minute * 5
	key := uint64(math.MaxUint64 / 10)

	f := func(atS, expS string) {
		t.Helper()
		at, err := time.Parse(time.RFC3339Nano, atS)
		if err != nil {
			t.Fatal(err)
		}
		expTS, err := time.Parse(time.RFC3339Nano, expS)
		if err != nil {
			t.Fatal(err)
		}
		delay := delayBeforeStart(at, key, g.Interval, g.EvalOffset)
		gotStart := at.Add(delay)
		if expTS != gotStart {
			t.Fatalf("expected to get %v; got %v instead", expTS, gotStart)
		}
	}

	// test group without offset
	f("2023-01-01T00:00:00.000+00:00", "2023-01-01T00:00:30.000+00:00")
	f("2023-01-01T00:00:00.999+00:00", "2023-01-01T00:00:30.000+00:00")
	f("2023-01-01T00:00:29.000+00:00", "2023-01-01T00:00:30.000+00:00")
	f("2023-01-01T00:00:31.000+00:00", "2023-01-01T00:05:30.000+00:00")

	// test group with offset smaller than above fixed randSleep,
	// this way randSleep will always be enough
	offset := 20 * time.Second
	g.EvalOffset = &offset

	f("2023-01-01T00:00:00.000+00:00", "2023-01-01T00:00:30.000+00:00")
	f("2023-01-01T00:00:29.000+00:00", "2023-01-01T00:00:30.000+00:00")
	f("2023-01-01T00:00:31.000+00:00", "2023-01-01T00:05:30.000+00:00")

	// test group with offset bigger than above fixed randSleep,
	// this way offset will be added to delay
	offset = 3 * time.Minute
	g.EvalOffset = &offset

	f("2023-01-01T00:00:00.000+00:00", "2023-01-01T00:03:30.000+00:00")
	f("2023-01-01T00:00:29.000+00:00", "2023-01-01T00:03:30.000+00:00")
	f("2023-01-01T00:01:00.000+00:00", "2023-01-01T00:08:30.000+00:00")
	f("2023-01-01T00:03:30.000+00:00", "2023-01-01T00:08:30.000+00:00")
	f("2023-01-01T00:07:30.000+00:00", "2023-01-01T00:13:30.000+00:00")

	offset = 10 * time.Minute
	g.EvalOffset = &offset
	// interval of 1h and key generate a static delay of 6m
	g.Interval = time.Hour

	f("2023-01-01T00:00:00.000+00:00", "2023-01-01T00:16:00.000+00:00")
	f("2023-01-01T00:05:00.000+00:00", "2023-01-01T00:16:00.000+00:00")
	f("2023-01-01T00:30:00.000+00:00", "2023-01-01T01:16:00.000+00:00")
}

func TestGetPrometheusReqTimestamp(t *testing.T) {
	f := func(g *Group, tsOrigin, tsExpected string) {
		t.Helper()

		originT, _ := time.Parse(time.RFC3339, tsOrigin)
		expT, _ := time.Parse(time.RFC3339, tsExpected)
		gotTS := g.adjustReqTimestamp(originT)
		if !gotTS.Equal(expT) {
			t.Fatalf("get wrong prometheus request timestamp: %s; want %s", gotTS, expT)
		}
	}

	offset := 30 * time.Minute
	evalDelay := 1 * time.Minute
	disableAlign := false

	// with query align + default evalDelay
	f(&Group{
		Interval: time.Hour,
	}, "2023-08-28T11:11:00+00:00", "2023-08-28T11:00:00+00:00")

	// without query align + default evalDelay
	f(&Group{
		Interval:      time.Hour,
		evalAlignment: &disableAlign,
	}, "2023-08-28T11:11:00+00:00", "2023-08-28T11:10:30+00:00")

	// with eval_offset, find previous offset point + default evalDelay
	f(&Group{
		EvalOffset: &offset,
		Interval:   time.Hour,
	}, "2023-08-28T11:11:00+00:00", "2023-08-28T10:30:00+00:00")

	// with eval_offset + default evalDelay
	f(&Group{
		EvalOffset: &offset,
		Interval:   time.Hour,
	}, "2023-08-28T11:41:00+00:00", "2023-08-28T11:30:00+00:00")

	// 1h interval with eval_delay
	f(&Group{
		EvalDelay: &evalDelay,
		Interval:  time.Hour,
	}, "2023-08-28T11:41:00+00:00", "2023-08-28T11:00:00+00:00")

	// 1m interval with eval_delay
	f(&Group{
		EvalDelay: &evalDelay,
		Interval:  time.Minute,
	}, "2023-08-28T11:41:13+00:00", "2023-08-28T11:40:00+00:00")

	// disable alignment with eval_delay
	f(&Group{
		EvalDelay:     &evalDelay,
		Interval:      time.Hour,
		evalAlignment: &disableAlign,
	}, "2023-08-28T11:41:00+00:00", "2023-08-28T11:40:00+00:00")
}

func TestRangeIterator(t *testing.T) {
	f := func(ri rangeIterator, resultExpected [][2]time.Time) {
		t.Helper()

		var j int
		for ri.next() {
			if len(resultExpected) < j+1 {
				t.Fatalf("unexpected result for iterator on step %d: %v - %v", j, ri.s, ri.e)
			}
			s, e := ri.s, ri.e
			expS, expE := resultExpected[j][0], resultExpected[j][1]
			if s != expS {
				t.Fatalf("expected to get start=%v; got %v", expS, s)
			}
			if e != expE {
				t.Fatalf("expected to get end=%v; got %v", expE, e)
			}
			j++
		}
	}

	f(rangeIterator{
		start: parseTime(t, "2021-01-01T12:00:00.000Z"),
		end:   parseTime(t, "2021-01-01T12:30:00.000Z"),
		step:  5 * time.Minute,
	}, [][2]time.Time{
		{parseTime(t, "2021-01-01T12:00:00.000Z"), parseTime(t, "2021-01-01T12:05:00.000Z")},
		{parseTime(t, "2021-01-01T12:05:00.000Z"), parseTime(t, "2021-01-01T12:10:00.000Z")},
		{parseTime(t, "2021-01-01T12:10:00.000Z"), parseTime(t, "2021-01-01T12:15:00.000Z")},
		{parseTime(t, "2021-01-01T12:15:00.000Z"), parseTime(t, "2021-01-01T12:20:00.000Z")},
		{parseTime(t, "2021-01-01T12:20:00.000Z"), parseTime(t, "2021-01-01T12:25:00.000Z")},
		{parseTime(t, "2021-01-01T12:25:00.000Z"), parseTime(t, "2021-01-01T12:30:00.000Z")},
	})

	f(rangeIterator{
		start: parseTime(t, "2021-01-01T12:00:00.000Z"),
		end:   parseTime(t, "2021-01-01T12:30:00.000Z"),
		step:  45 * time.Minute,
	}, [][2]time.Time{
		{parseTime(t, "2021-01-01T12:00:00.000Z"), parseTime(t, "2021-01-01T12:30:00.000Z")},
		{parseTime(t, "2021-01-01T12:30:00.000Z"), parseTime(t, "2021-01-01T12:30:00.000Z")},
	})

	f(rangeIterator{
		start: parseTime(t, "2021-01-01T12:00:12.000Z"),
		end:   parseTime(t, "2021-01-01T12:00:17.000Z"),
		step:  time.Second,
	}, [][2]time.Time{
		{parseTime(t, "2021-01-01T12:00:12.000Z"), parseTime(t, "2021-01-01T12:00:13.000Z")},
		{parseTime(t, "2021-01-01T12:00:13.000Z"), parseTime(t, "2021-01-01T12:00:14.000Z")},
		{parseTime(t, "2021-01-01T12:00:14.000Z"), parseTime(t, "2021-01-01T12:00:15.000Z")},
		{parseTime(t, "2021-01-01T12:00:15.000Z"), parseTime(t, "2021-01-01T12:00:16.000Z")},
		{parseTime(t, "2021-01-01T12:00:16.000Z"), parseTime(t, "2021-01-01T12:00:17.000Z")},
	})
}

func parseTime(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse("2006-01-02T15:04:05.000Z", s)
	if err != nil {
		t.Fatal(err)
	}
	return tt
}
