package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// scriptedDiscovery answers one scripted result per pass, the last pass
// repeating: what makes a test able to decide what each attempt saw.
type scriptedDiscovery struct {
	passes [][]discovery.Found
	mu     sync.Mutex
	calls  int
}

func (d *scriptedDiscovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	i := min(d.calls, len(d.passes)-1)
	d.calls++
	return d.passes[i], nil
}

// plainFound is one object of a scripted pass, healthy unless a problem says
// otherwise.
func plainFound(name, artifact string) discovery.Found {
	return discovery.Found{
		Name:     name,
		Type:     "postgres",
		Artifact: model.Artifact{Group: "databases", Name: artifact, Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
}

func stumbled(f discovery.Found, retryable bool) discovery.Found {
	f.Problem = "secret ns/cred: HTTP 500"
	f.ProblemRetryable = retryable
	return f
}

// TestTheLastPassDecidesTheSetAndTheMergeDecidesTheVerdict is the contract of a
// discovery that took several passes: the last pass says which objects exist,
// and an object it reports as a *retryable* stumble keeps the healthy verdict an
// earlier pass reached — one slow answer must not turn a workload that rendered
// minutes ago into a failed artifact. A permanent problem in the last pass
// stands: that is a fact about the object, not a stumble.
func TestTheLastPassDecidesTheSetAndTheMergeDecidesTheVerdict(t *testing.T) {
	a, b := plainFound("workloads/a", "ns/a"), plainFound("workloads/b", "ns/b")
	cases := []struct {
		name   string
		passes [][]discovery.Found
		want   map[string]string // instance name -> problem, "" for healthy
	}{
		{
			name: "a healthy object survives a later retryable stumble",
			passes: [][]discovery.Found{
				{a, stumbled(b, true)},
				{stumbled(a, true), b},
			},
			want: map[string]string{"workloads/a": "", "workloads/b": ""},
		},
		{
			name: "a permanent problem in the last pass stands",
			passes: [][]discovery.Found{
				{a, stumbled(b, true)},
				{stumbled(a, false), b},
			},
			want: map[string]string{"workloads/a": "secret ns/cred: HTTP 500", "workloads/b": ""},
		},
		{
			name: "an object the last pass lost is gone",
			passes: [][]discovery.Found{
				{a, stumbled(b, true)},
				{b},
			},
			want: map[string]string{"workloads/b": ""},
		},
		{
			name: "a retryable stumble nobody answered stands",
			passes: [][]discovery.Found{
				{a, stumbled(b, true)},
			},
			want: map[string]string{"workloads/a": "", "workloads/b": "secret ns/cred: HTTP 500"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := retryPolicy{attempts: 2, initial: time.Millisecond, max: time.Millisecond}
			got, err := (&Engine{}).discoverAll(context.Background(), p, "workloads", &scriptedDiscovery{passes: tc.passes}, retryableProblem)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("found %+v, want %d objects", got, len(tc.want))
			}
			for _, f := range got {
				want, ok := tc.want[f.Name]
				if !ok {
					t.Fatalf("found %q, which no pass should have produced: %+v", f.Name, got)
				}
				if f.Problem != want {
					t.Fatalf("%s: problem = %q, want %q", f.Name, f.Problem, want)
				}
			}
		})
	}
}

// retryFinding is a discoverer whose one Found asks for another attempt.
type retryFinding struct{ found []discovery.Found }

func (r retryFinding) Discover(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.found, nil
}

// TestDiscoverAllReturnsCancellationDuringRetryWait: a context that ends while
// a retryable problem waits for its next pass is the answer itself — the stale
// result of the pass before it is never handed back with a nil error.
func TestDiscoverAllReturnsCancellationDuringRetryWait(t *testing.T) {
	found := []discovery.Found{{
		Name:     "workloads/postgres",
		Type:     "postgres",
		Artifact: model.Artifact{Group: "databases", Name: "ns/postgres", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
		Problem:  "secret ns/cred: HTTP 500", ProblemRetryable: true,
	}}
	// The wait is long enough that the cancellation can only land inside it.
	p := retryPolicy{attempts: 3, initial: time.Hour, max: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	got, err := (&Engine{}).discoverAll(ctx, p, "workloads", retryFinding{found: found}, retryableProblem)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got != nil {
		t.Fatalf("found = %+v, want nil: the stale pass must not come back", got)
	}
}
