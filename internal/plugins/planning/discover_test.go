package planning

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// scriptedDiscovery answers one scripted result per pass, the last pass
// repeating: what makes a test able to decide what each attempt saw.
type scriptedDiscovery struct {
	passes [][]discovery.Found
	errs   []error
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
	if i < len(d.errs) && d.errs[i] != nil {
		return nil, d.errs[i]
	}
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

func testPlanner(passes int) Planner {
	return New(execution.Options{Attempts: passes, Initial: 0, Max: 0})
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
			name: "every object stumbled, so the last pass stands as it is",
			passes: [][]discovery.Found{
				{stumbled(a, true), stumbled(b, true)},
			},
			want: map[string]string{"workloads/a": "secret ns/cred: HTTP 500", "workloads/b": "secret ns/cred: HTTP 500"},
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
			got, err := testPlanner(2).Discover(context.Background(), "workloads", &scriptedDiscovery{passes: tc.passes}, AnyRetryable)
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

// TestDiscoverReturnsCancellationDuringRetryWait: a context that ends while a
// retryable problem waits for its next pass is the answer itself — the stale
// result of the pass before it is never handed back with a nil error.
func TestDiscoverReturnsCancellationDuringRetryWait(t *testing.T) {
	found := []discovery.Found{stumbled(plainFound("workloads/postgres", "ns/postgres"), true)}
	// The wait is long enough that the cancellation can only land inside it.
	p := New(execution.Options{Attempts: 3, Initial: time.Hour, Max: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 1)
	go func() {
		<-calls
		cancel()
	}()
	d := &signallingDiscovery{found: found, once: calls}

	got, err := p.Discover(ctx, "workloads", d, AnyRetryable)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got != nil {
		t.Fatalf("found = %+v, want nil: the stale pass must not come back", got)
	}
}

// signallingDiscovery reports its first call and then blocks until the context
// ends: the retry wait is where the cancellation lands.
type signallingDiscovery struct {
	found []discovery.Found
	once  chan<- struct{}
	calls sync.Once
}

func (d *signallingDiscovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.calls.Do(func() { d.once <- struct{}{} })
	return d.found, nil
}

// TestDiscoverReturnsADiscoveryLevelFailure: an error from Discover with no
// retryable object in the last pass is the caller's answer, and no earlier
// pass's objects stand for it.
func TestDiscoverReturnsADiscoveryLevelFailure(t *testing.T) {
	p := testPlanner(1)
	got, err := p.Discover(context.Background(), "workloads", &scriptedDiscovery{
		passes: [][]discovery.Found{{plainFound("workloads/a", "ns/a")}},
		errs:   []error{errors.New("the API is unreachable")},
	}, AnyRetryable)
	if err == nil || !strings.Contains(err.Error(), "the API is unreachable") {
		t.Fatalf("err = %v, want the discovery failure", err)
	}
	if got != nil {
		t.Fatalf("found = %+v, want nil", got)
	}
}

// TestDiscoverRecoversAPanic: a crashing discovery fails its step permanently,
// never the process.
func TestDiscoverRecoversAPanic(t *testing.T) {
	_, err := testPlanner(1).Discover(context.Background(), "workloads", panicDiscovery{}, AnyRetryable)
	if err == nil || !strings.Contains(err.Error(), "plugin panic") {
		t.Fatalf("err = %v, want a recovered panic", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("panic class = %q, want permanent", class)
	}
}

type panicDiscovery struct{}

func (panicDiscovery) Discover(context.Context) ([]discovery.Found, error) { panic("boom") }

// TestTargetUnderNamesOnlyTheTarget: a restore retries for the artifact it came
// to resolve — the name itself or a unit under its envelope — and for nothing
// else.
func TestTargetUnderNamesOnlyTheTarget(t *testing.T) {
	cases := []struct {
		name     string
		found    discovery.Found
		artifact string
		want     string
	}{
		{"the envelope itself", stumbled(plainFound("a", "ns/db"), true), "ns/db", "secret ns/cred: HTTP 500"},
		{"a unit under the envelope", stumbled(plainFound("a", "ns/db"), true), "ns/db/unit", "secret ns/cred: HTTP 500"},
		{"another object", stumbled(plainFound("a", "ns/db"), true), "ns/other", ""},
		{"a healthy target", plainFound("a", "ns/db"), "ns/db", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TargetUnder(tc.artifact)([]discovery.Found{tc.found}); got != tc.want {
				t.Fatalf("wanted = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAnyRetryableReturnsTheFirstProblem: a run and a preflight retry for any
// object that asks for another pass.
func TestAnyRetryableReturnsTheFirstProblem(t *testing.T) {
	if got := AnyRetryable(nil); got != "" {
		t.Fatalf("empty result wanted %q", got)
	}
	first := stumbled(plainFound("a", "ns/a"), true)
	second := stumbled(plainFound("b", "ns/b"), true)
	if got := AnyRetryable([]discovery.Found{first, second}); got != first.Problem {
		t.Fatalf("wanted = %q, want the first problem %q", got, first.Problem)
	}
}
