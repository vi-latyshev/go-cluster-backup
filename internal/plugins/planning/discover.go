package planning

import (
	"context"
	"errors"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
)

// Wanted answers "does this result need another pass?" for one discovery retry
// loop: the problem that makes another attempt worth its wait, or "" when the
// result stands.
type Wanted func([]discovery.Found) string

// AnyRetryable is Wanted for a run and a preflight: any object asking for
// another attempt makes the whole result worth re-running.
func AnyRetryable(found []discovery.Found) string {
	for _, f := range found {
		if f.ProblemRetryable {
			return f.Problem
		}
	}
	return ""
}

// TargetUnder is Wanted for a restore: only the artifact it came to resolve
// makes another pass worth its wait; a retryable problem on any other object
// leaves the target as this pass found it.
func TargetUnder(artifact string) Wanted {
	return func(found []discovery.Found) string {
		for _, f := range found {
			if under(f.Artifact.Name, artifact) && f.ProblemRetryable {
				return f.Problem
			}
		}
		return ""
	}
}

// Discover runs one discovery under the planner's retry policy and returns what
// its attempts saw, merged per object. An error from Discover itself — an
// unreachable API, a refused list — drives the attempt loop exactly as any other
// plugin call does. A Found carrying a retryable problem is not a
// discovery-level error, but it may be worth another attempt: wanted answers
// whether this result needs another pass, and while the budget lasts a
// synthetic transient error drives the same loop.
//
// The merge is the point of the loop: the last pass decides which objects
// exist, and an object it reports as a *retryable* stumble keeps the healthy
// verdict an earlier pass reached about it. One slow answer must not turn an
// object that rendered minutes ago into a failed artifact. A permanent problem
// in the last pass stands: that is a fact about the object, not a stumble, and
// so does a healthy last pass over an earlier problem.
//
// A context that ends during the retry wait is the answer itself, never the
// stale result of the pass before it. One attempt per plugin call stays true:
// one Discover call per pass, and the waits, the count and the classification
// are the policy's own, not a second one.
func (p Planner) Discover(ctx context.Context, name string, inst discovery.Discoverer, wanted Wanted) ([]discovery.Found, error) {
	var last []discovery.Found
	// healthy is the newest verdict per object that carries no problem at all:
	// what a later retryable stumble is not allowed to overwrite.
	healthy := map[string]discovery.Found{}
	_, err := execution.Attempt(ctx, p.policy, "discover "+name, func(ctx context.Context, _ int) (struct{}, error) {
		found, err := execution.SafeValue(func() ([]discovery.Found, error) { return inst.Discover(ctx) })
		if err != nil {
			// A discovery-level failure: nothing from an earlier attempt may
			// stand for it.
			last, healthy = nil, map[string]discovery.Found{}
			return struct{}{}, err
		}
		last = found
		for _, f := range found {
			if f.Problem == "" {
				healthy[f.Name] = f
			}
		}
		if problem := wanted(found); problem != "" {
			return struct{}{}, pluginerrors.Transient(errors.New(problem))
		}
		return struct{}{}, nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if AnyRetryable(last) == "" {
			return nil, err
		}
	}
	return keepHealthy(last, healthy), nil
}

// keepHealthy replaces every Found whose last verdict is a retryable stumble by
// the healthy verdict an earlier pass reached about the same object. The order
// and the object set stay the last pass's: an object that disappeared between
// passes is gone, and one that appeared only in the last pass has no earlier
// verdict to keep.
func keepHealthy(last []discovery.Found, healthy map[string]discovery.Found) []discovery.Found {
	if len(healthy) == 0 {
		return last
	}
	merged := make([]discovery.Found, 0, len(last))
	for _, f := range last {
		if f.ProblemRetryable {
			if earlier, ok := healthy[f.Name]; ok {
				merged = append(merged, earlier)
				continue
			}
		}
		merged = append(merged, f)
	}
	return merged
}

// under reports whether an instance's artifact envelope could hold the artifact
// the caller names: the name itself, or a unit of it — a unit's artifact name
// is "<envelope>/<unit>".
func under(envelope, artifact string) bool {
	return envelope == artifact || strings.HasPrefix(artifact, envelope+"/")
}
