package engine

import (
	"context"
	"errors"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// discoverAll runs one discovery under the engine's retry policy and returns
// what that policy's attempts saw, merged per object. An error from Discover
// itself — an unreachable API, a refused list — drives the attempt loop exactly
// as any other plugin call does. A Found carrying a retryable problem is not a
// discovery-level error, but it may be worth another attempt: wanted answers
// "does this result need another pass?" for the caller — a run and a preflight
// retry for any retryable object, a restore only for the target it came to
// resolve — and while the budget lasts a synthetic transient error drives the
// same loop.
//
// The merge is the point of the loop: the last pass decides which objects
// exist, and an object it reports as a *retryable* stumble keeps the healthy
// verdict an earlier pass reached about it. One slow answer must not turn an
// object that rendered minutes ago into a failed artifact — an API hiccup at the
// fourth pass would otherwise cost the night a workload that was never broken,
// and every such failure is what the operator is told to chase. A permanent
// problem in the last pass stands: that is a fact about the object, not a
// stumble, and so does a healthy last pass over an earlier problem.
//
// A context that ends during the retry wait is the answer itself, never the
// stale result of the pass before it. One attempt per plugin call stays true:
// one Discover call per pass, and the waits, the count and the classification
// are the config's own retry policy, not a second one.
func (e *Engine) discoverAll(ctx context.Context, p retryPolicy, name string, inst discovery.Discoverer, wanted func([]discovery.Found) string) ([]discovery.Found, error) {
	var last []discovery.Found
	// healthy is the newest verdict per object that carries no problem at all:
	// what a later retryable stumble is not allowed to overwrite.
	healthy := map[string]discovery.Found{}
	_, err := attempt(ctx, p, "discover "+name, func(ctx context.Context, _ int) (struct{}, error) {
		found, err := safeValue(func() ([]discovery.Found, error) { return inst.Discover(ctx) })
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
			return struct{}{}, plugin.Transient(errors.New(problem))
		}
		return struct{}{}, nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if retryableProblem(last) == "" {
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

// retryableProblem is wanted for a run and a preflight: any object asking for
// another attempt makes the whole result worth re-running.
func retryableProblem(found []discovery.Found) string {
	for _, f := range found {
		if f.ProblemRetryable {
			return f.Problem
		}
	}
	return ""
}

// targetProblem is wanted for a restore: only the artifact it came to resolve
// makes another pass worth its wait; a retryable problem on any other object
// leaves the target as this pass found it. An instance whose domain holds
// several artifacts is named by its envelope, so a retryable problem on it is
// about the target too — its units can be listed only once it is readable.
func targetProblem(artifact string) func([]discovery.Found) string {
	return func(found []discovery.Found) string {
		for _, f := range found {
			if targetUnder(f.Artifact.Name, artifact) && f.ProblemRetryable {
				return f.Problem
			}
		}
		return ""
	}
}

// targetUnder reports whether an instance's artifact envelope could hold the
// artifact the caller names: the name itself, or a unit of it — the engine
// builds a unit's artifact name as "<envelope>/<unit>".
func targetUnder(envelope, artifact string) bool {
	return envelope == artifact || strings.HasPrefix(artifact, envelope+"/")
}
