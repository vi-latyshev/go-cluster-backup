// Package discovery is the seam that finds sources at run time — labelled pods,
// PVCs, anything a plugin can enumerate. The interface obeys the seam rules
// documented in package instances.
package discovery

import (
	"context"
	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Found is one discovered source, ready to be built: the plugin has already
// rendered every template of the matched rule against the discovered object.
type Found struct {
	// Name is the instance name of the source to build; unique within a run.
	Name string
	// Type is the source plugin type.
	Type string
	// Config is the complete source config, rendered.
	Config jsontext.Value
	// Artifact is the artifact envelope of the rule (group, name, importance,
	// sensitivity); Ext is filled later by the source itself.
	Artifact model.Artifact
	// Pipeline is the pipeline of the run config this source flows through: the
	// processor instance names, in encode order. The run resolves them against
	// its own processors, and a name it does not have fails this one object.
	Pipeline []string
	// Problem, when non-empty, says the object matched a rule but its source
	// could not be rendered or resolved: Name and the artifact envelope are
	// still set — the plugin falls back to an identity derived from the rule,
	// the namespace and the object, and says so here — while Type and Config
	// may be empty. The run records one failed artifact for it with the
	// envelope's importance and this problem as its error, and processes every
	// other Found normally: one broken workload must not cost the night.
	Problem string
	// Hint is the short, actionable half of Problem: the one thing an operator
	// has to do, written for a notification rather than for a log — "add
	// <label> or <marker>" rather than the whole reasoning. It is empty for a
	// problem that has no single action, and the run records it beside the
	// artifact so a message template can show it without parsing Problem.
	Hint string
	// Excluded, when non-empty, says the object was seen and deliberately
	// produces no artifact of its own: model.ExclusionIgnored (the object
	// carries the exclusion marker the rule's config names) or
	// model.ExclusionCovered (another artifact already carries its data). Name
	// and the artifact envelope are set, ExcludedReason is non-empty, and
	// Problem is empty; Type, Config and Pipeline may be empty, because no
	// source is built for it. The run records one model.Exclusion for it and
	// processes every other Found normally — a deliberate exclusion must not
	// cost the night, and an object nobody accounted for is a problem, never a
	// silent absence.
	Excluded model.ExclusionKind
	// ExcludedGroup is the artifact group the exclusion record carries: the
	// group of the rule that would have produced the object, and empty when no
	// rule would have — a claim the accounting decided, or one outside every
	// volume rule's phase, is no artifact the run failed to make. The object's
	// own Artifact.Group names the rule that reported the decision, which is
	// not necessarily the rule that would have carried its data.
	ExcludedGroup string
	// ExcludedReason is why, in the terms the object and the rule supply: the
	// text an operator reads in the manifest and in the nightly message. It is
	// required beside Excluded and meaningless without it.
	ExcludedReason string
	// ProblemRetryable, when true, says the problem is a transient fault — a
	// 5xx, a timeout — another attempt may not answer the same way. The
	// planning layer re-runs Discover while the retry budget lasts, and when
	// the budget is spent the last attempt's result stands: the Found becomes
	// its one failed
	// artifact exactly as any other Problem does. A false value — the only
	// value for a permanent, not_found or unclassified problem — means the
	// problem is final; the field is meaningful only beside a non-empty Problem.
	ProblemRetryable bool
}

// Discoverer returns every source it can see. A cancelled context is an error,
// never a nil slice: "found nothing" and "was told to stop" must not look alike.
type Discoverer interface {
	Discover(ctx context.Context) ([]Found, error)
}

var Kind = instances.NewKind[Discoverer]("discovery")
