// Package kuberneteslabels is the discovery plugin over labelled cluster
// objects: two lists of rules — pods first, then persistent volume claims — each
// rule a list query plus the templates that turn one matched object into one
// Found.
//
// Coverage is opt-out, and every claim is accounted for exactly once. A claim is
// either covered — a pod rule with covers_volumes discovered the pod that mounts
// it, and that pod's dump carries the volume's data; or declared — a volume rule
// matches it and renders a file-tree source for it; or ignored — its own labels
// carry the exclusion marker, which is a visible decision; or it is reported:
// one failed artifact naming the claim, under the undeclared_volumes setting.
// That accounting always runs, whatever the lists hold. A claim in Pending has
// no volume yet and is neither (WaitForFirstConsumer provisions it when a pod
// first runs); a claim in Lost has lost its binding and is reported wherever it
// is seen.
//
// The plugin knows no label, namespace, project or host name: every selector,
// phase, artifact envelope, pipeline and source config stands in the config
// file. Only the two resource names it can list (pods and
// persistentvolumeclaims) are code.
//
// Templates render against the matched object: {{ .Namespace }}, {{ .Name }},
// {{ .Labels }}, {{ .Phase }}, plus the rule functions documented on
// TemplateFuncs. The functions that read a pod spec take explicit indices, so
// the convention a rule relies on is visible in its own config. A missing
// label, container, variable or port, an unsupported environment form or an
// unreadable Secret is an error naming the object — never a silent skip; Check
// resolves every rule at preflight, so such a config fails before a run starts.
package kuberneteslabels

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"text/template"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// resourcePhases is every status.phase the two resources a rule may list can
// carry: what a rule's phase must be one of, since a rule naming any other
// value lists objects that are never there.
var resourcePhases = map[string][]string{
	"pods":                   {"Pending", "Running", "Succeeded", "Failed", "Unknown"},
	"persistentvolumeclaims": {"Pending", "Bound", "Lost"},
}

// Config is the instance config of one discovery. Every field is required; the
// five connection fields are spelled out here so the descriptor and the
// missing-field report cover them.
type Config struct {
	APIHostEnv     string         `json:"api_host_env" doc:"name of the environment variable holding the API server host"`
	APIPortEnv     string         `json:"api_port_env" doc:"name of the environment variable holding the API server port"`
	TokenFile      string         `json:"token_file" doc:"path of the ServiceAccount token file, re-read on every request"`
	CAFile         string         `json:"ca_file" doc:"path of the CA bundle the API server certificate is verified against"`
	RequestTimeout model.Duration `json:"request_timeout" doc:"timeout of one API request"`
	// The key is secrets_path, not secrets_dir: the framework refuses a field
	// whose name mentions a secret unless it ends in _file or _path, a
	// location rather than a value (plugin/plugintest.secretNamed).
	SecretsDir   string `json:"secrets_path" required:"true" doc:"absolute path of a directory (a tmpfs) the envFile function writes resolved secrets into, mode 0600"`
	ListPageSize int    `json:"list_page_size" doc:"objects one API list page may carry; the client follows the continue token to every following page"`
	// Exclude is the marker a workload's own manifest carries to keep its data
	// out of the backup. Both fields are required and both live here, never in
	// code: the plugin knows no label name.
	Exclude ExcludeConfig `json:"exclude" required:"true" doc:"label that takes an object out of the backup; the object's own manifest carries it"`
	// Two lists, one per resource, and the order inside each fixes the order of
	// the discovered sources: pods first, then claims. A claim is accounted for
	// exactly once — covered by a pod rule whose source reads its volume,
	// declared by a volume rule, excluded by the marker, or reported as
	// undeclared_volumes — and the accounting of what is left always runs.
	Pods    []PodRule    `json:"pods" required:"true" doc:"rules over pods, in the order the discovered sources take"`
	Volumes []VolumeRule `json:"volumes" required:"true" doc:"rules over persistent volume claims, in the order the discovered sources take"`
	// UndeclaredVolumes names the one failed artifact every claim that no rule
	// claimed and no marker excluded is reported as.
	UndeclaredVolumes UndeclaredVolumes `json:"undeclared_volumes" doc:"what an object no rule accounted for is reported as"`

	state *state
}

// UndeclaredVolumes is the one setting behind the accounting: every claim that
// is neither covered by a pod's dump, nor declared as a file volume, nor marked
// is ONE failed artifact naming it — never a silent absence and never a green
// copy of a live database directory. The artifact is reported under Name, which
// is also the instance prefix of the failed artifact, and its importance is the
// deployment's decision: a volume that appeared with nobody declaring it is the
// deployment's to answer for.
type UndeclaredVolumes struct {
	Name       string           `json:"name" doc:"instance prefix and artifact group the failed artifacts of undeclared objects are reported under"`
	Importance model.Importance `json:"importance" required:"true" doc:"required or best_effort: how loudly an undeclared object is reported"`
}

// PodRule is one rule over pods: a list query plus the templates that turn one
// matched object into one Found. A pod rule's source reads the data of the
// containers it names, so the claims the pod mounts are covered by its artifact
// when CoversVolumes says so.
type PodRule struct {
	Name     string         `json:"name" required:"true" doc:"rule name; it prefixes the instance name of every source the rule finds"`
	Selector string         `json:"selector" required:"true" doc:"label selector of the list query; an empty selector matches every pod"`
	Phase    string         `json:"phase" doc:"required status.phase; a pod in any other phase is not discovered (Running for the usual case)"`
	Artifact ArtifactConfig `json:"artifact" doc:"artifact envelope every source of this rule produces"`
	Pipeline []string       `json:"pipeline" required:"true" doc:"processor instances the discovered sources flow through, in encode order"`
	Source   SourceConfig   `json:"source" required:"true" doc:"source plugin type and the config every discovered source is built from"`
	// CoversVolumes is true for a pod rule whose source reads the data of the
	// volumes its pod mounts: every claim that pod mounts is then covered by
	// the artifact instead of being copied as a tree of files of its own.
	CoversVolumes bool `json:"covers_volumes" required:"true" doc:"true when this rule's source reads the data of the volumes its pod mounts, so those claims are covered by its artifact"`
	// MinMatches is the least number of matches the rule must produce — 0 is
	// the explicit way to say "an empty result is fine". An API that answers
	// with fewer matches than this fails the run at preflight and at
	// discovery: an empty list is not evidence of nothing to back up, and a
	// silent empty backup is worse than a failed one.
	MinMatches int `json:"min_matches" doc:"least number of matches this rule must produce; 0 allows an empty result. Fewer than this fails the run"`
}

// VolumeRule is one rule over persistent volume claims: the claim declares
// itself as a plain file volume and the source reads the volume the claim is
// bound to. A claim rule never covers another claim's data — it produces the
// artifact for its own object — so it carries no CoversVolumes.
type VolumeRule struct {
	Name       string         `json:"name" required:"true" doc:"rule name; it prefixes the instance name of every source the rule finds"`
	Selector   string         `json:"selector" required:"true" doc:"label selector of the list query; an empty selector matches every claim"`
	Phase      string         `json:"phase" doc:"required status.phase; a claim in any other phase is not discovered (Bound for the usual case)"`
	Artifact   ArtifactConfig `json:"artifact" doc:"artifact envelope every source of this rule produces"`
	Pipeline   []string       `json:"pipeline" required:"true" doc:"processor instances the discovered sources flow through, in encode order"`
	Source     SourceConfig   `json:"source" required:"true" doc:"source plugin type and the config every discovered source is built from"`
	MinMatches int            `json:"min_matches" doc:"least number of matches this rule must produce; 0 allows an empty result. Fewer than this fails the run"`
}

// ExcludeConfig is the exclusion marker: an object whose label Label holds
// Value produces no artifact and is recorded as a decision.
type ExcludeConfig struct {
	Label string `json:"label" required:"true" doc:"name of the label carrying the exclusion marker"`
	Value string `json:"value" required:"true" doc:"label value that means the object is not backed up"`
}

// matches reports whether a set of labels carries the marker.
func (e ExcludeConfig) matches(labels map[string]string) bool {
	return labels[e.Label] == e.Value
}

// marker is the label and the value as an operator writes and reads them:
// "backup.example/ignore=true".
func (e ExcludeConfig) marker() string { return e.Label + "=" + e.Value }

// reason is the sentence an operator reads beside an excluded object.
func (e ExcludeConfig) reason() string {
	return "carries the exclusion marker " + e.marker()
}

// rule is one prepared rule of either list, in the shape the rest of this
// package works with: the two config lists differ only in their resource and in
// whether they may cover volumes, and the accounting pass is one more entry with
// undeclared true and no listing of its own beyond every claim.
type rule struct {
	Name     string
	Resource string // "pods" or "persistentvolumeclaims": which list it came from
	Path     string // its path in the config, e.g. "/pods/0" — what a rule source is reported under
	Selector string
	Phase    string
	Artifact ArtifactConfig
	Pipeline []string
	Source   SourceConfig
	// CoversVolumes is true for a pod rule whose source reads the data of the
	// volumes its pod mounts.
	CoversVolumes bool
	// Undeclared is true for the one synthetic entry that accounts for what no
	// other rule claimed.
	Undeclared bool
	MinMatches int
}

type ArtifactConfig struct {
	Group       plugin.Template   `json:"group" required:"true" doc:"artifact group template"`
	Name        plugin.Template   `json:"name" required:"true" doc:"artifact name template; unique within a run"`
	Importance  model.Importance  `json:"importance" doc:"required or best_effort"`
	Sensitivity model.Sensitivity `json:"sensitivity" required:"true" doc:"plain or secret; secret requires a pipeline that seals"`
}

// SourceConfig carries the source config as raw JSON: its strings are the
// templates, its shape is the source plugin's. The plugin does not decode it —
// it renders it and hands the bytes to the registry, which builds the source.
type SourceConfig struct {
	Type   string         `json:"type" required:"true" doc:"source plugin type"`
	Config jsontext.Value `json:"config" doc:"complete source config; every string value in it is a template rendered per matched object"`
}

// Register adds the kubernetes_labels discovery to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, discovery.Kind, plugin.Spec[Config, discovery.Discoverer]{
		Type:      "kubernetes_labels",
		Summary:   "labelled pods and persistent volume claims listed from the Kubernetes API",
		Stability: plugin.Stable,
		Example:   []byte(example),
		New:       New,
	})
}

// New builds the discoverer. It touches neither the environment nor the API:
// those are read at preflight, so an instance can be constructed (and its
// config validated) without a cluster.
func New(ctx context.Context, cfg Config, deps plugin.Deps) (discovery.Discoverer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	log := deps.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	st := cfg.stateOrNew()
	st.secretsDir = cfg.SecretsDir
	st.pageSize = cfg.ListPageSize
	st.exclude = cfg.Exclude
	st.opts = kubeapi.Options{
		HostEnv:   cfg.APIHostEnv,
		PortEnv:   cfg.APIPortEnv,
		TokenFile: cfg.TokenFile,
		CAFile:    cfg.CAFile,
		Timeout:   cfg.RequestTimeout.Std(),
	}
	st.log = log.With("kind", "discovery", "type", "kubernetes_labels", "instance", deps.Instance)
	return &Discoverer{state: st}, nil
}

type Discoverer struct {
	state *state
}

// Check resolves every rule against the live cluster, so a discovery-LEVEL
// problem stops the run at preflight: an unreachable or unauthenticating API, a
// list call the server refuses, fewer matches than a rule's min_matches (a rule
// that matches nothing is not an error when its min_matches is 0). A problem
// with one object's own source never fails this check — it travels as a
// Found.Problem (render states when it is worth retrying), because a run skips
// a discovery whose Check failed and one broken object must not cost the rest.
func (d *Discoverer) Check(ctx context.Context) error {
	_, err := d.collect(ctx)
	return err
}

// Discover returns every source the rules match, rules in config order and, in
// each rule, objects sorted by namespace and name: the same cluster state
// yields the same []Found. An object whose source could not be rendered comes
// back as one Found carrying Problem instead of failing the whole discovery, so
// one broken workload costs one artifact, not the night. A cancelled context is
// an error, never an empty result.
func (d *Discoverer) Discover(ctx context.Context) ([]discovery.Found, error) {
	return d.collect(ctx)
}

// RuleSources exposes every rule's source — its type, the nested config exactly
// as written and the routing — so offline validation can check what the strict
// decoder sees only as opaque bytes. Templates stay unrendered: this touches no
// cluster and evaluates nothing. The rule with undeclared true renders no source,
// so its entry carries an empty type: what still belongs to a validation is its
// routing.
func (d *Discoverer) RuleSources(ctx context.Context) ([]discovery.RuleSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st := d.state
	out := make([]discovery.RuleSource, 0, len(st.rules))
	for i := range st.rules {
		r := st.rules[i].rule
		rs := discovery.RuleSource{
			Path:        r.Path,
			Type:        r.Source.Type,
			Config:      r.Source.Config,
			Pipeline:    r.Pipeline,
			Importance:  r.Artifact.Importance,
			Sensitivity: r.Artifact.Sensitivity,
		}
		if r.Source.Type != "" {
			rs.ConfigPath = r.Path + "/source/config"
		}
		out = append(out, rs)
	}
	return out, nil
}

// collect finds every source in the order the contract promises — rules in
// config order, objects by namespace and name within a rule — and decides about
// every claim exactly once.
//
// It runs in three steps. First every rule lists its own objects, so the count
// min_matches compares against is that rule's raw answer: whether another rule
// claimed an object cannot change what this one's labels prove. Then the pod
// rules render, and the claims their pods mount are marked covered or ignored —
// built from every pod rule before any claim is decided, so the meaning of a
// claim never depends on where its own rule stands in the config. Then the claim
// rules classify what is left: the marker, a lost binding, a pod's verdict, the
// undeclared accounting, or the render the rule declares.
func (d *Discoverer) collect(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st := d.state
	if err := st.clientFor(); err != nil {
		return nil, err
	}
	client := st.client
	st.ctx = ctx
	st.forgetVolumes()

	rules := st.rules
	listings := make([][]object, len(rules))
	for i := range rules {
		objects, err := d.objects(ctx, client, rules[i].rule)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", rules[i].rule.Name, err)
		}
		if len(objects) < rules[i].rule.MinMatches {
			// The API answered: the count is a fact, not a fault. Retrying
			// the same query returns the same answer, so this is permanent —
			// and it stays distinguishable from a transient API failure,
			// which surfaces as the error above this check, never as this
			// message. Objects that matched but carry a Problem count here:
			// the count is about what the cluster labels, and a broken member
			// still proves the label exists.
			return nil, plugin.Permanent(fmt.Errorf("rule %q: %s with selector %q matched %d objects, want at least %d",
				rules[i].rule.Name, rules[i].rule.Resource, rules[i].rule.Selector, len(objects), rules[i].rule.MinMatches))
		}
		listings[i] = objects
	}

	byRule := make([][]discovery.Found, len(rules))
	acct := &accounting{marks: map[string]claimMark{}, mounting: map[string]string{}}
	for i := range rules {
		pr := &rules[i]
		if pr.rule.Resource != "pods" {
			continue
		}
		for _, o := range listings[i] {
			acct.rememberMounts(o)
			f := d.podFound(pr, o)
			byRule[i] = append(byRule[i], f)
			if !pr.rule.CoversVolumes {
				continue
			}
			mark, ok := podMark(pr, o, f, st.exclude.reason())
			if !ok {
				continue
			}
			for _, claim := range o.claims {
				acct.mark(o.namespace+"/"+claim, mark)
			}
		}
	}
	// The marker reaches the pods no rule lists: a pod that declares no engine
	// matches no rule's selector, and its marker is what must still keep the
	// volumes it mounts out of the run. The accounting of claims always runs —
	// every claim of the cluster is covered, declared, excluded or reported — so
	// this pass always runs with it.
	if err := d.markMarkedPods(ctx, client, acct); err != nil {
		return nil, err
	}

	// The books the claim rules share: a claim a rule rendered is reported once,
	// and the accounting rule — last of every rule of both lists — reports only
	// what no rule of the config accounted for.
	acct.book = claimBook{renderedBy: map[string]string{}, reported: map[string]bool{}}
	for i := range rules {
		pr := &rules[i]
		if pr.rule.Resource != "persistentvolumeclaims" {
			continue
		}
		for _, o := range listings[i] {
			if f, ok := claimFound(pr, o, st, acct); ok {
				byRule[i] = append(byRule[i], f)
			}
		}
	}

	var found []discovery.Found
	for i := range rules {
		for _, f := range byRule[i] {
			st.log.Debug("discovered", "rule", rules[i].rule.Name, "instance", f.Name,
				"artifact", f.Artifact.Name, "problem", f.Problem, "excluded", string(f.Excluded))
			found = append(found, f)
			if err := ctx.Err(); err != nil {
				// A problem produced after the context ended is not data: the
				// caller asked to stop, and a read cut off by that stop would
				// otherwise look like this object's own fault — cancellation is
				// terminal everywhere else, and it is here too.
				return nil, err
			}
		}
	}
	return found, nil
}

// claimMark is what the pod that mounts a claim decided about it: it excluded
// the claim (the pod itself carries the marker) or covered it (its own dump
// carries the data). A mark with no kind is the pod failing to decide: its read
// stumbled retryably, so the claim is reported as a retryable problem naming the
// pod, and the earlier pass that did decide it stands (engine/discover.go merges
// a retryable stumble back to the healthy verdict an earlier pass reached).
//
// group is the artifact group the exclusion is recorded under, and only a
// covering pod has one: it is the group of that pod's own artifact, which is
// what carries the claim's data. An ignored mark's group is the decision's to
// name where the claim is judged, not the pod's.
type claimMark struct {
	kind   model.ExclusionKind
	group  string
	reason string
}

// podMark is the verdict a rendered pod carries for the claims it mounts; only
// a rule that covers volumes asks for one. A pod that failed to render covers
// nothing: its permanent problems leave its claims unaccounted — the accounting
// rule reports them and names the pod — while a retryable one leaves the verdict
// to another pass.
func podMark(pr *preparedRule, o object, f discovery.Found, markerReason string) (claimMark, bool) {
	switch {
	case f.Excluded == model.ExclusionIgnored:
		return claimMark{kind: model.ExclusionIgnored,
			reason: fmt.Sprintf("the pod that mounts it, %s/%s, %s", o.namespace, o.name, markerReason)}, true
	case f.Problem != "" && f.ProblemRetryable:
		return claimMark{reason: fmt.Sprintf("the pod that mounts it, %s/%s, could not be read in this pass: %s",
			o.namespace, o.name, f.Problem)}, true
	case f.Problem != "":
		return claimMark{}, false
	default:
		return claimMark{kind: model.ExclusionCovered, group: f.Artifact.Group,
			reason: fmt.Sprintf("covered by the dump of pod %s/%s (rule %q)", o.namespace, o.name, pr.rule.Name)}, true
	}
}

// accounting is what the discovery has decided about every claim of the cluster
// while it walks its rules: the verdict a mounting pod carried (marks), the pod
// that mounts the claim at all — what an unaccounted claim names — and the books
// that keep two rules from backing one claim up twice.
type accounting struct {
	marks    map[string]claimMark
	mounting map[string]string
	book     claimBook
}

// mark records a pod's verdict for one claim; the first pod in config order
// names the reason.
func (a *accounting) mark(key string, mark claimMark) {
	if _, decided := a.marks[key]; !decided {
		a.marks[key] = mark
	}
}

// rememberMounts records which pod mounts a claim, for the message an
// unaccounted claim carries: a claim nobody declared is best explained by the
// pod that mounts it and the phase that pod is in.
func (a *accounting) rememberMounts(o object) {
	for _, claim := range o.claims {
		key := o.namespace + "/" + claim
		if _, known := a.mounting[key]; !known {
			a.mounting[key] = fmt.Sprintf("%s/%s (%s)", o.namespace, o.name, o.phase)
		}
	}
}

// claimBook is what the claim rules decided about one claim so far, so that two
// rules with overlapping selectors cannot produce two artifacts for one volume.
type claimBook struct {
	// renderedBy is the rule that produced an artifact for the claim.
	renderedBy map[string]string
	// reported is true once any rule emitted a Found for the claim — an
	// artifact, an exclusion or a problem. A later rule adds nothing.
	reported map[string]bool
}

// markMarkedPods decides the pods that carry the exclusion marker itself,
// whatever the rules say: a pod that declares no engine matches no rule's
// selector, and the marker on its own pod template is what keeps the volumes it
// mounts out of the run.
//
// It lists every pod rather than the marked ones, and applies the marker here:
// the same listing is what tells the accounting which pod mounts a claim, and a
// claim nobody declared is best explained by the pod that mounts it — including
// a pod in a phase no rule asks for, and one no rule matches at all. The listing
// is paged like every other, and a rule's own listing still filters server-side.
func (d *Discoverer) markMarkedPods(ctx context.Context, client *kubeapi.Client, acct *accounting) error {
	marker := d.state.exclude
	pods, err := client.ListPods(ctx, kubeapi.ListRequest{PageSize: d.state.pageSize})
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}
	for _, o := range podObjects(pods) {
		acct.rememberMounts(o)
		if !marker.matches(o.labels) {
			continue
		}
		mark := claimMark{kind: model.ExclusionIgnored,
			reason: fmt.Sprintf("the pod that mounts it, %s/%s, %s", o.namespace, o.name, marker.reason())}
		for _, claim := range o.claims {
			acct.mark(o.namespace+"/"+claim, mark)
		}
	}
	return nil
}

// claimFound decides one claim's fate for one rule. The verdicts are exclusive,
// in this order: the claim's own marker, a lost binding, the pod that mounts it,
// the undeclared accounting, and finally the render — so every claim of the
// cluster is exactly one of covered, excluded, declared or reported.
func claimFound(pr *preparedRule, o object, st *state, acct *accounting) (discovery.Found, bool) {
	key := o.namespace + "/" + o.name
	if acct.book.reported[key] {
		rendering := !pr.rule.Undeclared && o.phase == pr.rule.Phase
		if by, ok := acct.book.renderedBy[key]; ok && rendering {
			return pr.problemFound(st, o, fmt.Errorf("%s is already backed up by rule %q: two rules whose selectors match one claim would back it up twice",
				o.id(), by)), true
		}
		return discovery.Found{}, false
	}
	if st.exclude.matches(o.labels) {
		acct.book.reported[key] = true
		return pr.excludedFound(st, o, model.ExclusionIgnored, pr.wouldRenderGroup(st, o), st.exclude.reason()), true
	}
	if o.phase == "Lost" {
		// The claim lost the volume it was bound to: whatever it held is not
		// reachable any more. That is a fact about the cluster, not a blip, and
		// it must never read as an absent object.
		acct.book.reported[key] = true
		return pr.problemFound(st, o, fmt.Errorf("%s is in phase Lost: the volume it was bound to is gone, so its data is not readable", o.id())), true
	}
	if mark, ok := acct.marks[key]; ok {
		acct.book.reported[key] = true
		if mark.kind == "" {
			// The pod that mounts it stumbled retryably: the claim is not
			// decided in this pass, and the merge restores the verdict an
			// earlier pass reached.
			return pr.problemFound(st, o, plugin.Transient(errors.New(mark.reason))), true
		}
		group := mark.group
		if mark.kind == model.ExclusionIgnored {
			// The pod's marker keeps the claim out; the group is the one the
			// volume rule that would have rendered it carries, and this rule is
			// that rule when it matched the claim and the claim is in its phase.
			group = pr.wouldRenderGroup(st, o)
		}
		return pr.excludedFound(st, o, mark.kind, group, mark.reason), true
	}
	if pr.rule.Undeclared {
		if o.phase != "Bound" {
			// Pending under WaitForFirstConsumer: the volume does not exist
			// yet, so there is nothing to back up and nothing to report.
			return discovery.Found{}, false
		}
		acct.book.reported[key] = true
		problem, hint := undeclaredProblem(st, acct, o, key)
		f := pr.problemFound(st, o, errors.New(problem))
		f.Hint = hint
		return f, true
	}
	if o.phase != pr.rule.Phase {
		return discovery.Found{}, false
	}
	acct.book.reported[key] = true
	acct.book.renderedBy[key] = pr.rule.Name
	return pr.render(st, o), true
}

// undeclaredProblem is the one message every claim no rule accounted for gets,
// and it says what to DO: the label(s) that would declare it, read from this
// config's own volume rules — their selectors and their names — and the marker
// that would exclude it. Naming a rule's selector is the difference between a
// reader being told "a rule has to match" and being told which label to type;
// the plugin still hardcodes nothing, because the selectors come from the rules
// themselves.
//
// The second return is the short form a notification shows: the labels and the
// marker, without the reasoning.
func undeclaredProblem(st *state, acct *accounting, o object, key string) (problem, hint string) {
	marker := st.exclude.marker()
	var selectors, named []string
	for i := range st.rules {
		r := &st.rules[i]
		if r.rule.Resource != "persistentvolumeclaims" || r.rule.Undeclared {
			continue
		}
		selectors = append(selectors, r.rule.Selector)
		named = append(named, fmt.Sprintf("%s (rule %q)", r.rule.Selector, r.rule.Name))
	}
	context := undeclaredContext(acct, key)
	if len(named) == 0 {
		// A discovery with no volume rule at all: the only action available is
		// the opt-out, and saying otherwise would send the reader looking for a
		// label that does not exist.
		return fmt.Sprintf("%s is backed up by no rule: %s; this discovery declares no volume rule, so add %s to leave it out on purpose",
				o.id(), context, marker),
			fmt.Sprintf("no backup rule: add %s", marker)
	}
	return fmt.Sprintf("%s is backed up by no rule: %s; to back it up, add %s — or add %s to leave it out on purpose",
			o.id(), context, strings.Join(named, " or "), marker),
		fmt.Sprintf("no backup rule: add %s or %s", strings.Join(selectors, " or "), marker)
}

// undeclaredContext says what the run knows about an unaccounted claim beyond
// its name: the pod that mounts it and the phase that pod is in — the two facts
// that tell an operator whether the claim holds a database nobody declared or a
// workload that was simply not running. Every pod the run listed is known here,
// whatever its phase and whether or not a rule matched it; a claim no pod
// mounts at all is the one the last sentence describes.
func undeclaredContext(acct *accounting, key string) string {
	if pod, ok := acct.mounting[key]; ok {
		return fmt.Sprintf("no rule's source covers the pod that mounts it, %s", pod)
	}
	return "no rule and no pod accounts for it"
}

// objects lists the rule's resource: the label selector, and a pod's
// status.phase, are the server's own filters (the API server registers
// status.phase for pods only). A claim listing carries every phase: the server
// refuses a status.phase selector for the resource with HTTP 400, and a rule's
// phase decides per claim in claimFound — a claim in another phase is not
// silently absent from the run, because Pending and Lost are answers too.
func (d *Discoverer) objects(ctx context.Context, client *kubeapi.Client, r rule) ([]object, error) {
	// Validate builds a rule with one of the two resource names and nothing
	// else, so the claim listing is the other arm of this branch.
	if r.Resource == "pods" {
		pods, err := client.ListPods(ctx, kubeapi.ListRequest{Selector: r.Selector, Phase: r.Phase, PageSize: d.state.pageSize})
		if err != nil {
			return nil, err
		}
		return podObjects(pods), nil
	}
	pvcs, err := client.ListPVCs(ctx, kubeapi.ListRequest{Selector: r.Selector, PageSize: d.state.pageSize})
	if err != nil {
		return nil, err
	}
	objects := make([]object, 0, len(pvcs))
	for _, p := range pvcs {
		objects = append(objects, object{
			resource:   "persistentvolumeclaim",
			namespace:  p.Metadata.Namespace,
			name:       p.Metadata.Name,
			labels:     p.Metadata.Labels,
			phase:      p.Status.Phase,
			volumeName: p.Spec.VolumeName,
		})
	}
	return sortObjects(objects), nil
}

// podObjects turns a pod listing into the objects the rules and the marker pass
// work on, sorted by namespace and name.
func podObjects(pods []kubeapi.Pod) []object {
	objects := make([]object, 0, len(pods))
	for _, p := range pods {
		objects = append(objects, object{
			resource:   "pod",
			namespace:  p.Metadata.Namespace,
			name:       p.Metadata.Name,
			labels:     p.Metadata.Labels,
			phase:      p.Status.Phase,
			containers: p.Spec.Containers,
			claims:     claimNames(p.Spec.Volumes),
		})
	}
	return sortObjects(objects)
}

// claimNames is the claim every volume of a pod comes from, in spec order: the
// volumes a pod with a declared engine covers, and the ones a marked pod
// carries into exclusion. A volume that mounts no claim — a configMap, a
// secret, an emptyDir — is not one of them.
func claimNames(volumes []kubeapi.Volume) []string {
	var names []string
	for _, v := range volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName != "" {
			names = append(names, v.PersistentVolumeClaim.ClaimName)
		}
	}
	return names
}

func sortObjects(objects []object) []object {
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].namespace != objects[j].namespace {
			return objects[i].namespace < objects[j].namespace
		}
		return objects[i].name < objects[j].name
	})
	return objects
}

// render turns one matched object into one Found: the artifact envelope and the
// source config are rendered for it, environment secrets are written under
// secrets_dir, and the source instance name is the rule name plus the object's
// namespace and name — unique within a run.
//
// A template that cannot be rendered does not abort the discovery: the three
// parts are rendered independently so every problem of the object is reported
// at once, the Found keeps its identity (Name never depends on a template) and
// the artifact envelope falls back to names derived from the rule and the
// object, with Problem saying which part failed and what stands in its place.
// Type and Config are left empty only when their part failed; Problem never
// carries a secret value, only the message of the failing template call.
//
// Every problem — permanent, not_found, unclassified or transient — stays with
// this one Found: one object's fault is one artifact, never the discovery, and
// Check stays silent about it so a run that skips a discovery whose Check
// failed cannot lose the rest. The one difference is the class: an error the
// API client explicitly classed transient (it marks 5xx, timeouts and
// connection failures that way) also sets Found.ProblemRetryable, which asks
// the engine — the layer that owns the retry policy — for another Discover
// while its budget lasts; when the budget is spent this Found stands with its
// Problem like any other.
func (r *preparedRule) render(st *state, o object) discovery.Found {
	st.object = &o
	defer func() { st.object = nil }()
	f, envelopeErrs := r.envelope(st, o)
	f.Type = r.rule.Source.Type
	var problems []string
	add := func(err error) {
		problems = append(problems, fmt.Sprintf("%s: %v", o.id(), err))
		if class, _ := plugin.ClassOf(err); class == plugin.ClassTransient {
			f.ProblemRetryable = true
		}
	}
	for _, err := range envelopeErrs {
		add(err)
	}
	config, err := r.source.renderJSON(o.data())
	if err != nil {
		add(fmt.Errorf("source config: %w; the source config is left empty", err))
	} else {
		f.Config = config
	}
	if len(problems) > 0 {
		f.Problem = strings.Join(problems, "; ")
	}
	return f
}

// envelope is the Found every verdict about one object starts from: the source
// instance name — the rule, the namespace and the object name, unique within a
// run and independent of every template — and the artifact envelope, with the
// group and name templates rendered so an object that produced no artifact is
// still named the way its rule names one. The returned errors are the templates
// that could not be rendered and now stand as the rule-name and namespace/name
// fallbacks: a render reports them among the object's problems, while a problem
// and an exclusion keep the fallback silently — what they carry is already the
// message.
func (r *preparedRule) envelope(st *state, o object) (discovery.Found, []error) {
	f := discovery.Found{
		Name: r.rule.Name + "/" + o.namespace + "/" + o.name,
		Artifact: model.Artifact{
			Group:       r.rule.Name,
			Name:        o.namespace + "/" + o.name,
			Importance:  r.rule.Artifact.Importance,
			Sensitivity: r.rule.Artifact.Sensitivity,
		},
		Pipeline: r.rule.Pipeline,
	}
	if r.rule.Undeclared {
		// The accounting names its artifacts itself: undeclared_volumes is the
		// group and the instance prefix, the object is the artifact — the same
		// shape every rule's own fallback takes.
		return f, nil
	}
	// Bind the object for the templates and restore what was bound before: a
	// render binds it for the rest of the object's work, a problem or an
	// exclusion needs it only for the envelope.
	previous := st.object
	st.object = &o
	defer func() { st.object = previous }()
	data := o.data()
	var errs []error
	if group, err := r.rule.Artifact.Group.Render(data); err != nil {
		errs = append(errs, fmt.Errorf("artifact group: %w; falls back to %q", err, f.Artifact.Group))
	} else {
		f.Artifact.Group = group
	}
	if name, err := r.rule.Artifact.Name.Render(data); err != nil {
		errs = append(errs, fmt.Errorf("artifact name: %w; falls back to %q", err, f.Artifact.Name))
	} else {
		f.Artifact.Name = name
	}
	return f, errs
}

// problemFound is one object's fault as one failed artifact: the envelope, the
// message, and the retry class the error carries — a transient API failure asks
// the engine for another Discover, a permanent one stands.
func (r *preparedRule) problemFound(st *state, o object, err error) discovery.Found {
	f, _ := r.envelope(st, o)
	f.Problem = err.Error()
	if class, _ := plugin.ClassOf(err); class == plugin.ClassTransient {
		f.ProblemRetryable = true
	}
	return f
}

// excludedFound is an object that was seen and deliberately produces no
// artifact: the envelope names it, and nothing of its source is built — an
// ignored volume never reads a Secret and never writes a file under secrets_dir.
// group is the artifact group the record carries: the group the rule would have
// produced, when a rule would have produced this object, and empty when none
// would — the accounting setting's own name is what an UNDECLARED claim's failed
// artifact is called, never the group of a decision.
func (r *preparedRule) excludedFound(st *state, o object, kind model.ExclusionKind, group, reason string) discovery.Found {
	f, _ := r.envelope(st, o)
	f.Excluded = kind
	f.ExcludedGroup = group
	f.ExcludedReason = reason
	return f
}

// wouldRenderGroup is the artifact group this rule would have produced for an
// object it did render, or empty when this rule would not have rendered it at
// all: the accounting rule deploys no artifact, and a claim in a phase its rule
// does not name is one the rule never touched. A render that fails falls back to
// the rule's name, exactly as the object's own artifact would.
func (r *preparedRule) wouldRenderGroup(st *state, o object) string {
	if r.rule.Undeclared || o.phase != r.rule.Phase {
		return ""
	}
	f, _ := r.envelope(st, o)
	return f.Artifact.Group
}

// podFound renders one pod, unless the pod carries the exclusion marker: then
// the pod is a decision, and the volumes it mounts inherit it.
func (d *Discoverer) podFound(pr *preparedRule, o object) discovery.Found {
	if d.state.exclude.matches(o.labels) {
		return pr.excludedFound(d.state, o, model.ExclusionIgnored, pr.wouldRenderGroup(d.state, o), d.state.exclude.reason())
	}
	return pr.render(d.state, o)
}

// Validate runs inside plugin.Decode: structure and enums offline, and every
// template of every rule parsed — the artifact fields by the framework, the
// strings of each source config here.
func (c *Config) Validate() error {
	st := c.stateOrNew()
	var errs []error
	if c.APIHostEnv == "" {
		errs = append(errs, errors.New("/api_host_env is empty"))
	}
	if c.APIPortEnv == "" {
		errs = append(errs, errors.New("/api_port_env is empty"))
	}
	errs = append(errs, plugin.AbsolutePath(c.TokenFile, "/token_file")...)
	errs = append(errs, plugin.AbsolutePath(c.CAFile, "/ca_file")...)
	errs = append(errs, plugin.AbsolutePath(c.SecretsDir, "/secrets_path")...)
	if c.RequestTimeout.Std() <= 0 {
		errs = append(errs, fmt.Errorf("/request_timeout must be positive, got %s", c.RequestTimeout))
	}
	if c.ListPageSize < 1 {
		errs = append(errs, fmt.Errorf("/list_page_size must be at least 1, got %d", c.ListPageSize))
	}
	if c.Exclude.Label == "" {
		errs = append(errs, errors.New("/exclude/label is empty; the marker's label name is the config's, never the code's"))
	}
	if c.Exclude.Value == "" {
		errs = append(errs, errors.New("/exclude/value is empty; the marker's value is the config's, never the code's"))
	}
	if len(c.Pods) == 0 && len(c.Volumes) == 0 {
		errs = append(errs, errors.New("/pods and /volumes are both empty: a discovery with no rule finds nothing"))
	}
	errs = append(errs, validateUndeclared(c.UndeclaredVolumes)...)
	funcs := st.funcs()
	seen := make(map[string]bool, len(c.Pods)+len(c.Volumes))
	rules := make([]preparedRule, 0, len(c.Pods)+len(c.Volumes)+1)
	for i := range c.Pods {
		r := c.Pods[i]
		path := fmt.Sprintf("/pods/%d", i)
		pr, ruleErrs := c.prepareRule(rule{
			Name: r.Name, Resource: "pods", Path: path, Selector: r.Selector, Phase: r.Phase,
			Artifact: r.Artifact, Pipeline: r.Pipeline, Source: r.Source,
			CoversVolumes: r.CoversVolumes, MinMatches: r.MinMatches,
		}, path, seen, funcs)
		errs = append(errs, ruleErrs...)
		if pr != nil {
			rules = append(rules, *pr)
		}
	}
	for i := range c.Volumes {
		r := c.Volumes[i]
		path := fmt.Sprintf("/volumes/%d", i)
		pr, ruleErrs := c.prepareRule(rule{
			Name: r.Name, Resource: "persistentvolumeclaims", Path: path, Selector: r.Selector, Phase: r.Phase,
			Artifact: r.Artifact, Pipeline: r.Pipeline, Source: r.Source, MinMatches: r.MinMatches,
		}, path, seen, funcs)
		errs = append(errs, ruleErrs...)
		if pr != nil {
			rules = append(rules, *pr)
		}
	}
	// The accounting always runs, after every rule of both lists: one synthetic
	// entry that lists every claim and reports the ones nothing accounted for.
	// It renders no source and names itself from the setting, which therefore
	// has to be a name no rule of either list already took.
	if seen[c.UndeclaredVolumes.Name] {
		errs = append(errs, fmt.Errorf("/undeclared_volumes/name %q is already a rule name; the accounting is one more rule of this discovery", c.UndeclaredVolumes.Name))
	}
	rules = append(rules, preparedRule{rule: rule{
		Name: c.UndeclaredVolumes.Name, Resource: "persistentvolumeclaims", Path: "/undeclared_volumes",
		Undeclared: true,
		Artifact: ArtifactConfig{
			Importance:  c.UndeclaredVolumes.Importance,
			Sensitivity: model.SensitivityPlain,
		},
	}})
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	st.rules = rules
	return nil
}

// prepareRule validates one config rule and turns it into the shape this
// package works with: the checks both lists share, and the nested source config
// parsed into the templates it renders per object. It returns nil — after
// reporting why — when the rule cannot be prepared; Validate fails as a whole
// then, so the prepared list is never used.
func (c *Config) prepareRule(r rule, path string, seen map[string]bool, funcs template.FuncMap) (*preparedRule, []error) {
	var errs []error
	switch {
	case r.Name == "":
		errs = append(errs, fmt.Errorf("%s/name is empty", path))
	case seen[r.Name]:
		errs = append(errs, fmt.Errorf("%s/name %q repeats", path, r.Name))
	default:
		seen[r.Name] = true
	}
	// An empty selector is legitimate: it says "every object of this resource".
	switch {
	case r.Phase == "":
		errs = append(errs, fmt.Errorf("%s/phase is empty", path))
	case !slices.Contains(resourcePhases[r.Resource], r.Phase):
		// A phase the resource does not have is a rule that matches nothing,
		// forever, and nothing at run time would ever say so.
		errs = append(errs, fmt.Errorf("%s/phase %q is not a phase of %s (%s)", path, r.Phase, r.Resource, strings.Join(resourcePhases[r.Resource], ", ")))
	}
	for i, ref := range r.Pipeline {
		if ref == "" {
			errs = append(errs, fmt.Errorf("%s/pipeline/%d is empty; an empty pipeline is written as []", path, i))
		}
	}
	if r.Source.Type == "" {
		errs = append(errs, fmt.Errorf("%s/source/type is empty", path))
	}
	if r.MinMatches < 0 {
		errs = append(errs, fmt.Errorf("%s/min_matches must not be negative, got %d", path, r.MinMatches))
	}
	if r.Artifact.Group.Text() == "" {
		errs = append(errs, fmt.Errorf("%s/artifact/group is empty", path))
	}
	if r.Artifact.Name.Text() == "" {
		errs = append(errs, fmt.Errorf("%s/artifact/name is empty", path))
	}
	if r.Artifact.Importance != model.ImportanceRequired && r.Artifact.Importance != model.ImportanceBestEffort {
		errs = append(errs, fmt.Errorf("%s/artifact/importance must be \"required\" or \"best_effort\", got %q", path, r.Artifact.Importance))
	}
	if r.Artifact.Sensitivity != model.SensitivityPlain && r.Artifact.Sensitivity != model.SensitivitySecret {
		errs = append(errs, fmt.Errorf("%s/artifact/sensitivity must be \"plain\" or \"secret\", got %q", path, r.Artifact.Sensitivity))
	}
	if len(errs) > 0 {
		return nil, errs
	}
	source, err := parseSourceConfig(r.Source.Config, funcs, path+"/source/config")
	if err != nil {
		return nil, []error{err}
	}
	return &preparedRule{rule: r, source: source}, nil
}

// validateUndeclared checks the one setting behind the accounting.
func validateUndeclared(u UndeclaredVolumes) []error {
	var errs []error
	if u.Name == "" {
		errs = append(errs, errors.New("/undeclared_volumes/name is empty"))
	}
	if u.Importance != model.ImportanceRequired && u.Importance != model.ImportanceBestEffort {
		errs = append(errs, fmt.Errorf("/undeclared_volumes/importance must be \"required\" or \"best_effort\", got %q", u.Importance))
	}
	return errs
}

// example is the reference rule set of an opt-out deployment: two pod rules
// declare their engine on their pod templates — the database dump covers the
// volumes its pod mounts — one volume rule declares a claim as a file volume, and
// undeclared_volumes accounts for whatever claim is left. Every label name stands
// here, none in code.
const example = `{
  "api_host_env": "KUBERNETES_SERVICE_HOST",
  "api_port_env": "KUBERNETES_SERVICE_PORT",
  "token_file": "/var/run/secrets/kubernetes.io/serviceaccount/token",
  "ca_file": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
  "request_timeout": "30s",
  "secrets_path": "/tmp/discovered",
  "list_page_size": 500,
  "exclude": {"label": "backup.example/ignore", "value": "true"},
  "undeclared_volumes": {"name": "undeclared", "importance": "required"},
  "pods": [
    {
      "name": "postgres",
      "selector": "backup.example/engine=postgres",
      "phase": "Running",
      "covers_volumes": true,
      "min_matches": 0,
      "artifact": {"group": "databases", "name": "{{ .Namespace }}/{{ label \"backup.example/instance\" }}", "importance": "required", "sensitivity": "plain"},
      "pipeline": ["compress"],
      "source": {
        "type": "postgres",
        "config": {
          "address": "{{ label \"backup.example/instance\" }}.{{ .Namespace }}.svc.cluster.local:{{ port 0 0 }}",
          "database": "{{ envValue 0 \"POSTGRES_DB\" }}",
          "username": "{{ envValue 0 \"POSTGRES_USER\" }}",
          "password": {"file": "{{ envFile 0 \"POSTGRES_PASSWORD\" }}"},
          "format": "custom",
          "compression": "none",
          "connect_attempts": 6,
          "connect_interval": "5s",
          "all_databases": true,
          "exclude_databases": []
        }
      }
    },
    {
      "name": "redis",
      "selector": "backup.example/engine=redis",
      "phase": "Running",
      "covers_volumes": true,
      "min_matches": 0,
      "artifact": {"group": "caches", "name": "{{ .Namespace }}/{{ label \"backup.example/instance\" }}", "importance": "best_effort", "sensitivity": "plain"},
      "pipeline": ["compress"],
      "source": {
        "type": "redis",
        "config": {
          "address": "{{ label \"backup.example/instance\" }}.{{ .Namespace }}.svc.cluster.local:{{ port 0 0 }}",
          "password": {"file": "{{ envFile 0 \"REDIS_PASSWORD\" }}"},
          "connect_attempts": 6,
          "connect_interval": "5s"
        }
      }
    }
  ],
  "volumes": [
    {
      "name": "volumes",
      "selector": "backup.example/engine=files",
      "phase": "Bound",
      "min_matches": 0,
      "artifact": {"group": "volumes", "name": "{{ .Namespace }}/{{ .Name }}", "importance": "required", "sensitivity": "plain"},
      "pipeline": ["compress"],
      "source": {
        "type": "files",
        "config": {"root": "{{ volumePath }}", "ignore_vanished": true}
      }
    }
  ]
}`
