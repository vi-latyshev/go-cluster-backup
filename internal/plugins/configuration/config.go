package configuration

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Config is the whole configuration as one JSON document — the JSON form of the
// YAML file a deployment decodes. One file carries every command: a section a
// given command needs is one the file states, and the rest are absent. Sections
// are optional where a file may leave them out (a restore-only file has no run,
// no stage and no storages); inside a present section every field is required
// unless a defaults document fills it.
type Config struct {
	// Plugins is the unified plugin root: what the deployment selects and the
	// settings of every plugin type. Absent means every compiled plugin is
	// available and every type's settings come from the defaults document.
	Plugins *Plugins   `json:"plugins" optional:"true"`
	Run     *RunConfig `json:"run" optional:"true"`
	// Layout is the key layout of every storage: a section of its own, filled
	// by the defaults document, so a file need not state it at all.
	Layout      LayoutConfig               `json:"layout"`
	Stage       *StageConfig               `json:"stage" optional:"true"`
	Restore     *RestoreConfig             `json:"restore" optional:"true"`
	Discoveries map[string]Instance        `json:"discoveries" optional:"true"`
	Sources     map[string]SourceInstance  `json:"sources" optional:"true"`
	Processors  map[string]Instance        `json:"processors" optional:"true"`
	Storages    map[string]StorageInstance `json:"storages" optional:"true"`
	Catalog     *Instance                  `json:"catalog" optional:"true"`
	Notifiers   map[string]Instance        `json:"notifiers" optional:"true"`
}

// Plugins is the one plugin root shared by the defaults document and a
// deployment's file. The compiled set is the starting point; a bundle turns its
// plugins off, then `disabled` turns its own off in turn, and `config` carries
// the partial settings of every plugin type — keyed by the factory ID
// `kind/type` — including the application providers. Settings never create an
// instance or a discovery result.
type Plugins struct {
	// Bundles are the named selections applied in the order they are listed.
	// A deployment that wants a bundled-off plugin back removes the bundle.
	Bundles []string `json:"bundles" optional:"true" doc:"named selections applied in this order; [] applies none"`
	// Disabled names plugin IDs the deployment turns off after every bundle.
	Disabled []string `json:"disabled" optional:"true" doc:"plugin IDs turned off after every bundle; [] disables none"`
	// Config holds the partial settings of each plugin type, keyed kind/type.
	// Precedence: the defaults document, then this block, then the instance's
	// own config.
	Config map[string]jsontext.Value `json:"config" optional:"true" doc:"per-type partial settings, keyed kind/type; an instance's own config outranks it"`
}

// Kind names one plugin kind as the config file spells it: the first half of a
// factory ID.
type Kind string

const (
	KindDiscovery Kind = "discovery"
	KindSource    Kind = "source"
	KindProcessor Kind = "processor"
	KindStorage   Kind = "storage"
	KindRetention Kind = "retention"
	KindNotifier  Kind = "notifier"
	KindCatalog   Kind = "catalog"
)

var kindOrder = []Kind{KindDiscovery, KindSource, KindProcessor, KindStorage, KindRetention, KindNotifier, KindCatalog}

// ParseID splits one factory ID into its kind and its type — the one spelling
// of the derivation, shared by the selection, the plugin blocks and the
// diagnostics. A plugin that contributes no factory declares its own stable ID
// (a name without a slash), which ParseID refuses.
func ParseID(id string) (Kind, string, bool) {
	kind, typ, ok := strings.Cut(id, "/")
	if !ok || kind == "" || typ == "" || strings.Contains(typ, "/") {
		return "", "", false
	}
	return Kind(kind), typ, true
}

// ValidID reports whether one selection or settings key is a plugin ID: either
// a factory ID of a known kind, or the stable ID a non-factory plugin declares
// for itself.
func ValidID(id string) bool {
	if !strings.Contains(id, "/") {
		return id != ""
	}
	kind, _, ok := ParseID(id)
	return ok && kind.known()
}

// Role is one capability a command needs the selection to keep available.
type Role string

const (
	// RoleCLI is the default host and diagnostics role.
	RoleCLI Role = "cli"
	// RoleBackup runs a scheduled or a manual backup.
	RoleBackup Role = "backup-job"
	// RoleStage runs the stage job.
	RoleStage Role = "stage-job"
	// RoleRestore runs every restore command.
	RoleRestore Role = "restore-job"
	// RoleConfigCheck is validate, the offline hook included.
	RoleConfigCheck Role = "config-check"
)

// Roles is the compiled role table: which plugin IDs provide each role. The
// loader owns it; the configuration layer only reads it.
type Roles map[Role][]string

// Active is the resolved selection: every compiled plugin ID with the
// deployment's bundles and disabled list applied. Its zero value enables
// everything, which is what a configuration without a plugins root means.
type Active struct {
	ids map[string]bool
}

// Enabled reports whether one plugin ID is active. An ID the compiled set does
// not carry is not active.
func (a Active) Enabled(id string) bool {
	if a.ids == nil {
		return true
	}
	return a.ids[id]
}

// Bundle is one named selection a distribution compiles in. Applying a bundle
// turns off every plugin it names; a deployment turns a plugin back on by
// dropping the bundle.
type Bundle struct {
	Name     string
	Disables []string
}

// SelectionRef is one entry of plugins.bundles or plugins.disabled with the
// index it carried in the document. A tolerant reader keeps each readable
// entry's original JSON index — a malformed element is skipped, never
// renumbered — and the validator reports that index.
type SelectionRef struct {
	Index int
	Name  string
}

// SelectionRefs projects one decoded Plugins value into indexed references: the
// slice position is the document index.
func (p *Plugins) SelectionRefs() (bundles, disabled []SelectionRef) {
	if p == nil {
		return nil, nil
	}
	bundles = make([]SelectionRef, 0, len(p.Bundles))
	for i, name := range p.Bundles {
		bundles = append(bundles, SelectionRef{Index: i, Name: name})
	}
	disabled = make([]SelectionRef, 0, len(p.Disabled))
	for i, id := range p.Disabled {
		disabled = append(disabled, SelectionRef{Index: i, Name: id})
	}
	return bundles, disabled
}

// CheckSelection is the one definition of the selection-entry rules, shared by
// the decoded document and a tolerant reader that kept the raw indices: an
// empty value, an unknown bundle or plugin ID and a repeated entry are errors
// naming the entry's own index. A refused entry is dropped from the returned
// valid lists, so it can neither grant nor withdraw a plugin, and a later
// readable entry is judged on its own.
func CheckSelection(bundles, disabled []SelectionRef, compiled, bundleNames []string) (validBundles, validDisabled []SelectionRef, errs []error) {
	knownBundles := append([]string(nil), bundleNames...)
	slices.Sort(knownBundles)
	bundleSet := make(map[string]bool, len(knownBundles))
	for _, name := range knownBundles {
		bundleSet[name] = true
	}
	compiledSet := make(map[string]bool, len(compiled))
	for _, id := range compiled {
		compiledSet[id] = true
	}
	off := sortedIDs(compiled)
	seenBundles := make(map[string]bool, len(bundles))
	for _, ref := range bundles {
		switch {
		case ref.Name == "":
			errs = append(errs, fmt.Errorf("/plugins/bundles/%d is empty", ref.Index))
		case !bundleSet[ref.Name]:
			errs = append(errs, fmt.Errorf("/plugins/bundles/%d: unknown bundle %q (known: %v)", ref.Index, ref.Name, knownBundles))
		case seenBundles[ref.Name]:
			errs = append(errs, fmt.Errorf("/plugins/bundles/%d: bundle %q is listed twice", ref.Index, ref.Name))
		default:
			seenBundles[ref.Name] = true
			validBundles = append(validBundles, ref)
		}
	}
	seenIDs := make(map[string]bool, len(disabled))
	for _, ref := range disabled {
		switch {
		case !ValidID(ref.Name):
			errs = append(errs, fmt.Errorf("/plugins/disabled/%d: %q is not a plugin ID (a factory kind/type of a known kind, or a stable name)", ref.Index, ref.Name))
		case !compiledSet[ref.Name]:
			errs = append(errs, fmt.Errorf("/plugins/disabled/%d: unknown plugin %q (compiled: %v)", ref.Index, ref.Name, off))
		case seenIDs[ref.Name]:
			errs = append(errs, fmt.Errorf("/plugins/disabled/%d: plugin %q is listed twice", ref.Index, ref.Name))
		default:
			seenIDs[ref.Name] = true
			validDisabled = append(validDisabled, ref)
		}
	}
	return validBundles, validDisabled, errs
}

// SelectionEffects is the one application of validated entries: every disable
// of every valid bundle, then the valid disabled list, in the order they turn
// plugins off.
func SelectionEffects(validBundles, validDisabled []SelectionRef, table []Bundle) []string {
	byName := make(map[string][]string, len(table))
	for _, b := range table {
		byName[b.Name] = b.Disables
	}
	off := map[string]bool{}
	var out []string
	turnOff := func(id string) {
		if id == "" || off[id] {
			return
		}
		off[id] = true
		out = append(out, id)
	}
	for _, ref := range validBundles {
		for _, id := range byName[ref.Name] {
			turnOff(id)
		}
	}
	for _, ref := range validDisabled {
		turnOff(ref.Name)
	}
	return out
}

// Select resolves the deployment's selection against the compiled plugin set.
// Every bundle is applied in the order listed, then the disabled list; an
// unknown bundle, an unknown plugin ID and a repeated entry are errors, so a
// typo never silently leaves a plugin on. The returned Active still carries
// every compiled ID — an invalid selection fails the caller closed, it never
// grants work. The entry rules are CheckSelection's; this method adds the
// compiled table's own integrity check and the effects.
func (p *Plugins) Select(compiled []string, bundles []Bundle) (Active, []error) {
	var errs []error
	compiledSet := make(map[string]bool, len(compiled))
	for _, id := range compiled {
		compiledSet[id] = true
	}
	byName := make(map[string][]string, len(bundles))
	names := make([]string, 0, len(bundles))
	for _, b := range bundles {
		if _, dup := byName[b.Name]; dup {
			errs = append(errs, fmt.Errorf("compiled plugin bundle %q is declared twice", b.Name))
			continue
		}
		byName[b.Name] = b.Disables
		names = append(names, b.Name)
		for _, id := range b.Disables {
			if !compiledSet[id] {
				errs = append(errs, fmt.Errorf("compiled plugin bundle %q disables %q, which is not a compiled plugin (compiled: %v)", b.Name, id, sortedIDs(compiled)))
			}
		}
	}
	bundleRefs, disabledRefs := p.SelectionRefs()
	validBundles, validDisabled, selErrs := CheckSelection(bundleRefs, disabledRefs, compiled, names)
	errs = append(errs, selErrs...)
	off := map[string]bool{}
	for _, id := range SelectionEffects(validBundles, validDisabled, bundles) {
		off[id] = true
	}
	active := Active{ids: make(map[string]bool, len(compiled))}
	for _, id := range compiled {
		active.ids[id] = !off[id]
	}
	return active, errs
}

// ActiveFrom builds the resolved selection from the effective active set — the
// plugin IDs the loader keeps after it applies the deployment's disabled list
// and its own floor and dependency rules.
func ActiveFrom(ids []string) Active {
	active := Active{ids: make(map[string]bool, len(ids))}
	for _, id := range ids {
		active.ids[id] = true
	}
	return active
}

// Bundles is the compiled bundle table of one distribution: the named
// selections its composition root registers. It is a schema kind, so the loader
// holds no policy about bundle names and every build carries its own table.
var Bundles = loader.NewKind[Bundle]("configuration.bundles", loader.KindOptions[Bundle]{Schema: true, Validate: validateBundles})

// AddBundle registers one distribution bundle: the body of a composition root's
// loader.Add apply beside its plugin registrations.
func AddBundle(lr *loader.Registry, b Bundle) error {
	return loader.Register(lr, Bundles, b)
}

// LocalOnly is the distribution's one shipped bundle: the deployment keeps its
// data in local storage only, so the remote backends are off. The IDs are the
// one spelling of a factory identity — the kind token joined with its type —
// never a hand-written kind/type pair, and the bundle is built when it is
// registered rather than in a package-level initializer.
func LocalOnly() Bundle {
	return Bundle{Name: "local-only", Disables: []string{
		ID(KindStorage, "s3"),
		ID(KindStorage, "yandex_disk"),
	}}
}

// validateBundles refuses a table two bundles share a name in, a nameless
// bundle and an empty or repeated disable: one name is one selection, and a
// repeated entry hides which plugin turned a backend off.
func validateBundles(bundles []Bundle) error {
	var errs []error
	seen := map[string]bool{}
	for _, b := range bundles {
		if b.Name == "" {
			errs = append(errs, errors.New("a compiled plugin bundle has no name"))
			continue
		}
		if seen[b.Name] {
			errs = append(errs, fmt.Errorf("compiled plugin bundle %q is declared twice", b.Name))
		}
		seen[b.Name] = true
		ids := map[string]bool{}
		for _, id := range b.Disables {
			if id == "" {
				errs = append(errs, fmt.Errorf("compiled plugin bundle %q disables an empty plugin ID", b.Name))
				continue
			}
			if ids[id] {
				errs = append(errs, fmt.Errorf("compiled plugin bundle %q disables %q twice", b.Name, id))
			}
			ids[id] = true
		}
	}
	return errors.Join(errs...)
}

// Resolve is the one plugin-owned resolver from a decoded configuration to the
// effective selection. It applies the plugins root's bundles and disabled list
// to the compiled set, hands the requested disables to the loader — whose floor,
// dependency and library rules then decide what may actually be turned off —
// and returns the effective set plus every problem. Any error means the caller
// refuses all operational work before the first call; a refused disable never
// narrows the set.
func Resolve(cfg *Config, lr *loader.Registry) (Active, []error) {
	if lr == nil {
		return Active{}, []error{errors.New("the compiled loader registry is required: without it no selection exists")}
	}
	plugins := loader.Plugins(lr)
	compiled := make([]string, 0, len(plugins))
	for _, p := range plugins {
		compiled = append(compiled, p.ID)
	}
	var root *Plugins
	if cfg != nil {
		root = cfg.Plugins
	}
	requested, errs := root.Select(compiled, loader.All(lr, Bundles))
	disabled := make([]string, 0, len(compiled))
	for _, id := range compiled {
		if !requested.Enabled(id) {
			disabled = append(disabled, id)
		}
	}
	for _, problem := range loader.ApplySelection(lr, disabled) {
		errs = append(errs, errors.New(problem))
	}
	return ActiveFrom(loader.ActiveIDs(lr)), errs
}

// RolesFrom inverts the loader's owner-to-roles table into the role-to-providers
// table CheckRoles reads: a role is available while one of its providers is
// active.
func RolesFrom(lr *loader.Registry) Roles {
	if lr == nil {
		return nil
	}
	out := Roles{}
	for owner, roles := range loader.Roles(lr) {
		for _, role := range roles {
			out[Role(role)] = append(out[Role(role)], owner)
		}
	}
	return out
}

// RequiredRoles lists the roles the sections this file carries demand: a run
// section needs the backup role, a stage section the stage role, a restore
// section the restore role. Diagnostic sections need none of them.
func (c *Config) RequiredRoles() []Role {
	var out []Role
	if c.Run != nil {
		out = append(out, RoleBackup)
	}
	if c.Stage != nil {
		out = append(out, RoleStage)
	}
	if c.Restore != nil {
		out = append(out, RoleRestore)
	}
	return out
}

// CheckRoles reports one error per required role the selection leaves without
// a single active provider. The check is offline: a role is available when a
// compiled plugin providing it survives the selection, never when a command
// happens to run.
func (c *Config) CheckRoles(active Active, roles Roles) []error {
	var errs []error
	for _, role := range c.RequiredRoles() {
		providers := roles[role]
		available := false
		for _, id := range providers {
			if active.Enabled(id) {
				available = true
				break
			}
		}
		if available {
			continue
		}
		section := "/plugins"
		switch role {
		case RoleBackup:
			section = "/run"
		case RoleStage:
			section = "/stage"
		case RoleRestore:
			section = "/restore"
		}
		errs = append(errs, fmt.Errorf("%s: the section needs the %q role, and the plugin selection leaves no active plugin providing it", section, role))
	}
	return errs
}

// Validate runs inside Decode: enum values, ranges, template-independent
// structure. Reference checks between instances live in the assembly, which has
// the registry.
func (c *Config) Validate() error {
	var errs []error
	if c.Plugins != nil {
		errs = append(errs, c.Plugins.validate()...)
	}
	if c.Run != nil {
		r := c.Run
		if r.IDLayout == "" {
			errs = append(errs, errors.New("/run/id_layout is empty"))
		}
		if r.SpoolDir == "" {
			errs = append(errs, errors.New("/run/spool_dir is empty"))
		}
		if r.SpoolReserveBytes < 0 {
			errs = append(errs, fmt.Errorf("/run/spool_reserve_bytes is negative: %d", r.SpoolReserveBytes))
		}
		if r.MaxArtifactBytes < 1 {
			errs = append(errs, fmt.Errorf("/run/max_artifact_bytes must be at least 1, got %d", r.MaxArtifactBytes))
		}
		if r.Deadline.Std() <= 0 {
			errs = append(errs, errors.New("/run/deadline must be positive"))
		}
		if r.FinishAllowance.Std() <= 0 {
			errs = append(errs, errors.New("/run/finish_allowance must be positive: a run that spends its deadline still has to report what it uploaded"))
		}
		if r.MinVerifiedStorages < 1 {
			errs = append(errs, fmt.Errorf("/run/min_verified_storages must be at least 1, got %d", r.MinVerifiedStorages))
		}
		if r.DegradedExitCode < 0 || r.FailedExitCode < 0 {
			errs = append(errs, errors.New("/run exit codes must not be negative"))
		}
		if r.StorageParallelism < 1 {
			errs = append(errs, fmt.Errorf("/run/storage_parallelism must be at least 1, got %d", r.StorageParallelism))
		}
		if r.UnclassifiedError != pluginerrors.ClassTransient && r.UnclassifiedError != pluginerrors.ClassPermanent {
			errs = append(errs, fmt.Errorf("/run/unclassified_error must be \"transient\" or \"permanent\", got %q", r.UnclassifiedError))
		}
		errs = append(errs, validateRetry(r.Retry, "/run/retry")...)
		if r.LeaseWait.Std() < 0 {
			errs = append(errs, fmt.Errorf("/run/lease_wait must not be negative: %s", r.LeaseWait))
		}
		if r.LeasePoll.Std() <= 0 {
			errs = append(errs, fmt.Errorf("/run/lease_poll must be positive: %s", r.LeasePoll))
		}
		if r.LeaseHardBound.Std() <= 0 {
			errs = append(errs, fmt.Errorf("/run/lease_hard_bound must be positive: %s", r.LeaseHardBound))
		}
	}
	if c.Layout.ManualIDSuffix == "" {
		errs = append(errs, errors.New("/layout/manual_id_suffix is empty; a manual run's id would then collide with a scheduled one"))
	} else if strings.ContainsAny(c.Layout.ManualIDSuffix, "/") || strings.Contains(c.Layout.ManualIDSuffix, "..") {
		errs = append(errs, fmt.Errorf("/layout/manual_id_suffix %q must not carry \"/\" or \"..\": it is part of a run id, which is part of every key", c.Layout.ManualIDSuffix))
	}
	if c.Restore != nil {
		r := c.Restore
		if r.SpoolDir == "" {
			errs = append(errs, errors.New("/restore/spool_dir is empty"))
		}
		if r.SpoolReserveBytes < 0 {
			errs = append(errs, fmt.Errorf("/restore/spool_reserve_bytes is negative: %d", r.SpoolReserveBytes))
		}
		if len(r.StorageOrder) == 0 {
			errs = append(errs, errors.New("/restore/storage_order is empty"))
		}
		for i, name := range r.StorageOrder {
			if _, ok := c.Storages[name]; !ok {
				errs = append(errs, fmt.Errorf("/restore/storage_order/%d: unknown storage %q (known: %v)", i, name, sortedKeys(c.Storages)))
			}
		}
		if r.UnclassifiedError != pluginerrors.ClassTransient && r.UnclassifiedError != pluginerrors.ClassPermanent {
			errs = append(errs, fmt.Errorf("/restore/unclassified_error must be \"transient\" or \"permanent\", got %q", r.UnclassifiedError))
		}
		errs = append(errs, validateRetry(r.Retry, "/restore/retry")...)
	}
	if c.Stage != nil {
		if c.Stage.Dir == "" {
			errs = append(errs, errors.New("/stage/dir is empty"))
		}
		if c.Stage.DegradedExitCode < 0 || c.Stage.FailedExitCode < 0 {
			errs = append(errs, errors.New("/stage exit codes must not be negative"))
		}
	}
	errs = append(errs, validateSources(c.Sources, "/sources")...)
	if c.Stage != nil {
		errs = append(errs, validateSources(c.Stage.Sources, "/stage/sources")...)
	}
	for _, name := range sortedKeys(c.Storages) {
		si := c.Storages[name]
		if si.MaxTotalBytes < 0 {
			errs = append(errs, fmt.Errorf("/storages/%s/max_total_bytes must not be negative: %d", name, si.MaxTotalBytes))
		}
		if si.CallParallelism < 1 {
			errs = append(errs, fmt.Errorf("/storages/%s/call_parallelism must be at least 1, got %d", name, si.CallParallelism))
		}
		for kind, rt := range si.Retention {
			if rt == nil || rt.Type == "" {
				errs = append(errs, fmt.Errorf("/storages/%s/retention/%s/type is empty", name, kind))
			}
			if !model.RunKind(kind).Valid() {
				errs = append(errs, fmt.Errorf("/storages/%s/retention/%s: unknown run kind (known: %s, %s)", name, kind, model.RunKindScheduled, model.RunKindManual))
			}
		}
	}
	return errors.Join(errs...)
}

// validate checks the plugins root's own structure the decoder can judge
// without the compiled set: every settings key is a plugin ID and no per-type
// setting is a null. A null is refused here because the value is opaque to the
// decoder's own walk. The selection lists are CheckSelection's, which needs the
// compiled plugin set and the bundle table.
func (p *Plugins) validate() []error {
	var errs []error
	for _, id := range sortedKeys(p.Config) {
		if !ValidID(id) {
			errs = append(errs, fmt.Errorf("/plugins/config/%s: not a plugin ID (a factory kind/type of a known kind, or a stable name); kinds: %v", id, knownKinds()))
			continue
		}
		errs = append(errs, CheckNulls(p.Config[id], "/plugins/config/"+id)...)
	}
	return errs
}

func (k Kind) known() bool {
	for _, known := range kindOrder {
		if k == known {
			return true
		}
	}
	return false
}

func knownKinds() []Kind { return kindOrder }

// ID is the one spelling of a factory identity: kind/type.
func ID(kind Kind, typ string) string { return string(kind) + "/" + typ }

// validateSources checks every source instance of one section: its artifact
// envelope, and its pipeline — a list, whose entries are the processor instance
// names the assembly resolves in New (an empty list is the explicit "no
// processors", never a missing value).
func validateSources(sources map[string]SourceInstance, path string) []error {
	var errs []error
	for _, name := range sortedKeys(sources) {
		s := sources[name]
		errs = append(errs, validateArtifact(s.Artifact, path+"/"+name+"/artifact")...)
		for i, ref := range s.Pipeline {
			if ref == "" {
				errs = append(errs, fmt.Errorf("%s/%s/pipeline/%d is empty; an empty pipeline is written as []", path, name, i))
			}
		}
	}
	return errs
}

func validateRetry(rt RetryConfig, path string) []error {
	var errs []error
	if rt.Attempts < 1 {
		errs = append(errs, fmt.Errorf("%s/attempts must be at least 1, got %d", path, rt.Attempts))
	}
	if rt.InitialDelay.Std() <= 0 {
		errs = append(errs, fmt.Errorf("%s/initial_delay must be positive", path))
	}
	if rt.MaxDelay.Std() < rt.InitialDelay.Std() {
		errs = append(errs, fmt.Errorf("%s/max_delay must not be smaller than initial_delay", path))
	}
	if rt.Jitter < 0 || rt.Jitter >= 1 {
		errs = append(errs, fmt.Errorf("%s/jitter must be in [0, 1), got %v", path, rt.Jitter))
	}
	return errs
}

func validateArtifact(a ArtifactConfig, path string) []error {
	var errs []error
	if a.Group == "" {
		errs = append(errs, fmt.Errorf("%s/group is empty", path))
	}
	if a.Name == "" {
		errs = append(errs, fmt.Errorf("%s/name is empty", path))
	}
	if a.Importance != model.ImportanceRequired && a.Importance != model.ImportanceBestEffort {
		errs = append(errs, fmt.Errorf("%s/importance must be \"required\" or \"best_effort\", got %q", path, a.Importance))
	}
	if a.Sensitivity != model.SensitivityPlain && a.Sensitivity != model.SensitivitySecret {
		errs = append(errs, fmt.Errorf("%s/sensitivity must be \"plain\" or \"secret\", got %q", path, a.Sensitivity))
	}
	return errs
}

// sortedKeys is the one key-order read of this package: the message of every
// diagnostic and the merge walk read a map in the same deterministic order.
func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }

func sortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	slices.Sort(out)
	return out
}

type RunConfig struct {
	// IDLayout is a Go time layout producing the run id, e.g. "20060102T150405Z".
	IDLayout               string             `json:"id_layout" doc:"Go time layout formatting the run id"`
	SpoolDir               string             `json:"spool_dir" required:"true" doc:"directory for artifact buffer files"`
	SpoolReserveBytes      model.ByteSize     `json:"spool_reserve_bytes" required:"true" doc:"free space kept free in the spool before every artifact"`
	MaxArtifactBytes       model.ByteSize     `json:"max_artifact_bytes" required:"true" doc:"largest artifact the run produces; past it the source is stopped and that artifact fails, so one oversized volume cannot fill the spool"`
	Deadline               model.Duration     `json:"deadline" required:"true" doc:"whole-run time limit"`
	FinishAllowance        model.Duration     `json:"finish_allowance" doc:"time the end of a run gets beyond its deadline to write the manifest, finish the catalog entry and send the finished notification; a caller that cancels the run ends it earlier"`
	MinVerifiedStorages    int                `json:"min_verified_storages" doc:"verified copies the run needs to count as delivered"`
	DegradedExitCode       int                `json:"degraded_exit_code" doc:"exit code of the degraded outcome"`
	FailedExitCode         int                `json:"failed_exit_code" doc:"exit code of the failed outcome"`
	StorageParallelism     int                `json:"storage_parallelism" doc:"how many storages upload at once"`
	UnclassifiedError      pluginerrors.Class `json:"unclassified_error" doc:"class applied to an error a plugin did not classify: transient or permanent"`
	NotifierFailureIsFatal bool               `json:"notifier_failure_is_fatal" doc:"a failed finished notification that no notifier delivered makes the outcome failed; a failed started notification only degrades it, and a notifier subscribed to nothing is neither"`
	Retry                  RetryConfig        `json:"retry" doc:"retry policy for every plugin call"`
	// ProgressInterval is how often a running dump reports the bytes it has
	// produced so far. 0 reports none.
	ProgressInterval model.Duration `json:"progress_interval" doc:"how often a running dump reports the bytes it has produced so far; 0 reports none"`
	// LeaseWait bounds how long a scheduled run waits for the writer lease a
	// live run holds before it gives up loudly. A manual run never waits.
	LeaseWait model.Duration `json:"lease_wait" doc:"how long a scheduled run waits for the writer lease before giving up"`
	// LeasePoll is how often a waiting scheduled run asks for the lease again.
	LeasePoll model.Duration `json:"lease_poll" doc:"how often a scheduled run waiting for the writer lease asks again"`
	// HolderNamespaceEnv, HolderPodEnv and HolderUIDEnv name the environment
	// variables carrying this process's identity — the pod namespace, name and
	// UID from the downward API — which the lease records so a leaser can find
	// the holder and report whether it terminated.
	HolderNamespaceEnv string `json:"holder_namespace_env" doc:"name of the environment variable holding this process's pod namespace"`
	HolderPodEnv       string `json:"holder_pod_env" doc:"name of the environment variable holding this process's pod name"`
	HolderUIDEnv       string `json:"holder_uid_env" doc:"name of the environment variable holding this process's pod UID"`
	// LeaseHardBound is the longest any writer can live under the deployment's
	// Job deadline: the run deadline plus its finishing allowance plus the stage
	// container's allowance. It must be set to match activeDeadlineSeconds; it is
	// the only evidence allowed to stand in for a pod object that vanished.
	LeaseHardBound model.Duration `json:"lease_hard_bound" required:"true" doc:"longest any writer can live (run deadline + finish allowance + stage allowance); must match the Job's activeDeadlineSeconds"`
}

type RetryConfig struct {
	Attempts     int            `json:"attempts" doc:"attempts per retry loop (including the first)"`
	InitialDelay model.Duration `json:"initial_delay" doc:"delay before the second attempt"`
	MaxDelay     model.Duration `json:"max_delay" doc:"cap of the doubling delay"`
	Jitter       float64        `json:"jitter" doc:"fraction of the delay randomised (0 to 1)"`
}

// LayoutConfig holds the key templates. Keys are relative to the plugin's own
// root (its config), so every storage keeps the same layout under its own root.
//
// The literal part of RunPrefix and the whole ManifestKey are survey anchors: a
// survey lists the literal prefix and fetches the manifest by rendering its key,
// so renaming either one orphans runs written under the older name. Artifact
// keys and pipeline names carry no such constraint — a manifest records both.
type LayoutConfig struct {
	// RunPrefix must be a literal prefix followed by exactly {{ .Run.ID }}; the
	// literal part is what a retention survey lists.
	RunPrefix   Template `json:"run_prefix" doc:"key prefix of one run, e.g. \"runs/{{ .Run.ID }}\""`
	ArtifactKey Template `json:"artifact_key" doc:"key of one artifact under the run prefix"`
	ManifestKey Template `json:"manifest_key" doc:"key of the run manifest under the run prefix"`
	// ManualIDSuffix is appended to the run id of a manual run. It is a stored
	// format: a survey derives a run's kind from its id, so changing it makes
	// every manual run already written read as a scheduled one.
	ManualIDSuffix string `json:"manual_id_suffix" doc:"suffix appended to a manual run's id, so its kind is derivable from the key"`
}

// StageConfig needs its own exit codes: a stage config has no run section, and
// without a configured code a failing stage could only guess one. Its sources
// are the stage's own — the run never builds them, and a check set follows that
// (see checkScope) — and the staged source of the run side reads back what they
// leave in dir.
type StageConfig struct {
	Dir              string                    `json:"dir" required:"true" doc:"directory sealed artifacts are staged into"`
	DegradedExitCode int                       `json:"degraded_exit_code" doc:"exit code of the degraded outcome"`
	FailedExitCode   int                       `json:"failed_exit_code" doc:"exit code of the failed outcome"`
	Sources          map[string]SourceInstance `json:"sources" optional:"true" doc:"sources the stage command produces, one instance each; the run side reads them back with the staged source"`
}

// RestoreConfig is the restore-only section: a config that only fetches, applies
// or verifies carries no run knobs. Inside it every field is required; the Apply
// options map is optional because fetching needs none.
type RestoreConfig struct {
	SpoolDir          string                    `json:"spool_dir" required:"true" doc:"directory restore paths buffer objects in"`
	SpoolReserveBytes model.ByteSize            `json:"spool_reserve_bytes" required:"true" doc:"free space kept beyond the object being written to the restore spool, checked before the first byte and during every write"`
	StorageOrder      []string                  `json:"storage_order" required:"true" doc:"storages tried in this order on every restore path"`
	UnclassifiedError pluginerrors.Class        `json:"unclassified_error" doc:"class applied to an error a plugin did not classify: transient or permanent"`
	Retry             RetryConfig               `json:"retry" doc:"retry policy for every plugin call on restore paths"`
	Apply             map[string]jsontext.Value `json:"apply" optional:"true" doc:"per source type, the options its restorer decodes"`
}

// Instance is the {type, config} envelope every plugin instance shares.
type Instance struct {
	Type   string         `json:"type" required:"true" doc:"plugin type"`
	Config jsontext.Value `json:"config" doc:"plugin config, decoded by the plugin itself"`
}

// SourceInstance adds the artifact envelope and pipeline a source flows through.
type SourceInstance struct {
	Type     string         `json:"type" required:"true" doc:"plugin type"`
	Artifact ArtifactConfig `json:"artifact" doc:"artifact envelope: what this source produces"`
	// Pipeline is the processor instances this source flows through, in encode
	// order, by name: [zstd] or [zstd, seal]. The names are the config's own
	// instance names; a restore never reads this list, it reads the chain the
	// manifest recorded ([zstd, seal]).
	Pipeline []string `json:"pipeline" required:"true" doc:"names of the processor instances this source flows through, in encode order; [] means the bytes go as the source wrote them"`
	// Config is the plugin's own config: opaque to the assembly, and stated by the
	// deployment unless the type's defaults block creates it whole. The schema decides
	// per type; the defaults check reads it against that type's block.
	Config jsontext.Value `json:"config" doc:"plugin config, decoded by the plugin itself"`
}

type ArtifactConfig struct {
	Group       string            `json:"group" required:"true" doc:"key group, e.g. databases"`
	Name        string            `json:"name" required:"true" doc:"artifact name, unique within a run; may contain /"`
	Importance  model.Importance  `json:"importance" doc:"required or best_effort"`
	Sensitivity model.Sensitivity `json:"sensitivity" required:"true" doc:"plain or secret"`
}

type StorageInstance struct {
	Type string `json:"type" required:"true" doc:"plugin type"`
	// Retention is the policy deciding what this storage keeps, inline and keyed
	// by run kind: {"scheduled": {...}, "manual": {...}}. Each partition sees
	// only runs of its own kind, so a manual run never takes a scheduled slot.
	// A storage with no `manual` partition refuses manual runs. Required for a
	// run; a restore-only config omits it.
	Retention map[string]*Instance `json:"retention" optional:"true" doc:"retention policy of this storage by run kind: {scheduled: {type, config}, manual: {type, config}}; the scheduled partition is required for a run"`
	// MaxTotalBytes is this storage's configured byte cap over every kind at
	// once. A manual run's admission checks it before every object write and
	// refuses on it; a scheduled run never fails on it and frees room instead.
	// Required for a run; a restore-only config omits it.
	MaxTotalBytes model.ByteSize `json:"max_total_bytes" optional:"true" doc:"configured byte cap of this storage over every run kind; a manual run is refused past it, a scheduled run frees room"`
	// CallParallelism is the ceiling a run holds in flight to this storage:
	// the manifest reads of one survey, and the parts of one multipart upload.
	// The default lives in the defaults document; a deployment states it only to
	// raise or lower the ceiling for its own storage.
	CallParallelism int            `json:"call_parallelism" doc:"the most calls a run holds in flight to this storage"`
	TimeBudget      model.Duration `json:"time_budget" required:"true" doc:"time budget for this storage within one run"`
	Config          jsontext.Value `json:"config" doc:"plugin config, decoded by the plugin itself"`
}

// Validate runs inside plugin.Decode: enum values, ranges, template-independent
