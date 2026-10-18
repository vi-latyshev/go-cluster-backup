package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Config is the whole engine config as one JSON document — the JSON form of the
// YAML file a distribution decodes. One file carries every command: a section a
// given command needs is one the file states, and the rest are absent. Sections
// are optional where a file may leave them out (a restore-only file has no run,
// no stage and no storages); inside a present section every field is required
// unless a defaults document fills it.
type Config struct {
	Version int        `json:"version" required:"true" doc:"config format version; this build reads version 1 only"`
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

type RunConfig struct {
	// IDLayout is a Go time layout producing the run id, e.g. "20060102T150405Z".
	IDLayout               string         `json:"id_layout" doc:"Go time layout formatting the run id"`
	SpoolDir               string         `json:"spool_dir" required:"true" doc:"directory for artifact buffer files"`
	SpoolReserveBytes      model.ByteSize `json:"spool_reserve_bytes" required:"true" doc:"free space kept free in the spool before every artifact"`
	MaxArtifactBytes       model.ByteSize `json:"max_artifact_bytes" required:"true" doc:"largest artifact the run produces; past it the source is stopped and that artifact fails, so one oversized volume cannot fill the spool"`
	Deadline               model.Duration `json:"deadline" required:"true" doc:"whole-run time limit"`
	FinishAllowance        model.Duration `json:"finish_allowance" doc:"time the end of a run gets beyond its deadline to write the manifest, finish the catalog entry and send the finished notification; a caller that cancels the run ends it earlier"`
	MinVerifiedStorages    int            `json:"min_verified_storages" doc:"verified copies the run needs to count as delivered"`
	DegradedExitCode       int            `json:"degraded_exit_code" doc:"exit code of the degraded outcome"`
	FailedExitCode         int            `json:"failed_exit_code" doc:"exit code of the failed outcome"`
	StorageParallelism     int            `json:"storage_parallelism" doc:"how many storages upload at once"`
	UnclassifiedError      plugin.Class   `json:"unclassified_error" doc:"class applied to an error a plugin did not classify: transient or permanent"`
	NotifierFailureIsFatal bool           `json:"notifier_failure_is_fatal" doc:"a failed notification makes the outcome failed"`
	Retry                  RetryConfig    `json:"retry" doc:"engine retry policy for every plugin call"`
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
	Attempts     int            `json:"attempts" doc:"attempts per engine retry loop (including the first)"`
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
	RunPrefix   plugin.Template `json:"run_prefix" doc:"key prefix of one run, e.g. \"runs/{{ .Run.ID }}\""`
	ArtifactKey plugin.Template `json:"artifact_key" doc:"key of one artifact under the run prefix"`
	ManifestKey plugin.Template `json:"manifest_key" doc:"key of the run manifest under the run prefix"`
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
	SpoolReserveBytes model.ByteSize            `json:"spool_reserve_bytes" required:"true" doc:"free space kept free before every download"`
	StorageOrder      []string                  `json:"storage_order" required:"true" doc:"storages tried in this order on every restore path"`
	UnclassifiedError plugin.Class              `json:"unclassified_error" doc:"class applied to an error a plugin did not classify: transient or permanent"`
	Retry             RetryConfig               `json:"retry" doc:"engine retry policy for every plugin call on restore paths"`
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
	// Config is the plugin's own config: opaque to the engine, and stated by the
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
	TimeBudget    model.Duration `json:"time_budget" required:"true" doc:"time budget for this storage within one run"`
	Config        jsontext.Value `json:"config" doc:"plugin config, decoded by the plugin itself"`
}

// Validate runs inside plugin.Decode: enum values, ranges, template-independent
// structure. Reference checks between instances live in New, which has the registry.
func (c *Config) Validate() error {
	var errs []error
	if c.Version != 1 {
		errs = append(errs, fmt.Errorf("/version: unsupported config version %d (this build reads 1)", c.Version))
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
		if r.UnclassifiedError != plugin.ClassTransient && r.UnclassifiedError != plugin.ClassPermanent {
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
		if r.UnclassifiedError != plugin.ClassTransient && r.UnclassifiedError != plugin.ClassPermanent {
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

// validateSources checks every source instance of one section: its artifact
// envelope, and its pipeline — a list, whose entries are the processor instance
// names the engine resolves in New (an empty list is the explicit "no
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

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
