// Package model holds the flat data every seam and the engine facade pass around.
// No methods, no host types: a value of this package survives JSON and a future
// out-of-process bridge unchanged.
//
// Every field carries an explicit JSON tag: RunReport is the wire format of the
// run manifest, which lives in a storage for weeks, so its keys are frozen here
// on purpose and a Go field rename must never change them.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	json "encoding/json/v2"
)

// Duration is a Go duration with the one JSON representation json/v2 does not
// provide itself: the string form ("30s", "85m"). It serves both sides of the
// seam — a config field and a report field serialize the same way.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }
func (d Duration) String() string     { return time.Duration(d).String() }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Sensitivity string

const (
	SensitivityPlain  Sensitivity = "plain"
	SensitivitySecret Sensitivity = "secret"
)

type Importance string

const (
	ImportanceRequired   Importance = "required"
	ImportanceBestEffort Importance = "best_effort"
)

type Outcome string

const (
	OutcomeRunning   Outcome = "running"
	OutcomeDelivered Outcome = "delivered"
	OutcomeDegraded  Outcome = "degraded"
	OutcomeFailed    Outcome = "failed"
)

// RunKind is the identity of one run — who asked for it — not a second
// configuration profile: a manual run reads the same file a scheduled one does.
// It is recorded in the manifest (RunReport.Kind) and in the run id itself, so a
// run killed before it wrote a manifest is still identifiable.
type RunKind string

const (
	RunKindScheduled RunKind = "scheduled"
	RunKindManual    RunKind = "manual"
)

// Valid reports whether k is one of the two kinds this build knows.
func (k RunKind) Valid() bool { return k == RunKindScheduled || k == RunKindManual }

// NormalizeKind reads a kind back from a stored manifest: a manifest written
// before the field existed carries no kind and is a scheduled run — the
// owner-approved data-format compatibility — while any other value is returned
// as it is, for the caller to refuse.
func NormalizeKind(k RunKind) RunKind {
	if k == "" {
		return RunKindScheduled
	}
	return k
}

// Artifact outcomes within one run.
const (
	ArtifactOK     = "ok"
	ArtifactFailed = "failed"
)

// Storage outcomes within one run.
const (
	StorageVerified = "verified"
	StorageFailed   = "failed"
	StorageSkipped  = "skipped"
)

// Artifact is one logical unit of a run: one database dump, one cache snapshot,
// one file TREE (a single tar stream), one sealed snapshot. One artifact = one object.
type Artifact struct {
	// Name is unique within a run; it may carry "/" (e.g. "example-prod/postgres").
	Name string `json:"name"`
	// Group organizations artifacts into a key namespace, e.g. "databases".
	Group string `json:"group"`
	// Ext is set by the source and extended by each processor of the pipeline.
	Ext string `json:"ext"`
	// Sensitivity "secret" requires the pipeline to seal (or the source to deliver sealed).
	Sensitivity Sensitivity `json:"sensitivity"`
	Importance  Importance  `json:"importance"`
	// Sealed is true when the bytes are encrypted at rest (source or a processor sealed them).
	Sealed bool `json:"sealed"`
}

// SHA256Hex is the hex digest every artifact identity is expressed in: the
// manifest's sha256, a PutRequest's expected digest, a storage's report.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ObjectInfo is what a storage knows about one stored object.
type ObjectInfo struct {
	Key     string    `json:"key"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	SHA256  string    `json:"sha256"` // "" when the storage does not expose it
	MD5     string    `json:"md5"`
}

// ProcessorStep is one stage of an artifact's actual chain, in encode order, as
// the manifest records it: a restore decodes with exactly this chain reversed and
// never looks the pipeline up in a current config (the owner may have renamed it).
type ProcessorStep struct {
	Instance string `json:"instance"`
	Type     string `json:"type"`
	Ext      string `json:"ext"`
	Seals    bool   `json:"seals"`
}

// SourceRef identifies where an artifact came from: the instance and type that
// produced it, and a non-secret origin identity (a database address, a file path).
type SourceRef struct {
	Instance string `json:"instance"`
	Type     string `json:"type"`
	Origin   string `json:"origin"`
}

// ExclusionKind says why a discovered object deliberately produced no artifact
// of its own.
type ExclusionKind string

const (
	// ExclusionIgnored: the object carries the exclusion marker the run config
	// names, so the decision is the workload's own.
	ExclusionIgnored ExclusionKind = "ignored"
	// ExclusionCovered: another artifact already carries the object's data —
	// a volume a pod's own dump covers.
	ExclusionCovered ExclusionKind = "covered"
)

// Exclusion is one object a run saw and deliberately did not back up: a record
// of a decision, never of a failure. A run manifest carries one entry per
// excluded object, and the nightly message may name the ignored ones.
type Exclusion struct {
	// Name is the artifact name the object's rule would have produced, so a
	// reader can tell two objects of one rule apart.
	Name string `json:"name"`
	// Group is the artifact group of the rule that would have produced the
	// object, and is left out when no rule would have: a claim no rule declared
	// is no artifact the run failed to make, and naming it under the accounting
	// setting's own name would read as one.
	Group string        `json:"group,omitempty"`
	Kind  ExclusionKind `json:"kind"`
	// Reason says why, in the terms the object and the rule supply.
	Reason string `json:"reason"`
}

// DumpStats is what a source reports about one artifact beyond its validity:
// what it left out of the stream and what it wrote short of what it read. The
// engine records it beside the artifact, so it is part of the stored manifest
// and of the nightly message, not only of a log line.
type DumpStats struct {
	// SkippedEntries counts the entries a Dump deliberately left out: an entry
	// kind or a link the artifact cannot carry, a file that vanished under the
	// read. Zero for a source that leaves nothing out.
	SkippedEntries int64 `json:"skipped_entries"`
	// PaddedFiles counts files that shrank while they were read: the artifact
	// carries their recorded length zero-padded, so it is valid but not the
	// tree of one moment.
	PaddedFiles int64 `json:"padded_files"`
}

type ArtifactResult struct {
	Artifact Artifact `json:"artifact"`
	Outcome  string   `json:"outcome"` // ArtifactOK | ArtifactFailed
	// Key is the object key exactly as written to the storages.
	Key string `json:"key"`
	// Chain is the actual processor chain, in encode order.
	Chain []ProcessorStep `json:"chain"`
	// Source identifies the source instance and where it read from.
	Source SourceRef `json:"source"`
	// Pipeline is the config pipeline name, kept for humans; a restore does not
	// read it — it reads Chain.
	Pipeline string `json:"pipeline"`
	RawBytes int64  `json:"raw_bytes"` // bytes the source produced, before processors
	Bytes    int64  `json:"bytes"`     // bytes uploaded
	// Stats is what the source reported beyond the artifact's validity: what it
	// left out, what it padded. Zero for a source that leaves nothing out.
	Stats    DumpStats `json:"stats"`
	SHA256   string    `json:"sha256"`
	Duration Duration  `json:"duration"`
	Error    string    `json:"error"`
	// Hint is the short, actionable half of Error, when the failure has one:
	// what an operator has to do, written for a notification. Empty for a
	// failure with no single action, and a template falls back to Error.
	Hint string `json:"hint,omitempty"`
}

type StorageResult struct {
	Storage  string   `json:"storage"`
	Type     string   `json:"type"`
	Outcome  string   `json:"outcome"` // StorageVerified | StorageFailed | StorageSkipped
	Objects  int      `json:"objects"`
	Bytes    int64    `json:"bytes"`
	Duration Duration `json:"duration"`
	Attempts int      `json:"attempts"`
	// CapExceeded is true when this storage refused an object because the run's
	// own configured byte cap was reached. It is the machine-readable reason a
	// caller reads instead of parsing Error's text: a manual run's cap refusal is
	// the run's own decision, never a storage outage.
	CapExceeded bool `json:"cap_exceeded,omitempty"`
	// CapLimitBytes and CapUsedBytes are the numbers behind a cap refusal: the
	// storage's configured cap and the bytes it held when the refusal fired, so a
	// message can say "cap exceeded (14 GB limit, 13.8 GB used)" without parsing
	// text. Zero on a storage without a configured cap.
	CapLimitBytes int64 `json:"cap_limit_bytes,omitempty"`
	CapUsedBytes  int64 `json:"cap_used_bytes,omitempty"`
	// Retention outcome: how many runs remained, which were dropped, bytes freed.
	Kept    int      `json:"kept"`
	Deleted []string `json:"deleted"`
	Freed   int64    `json:"freed"`
	Error   string   `json:"error"`
}

// ManifestSchema is the schema version every manifest this build writes carries.
// A reader accepts exactly the versions whose runs can still be in rotation;
// today there is one, and any other value is an error.
const ManifestSchema = 1

type RunReport struct {
	// Schema is the manifest format version; see ManifestSchema.
	Schema int    `json:"schema"`
	RunID  string `json:"run_id"`
	// Kind is the run's identity: scheduled or manual. A manifest written before
	// this key existed carries none; see NormalizeKind.
	Kind      RunKind          `json:"kind"`
	StartedAt time.Time        `json:"started_at"`
	Duration  Duration         `json:"duration"`
	Outcome   Outcome          `json:"outcome"`
	Artifacts []ArtifactResult `json:"artifacts"`
	Storages  []StorageResult  `json:"storages"`
	// Excluded lists every object the run saw and deliberately did not back up.
	// It is the record of the opt-out decision: an object that appeared here is
	// accounted for, one that appeared nowhere at all is not.
	Excluded []Exclusion `json:"excluded"`
	Previous PreviousRun `json:"previous"`
	// ExitCode is filled by the engine from the config: 0 for delivered,
	// run.degraded_exit_code for degraded, run.failed_exit_code for failed.
	ExitCode int `json:"exit_code"`
	// Lease is set when the run's writer lease decided something a message has to
	// name: refused at the start, or lost while the run was writing. Absent on a
	// run whose lease was taken and held to the end — the ordinary case — so a
	// template reads its presence, never a guess from outcome and storages.
	Lease *LeaseState `json:"lease,omitempty"`
}

// LeaseState is the machine-readable record of the writer lease's effect on one
// run: refused, because a live claim was held at the start. Holder and HolderKind
// describe the claim that was in the way.
type LeaseState struct {
	// Refused is true when the run never started writing because a live claim
	// was held.
	Refused bool   `json:"refused,omitempty"`
	Holder  string `json:"holder,omitempty"`
	// HolderKind is the kind of the run holding the claim, when it is known.
	HolderKind RunKind `json:"holder_kind,omitempty"`
	// Error is set when the run failed to take the claim rather than refusing it
	// outright: a scheduled run whose bounded wait for a live holder expired. The
	// run never wrote, and this is the reason a message names.
	Error string `json:"error,omitempty"`
}

type PreviousRun struct {
	RunID      string    `json:"run_id"`
	Unfinished bool      `json:"unfinished"` // started and never reported an outcome
	LastGoodAt time.Time `json:"last_good_at"`
	// Failure describes why the previous Job of this run's own CronJob failed
	// before this tool could report — the failures a catalog never sees (a
	// rejected initContainer, ImagePullBackOff, a PSA refusal, a deadline, an
	// OOMKill). Nil when the previous Job succeeded or there is none.
	Failure *PreviousFailure `json:"failure,omitempty"`
	// Unavailable is set when the previous Job's status could not be read — a
	// missing RBAC grant, no identity. It is visible, and it never fails a run.
	Unavailable string `json:"unavailable,omitempty"`
}

// PreviousFailure is why a previous Job failed before the tool ran, read from
// the cluster: the Job's own condition and the per-container reasons. It is
// readable data only, and it is what a message renders when a night died before
// its first log line.
type PreviousFailure struct {
	Job string `json:"job"`
	// Reason and Message are the Job's Failed condition — BackoffLimitExceeded,
	// DeadlineExceeded, FailedCreate and the message the controller wrote.
	Reason     string             `json:"reason,omitempty"`
	Message    string             `json:"message,omitempty"`
	Containers []ContainerFailure `json:"containers,omitempty"`
	// Events are the Job's own Warning events, newest first. They are the only
	// record when no Pod was ever created — a Pod Security admission refusal
	// (FailedCreate) leaves an Event and no container status at all.
	Events []EventFailure `json:"events,omitempty"`
}

// EventFailure is one Warning event recorded against the failed Job.
type EventFailure struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// ContainerFailure is one container of the failed Job that explains it: what it
// terminated with (reason, exit code, and the message a
// FallbackToLogsOnError policy filled with the last log lines), or why it never
// started (ImagePullBackOff, CreateContainerConfigError).
type ContainerFailure struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
	// Init is true for an init container: its failure is why the main container
	// never ran.
	Init       bool                  `json:"init,omitempty"`
	Terminated *ContainerTermination `json:"terminated,omitempty"`
	Waiting    string                `json:"waiting,omitempty"`
}

// ContainerTermination is a container that ran and stopped.
type ContainerTermination struct {
	Reason   string `json:"reason"`
	ExitCode int    `json:"exit_code"`
	Message  string `json:"message,omitempty"`
}

// The two events the engine delivers to every notifier: one as a run starts,
// one as it ends. They are part of the contract a notifier subscribes to — a
// consumer names them in its own config — so they live here and not in the
// engine.
const (
	EventStarted  = "run_started"
	EventFinished = "run_finished"
)

type Event struct {
	Name string    `json:"name"` // EventStarted | EventFinished
	At   time.Time `json:"at"`
	Run  RunReport `json:"run"`
}

// StoredRun is what a retention policy decides about: one run as present on ONE storage.
type StoredRun struct {
	RunID string `json:"run_id"`
	// Kind is the run's identity, read from the run id and agreeing with its
	// manifest: retention partitions by it, so a manual run never takes a
	// scheduled slot and a manual copy never releases a scheduled pin.
	Kind  RunKind   `json:"kind"`
	At    time.Time `json:"at"`
	Bytes int64     `json:"bytes"`
	// Verified is true when the run's manifest on this storage reports this
	// storage's outcome as "verified".
	Verified bool `json:"verified"`
	// Manifest is true when this storage holds the run's manifest and this build
	// read it: what separates a run retention judges by its own report from the
	// objects of a run that was killed before it wrote one.
	Manifest bool `json:"manifest"`
}
