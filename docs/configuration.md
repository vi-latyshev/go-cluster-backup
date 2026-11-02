# Configuration

`go-cluster-backup` reads one YAML document. `run`, `stage` and the `restore`
subcommands read the same file. Every command builds every declared instance at
startup, so a broken section stops it even when that command does not use it;
only the **checks** are scoped: `run` checks everything except `stage.sources`,
`stage` its own sources and the processors their pipelines name, and `restore`
resolves its storages and apply options on use. The chart mounts the file at
`/etc/go-cluster-backup/config.yaml`.

Reference configurations:

- [examples/config/minimal.yaml](../examples/config/minimal.yaml) — Postgres by
  label, one S3-compatible storage.
- [examples/config/k3s-single-node.yaml](../examples/config/k3s-single-node.yaml)
  — adds the k3s datastore stage and file volumes.
- `cmd/go-cluster-backup/testdata/config.yaml` — every field of every plugin,
  validated by a test.

## Concepts

### Kind

A seam: `source`, `processor`, `storage`, `retention`, `discovery`, `catalog`,
`notifier`. A kind's interface grows only by a NEW optional interface; a
capability is discovered by calling (`errors.ErrUnsupported`), never by widening
an interface that already has implementations.

### Instance

One configured plugin: the kind's envelope plus `{type, config}`. Every field of
an instance is required — the file or a defaults document states it, and no
plugin supplies a value — and an unknown member is an error with its
JSON-pointer path.

### Defaults

The fields no deployment may choose (a credential, a storage's root, a retention
tier or byte budget, a message text) are the deployment's to state, and every
other field has one value in the service's own document,
`internal/defaults/defaults.yaml`, embedded into the binary. The engine merges
it under the configuration file before a byte reaches a plugin: objects merge
key by key, a value the file carries replaces the default whole, `name[]` fills
the elements of the list `name` the file declares, `null` is refused, and a
default never creates an optional section, an instance or a list element. A
plugin still receives complete bytes and treats every field as required.
`config defaults` prints the document; `config effective --config FILE` prints
the merged file with every defaulted value marked.

### Size units

A whole number of bytes, or digits with a unit: `KB`, `MB`, `GB`, `TB` are
powers of 1000; `KiB`, `MiB`, `GiB`, `TiB` powers of 1024. Nothing else decodes:
a bare `"1024"` and a lowercase `5gb` are refused by name.

### Pipeline

An ordered list of processor instances an artifact flows through. Processors are
stream wrappers: `zstd` compresses, `age` seals. An artifact with
`sensitivity: secret` whose pipeline does not seal (and whose source does not
deliver sealed bytes) is refused before the first byte. A source whose bytes
arrive already encoded — `staged`, reading what `stage` left — declares no
pipeline: it carries the chain of the process that produced its bytes, and the
manifest records that chain.

### Artifact

One logical unit: a database dump, a cache snapshot, a file tree as one tar
stream, a sealed snapshot. One artifact is one object;
`importance: required|best_effort` decides what its failure means to the run.

### Unit

One artifact of an instance whose domain holds several (a database server's
databases). Such a source answers `source.Enumerator` with each unit's name and
the config that dumps exactly it; the engine builds one source per unit of the
same type and names the artifact `<envelope>/<unit>` and the instance
`<instance>/<unit>`. A name the engine cannot build a name from (empty, a `/`, a
control character) is that unit's own failed artifact, and its siblings still
run; a unit whose source cannot be built or checked fails alone too. A source
without the capability, or one answering `errors.ErrUnsupported`, is one
artifact. An enumeration that fails, or answers nothing, is ONE failed artifact
carrying the instance's envelope — never a run that silently produces nothing.

### Coverage

A discovery decides about every object it sees, exclusively. A rule renders an
object into an artifact; an object the exclusion marker names, or one another
artifact already carries (a volume the dump of the pod mounting it covers),
becomes an **exclusion** — recorded in the report and the manifest, nothing
built for it, carrying the artifact group of the rule that would have produced
it and leaving the group out when no rule would have. A claim no rule accounted
for is ONE failed artifact under the `undeclared_volumes` setting — never a
silent absence and never a raw copy of a live database directory. The verdicts
are tried in this order: the marker, a binding that is gone (`Lost`), the pod
mounting the claim, the undeclared accounting, then the render, so every claim
is exactly one of covered, excluded, declared or reported; a claim still
`Pending` (its volume is provisioned when a pod first uses it) is legitimately
nothing. Covered and ignored objects are counted in the discovery line and each
named only at `--log-level debug`. Whether an object is excluded is the
deployment's decision, written in its marker or its rules — the tool reports an
undeclared volume as a failure and never decides for the deployment that it may
be dropped.

### Layout

Key templates. Artifacts land under `runs/<run id>/<group>/<name><ext>`, the
manifest at `manifest.json` beside them. The literal head of
`layout.run_prefix` and the whole `layout.manifest_key` are survey anchors:
renaming either orphans every run already written. A manual run's id is the
scheduled one plus `layout.manual_id_suffix` (default `-manual`), so a survey
tells a run's kind from its key alone, and a manifest whose `kind` disagrees
with it is a problem run the engine never deletes.

### Manifest

The run report as the last object of the run, carrying `schema`. A restore
decodes with the chain the manifest records, never with a pipeline looked up in
the current config. **The manifest's JSON keys and its schema version are a
stored format: they never change.**

### Run outcome

A run that returns a report ends with exactly one outcome, and it decides the
exit code. The verdict reads the writer lease, every storage's verification,
every artifact's result and the started notification, and nothing else;
`Engine.reportOutcome` evaluates them in this order, and the first rule that
holds wins:

1. `failed` — the writer lease decided the run: a manual run refused while a live
   holder held the claim, or a scheduled run whose acquisition failed (the wait
   expired or the error was permanent). A catalog that cannot hold a lease leaves
   a scheduled run lease-free (§ One writer); the rules below then apply without
   a lease. Nothing was uploaded.
2. `failed` — a required artifact is not `ok`, or fewer than
   `run.min_verified_storages` storages are verified. A run that produced no
   artifact verifies no storage, so it fails here too.
3. `degraded` — one of the other signals failed: a best-effort artifact that is
   not `ok`, a storage that is not verified, or the started notification when
   `run.notifier_failure_is_fatal` is true.
4. `delivered` — none of them failed.

The run's own last event can still lower that verdict: with
`run.notifier_failure_is_fatal`, the outcome becomes `failed` only when **no**
notifier delivered the finished event — one notifier failing while another
delivered the same event leaves the verdict as it stood, so a delivered finished
message always names the final outcome (`Engine.Run`, after `reportOutcome`). A
notifier that skips an event — an unsubscribed one returns `notifier.ErrSkipped`
— counts as neither delivered nor failed, so a started-only or muted webhook
cannot mask another notifier's failed finished delivery. The started notification
is not symmetric — its failure is one more failure, so with the flag it degrades
a run that would otherwise be delivered, and without the flag either event's
failure changes nothing. The verdict reads no other input: a failed retention
pass, catalog write, lease release or source close is recorded in the report or
the log and leaves the outcome alone.

A storage is verified (`Engine.assembleResults`) when the run tried it, it holds
at least one object of this run, and nothing failed on it: no upload, no
verification, no cap refusal and no manifest write or verification. The manifest
is part of the verdict — the run's manifest must be saved and verified on that
storage, because a copy no manifest records is a copy a restore cannot find. A
storage the run never tried is `skipped`, and an unverified storage counts
against the outcome like any other failure.

Exit codes: `delivered` is 0, `degraded` is `run.degraded_exit_code`, `failed` is
`run.failed_exit_code` (`0` and `1` by default). A run that returns an error
instead of a report has no outcome and exits 1 — a config error, a missing
binary, an unusable spool, a run id a storage already holds, or a manual run that
could not reach the claim at all: its catalog cannot hold a lease, or the
acquisition failed for a reason other than a live holder.

The manifest is written before the final verdict is recomputed and records the
report as it stood at that moment. It can name a storage verified whose own
manifest write then fails; that later failure can only make the final outcome
worse, never better — the exit code follows it, and can rise from `delivered`'s
0 to `run.failed_exit_code`. The manifest is also written before the finished
event, so with `run.notifier_failure_is_fatal` and a finished event no notifier
received, the stored manifest can name `delivered` or `degraded` while the
catalog entry and the exit code say `failed`. A run refused the writer lease
writes no manifest. The report the command prints, and the catalog entry where
one is configured, carry the final verdict.

`--dry-run` decides from the plans alone: `delivered` (exit 0) when every planned
artifact resolved — its check, traits, recorded chain and the secret invariant —
and `failed` (`run.failed_exit_code`) otherwise, a best-effort failure included.
Nothing is produced, uploaded, notified or recorded; the storage results are
`skipped`, or `failed` where its check or its describe already failed, and
neither changes the verdict.

A best-effort artifact's failed upload costs its storage the verification of the
whole storage, so with one storage it can turn `delivered` into `failed` — the
place to look when every required artifact reads `ok`. A manual run's cap
refusal fails one storage like any other, so it is `degraded` while the remaining
verified storages still meet `run.min_verified_storages` and `failed` when they
do not — never a silent success.

### Message

The `webhook` notifier's texts are config, not the plugin's. `events` maps an
event name to a plain-text template; the engine's events are `run_started` and
`run_finished`, and an event with no entry is not subscribed — the notifier
answers `notifier.ErrSkipped`, which counts as neither a delivery nor a failure.
A template renders
into lines: an empty line is dropped (an action-only line needs no trim marker),
`{{ blank }}` keeps a deliberate empty one, the first line left is the title,
and a pair of literal backticks marks a line's monospace part, which must end
its line. The backticks are read from the template's own parse tree, so a
backtick that arrives from data is never markup. Data from the cluster is
cleaned before any template sees it: invalid UTF-8 and control characters become
a replacement rune and line breaks inside a value become spaces, because a
broken name must not cost the night's one message. Sizes have two helpers:
`bytes` renders binary units (KiB, MiB, GiB) for an artifact's stored size,
`sibytes` renders SI units (base 1000, one decimal — kB, MB, GB) for a cap
stated in `GB`; a cap stated in `GiB` is rendered by `bytes`. The engine's own
`cap_exceeded` text carries the exact byte count in binary units, never a
rounded SI reading.
`payload` is one body template for **every** event, executed with `.Title`,
`.Lines` (each `.Text` and `.Code`), `.Text` (the whole message) and `.Event`;
its keys are the receiver's own. `body_format: yaml` drops the empty rendered
lines, reads the rest as a YAML document and sends its JSON encoding; `text`
sends the render as it is. `validate` renders every event and the payload
against a matrix of built-in sample runs — started with and without an
unfinished previous run; finished delivered, degraded and failed, with and
without exclusions, artifacts and storages, both run kinds, a manual run refused
by a cap and one refused the writer lease — so a template that only fits one
night is refused offline. `.Run.Previous` carries the previous run's record,
including a failure from before the tool started (`.Run.Previous.Failure`); a
storage's `CapExceeded`, `CapLimitBytes` and `CapUsedBytes` and a lease refusal
(`.Run.Lease`) reach a template too.

### Run knobs and budgeting

| Key | What it bounds |
|---|---|
| `run.deadline` | the whole run: the context every artifact and storage call runs under |
| `run.finish_allowance` | the ending beyond that deadline; the manifests take its first half and are written together, so a storage that stalls on its own manifest costs only its own record, while the finished notification and the catalog entry keep the second half |
| `run.max_artifact_bytes` | one artifact: a source that would produce more is stopped and that artifact fails, so one oversized volume cannot fill the spool |
| `run.spool_reserve_bytes` | free space kept free in the spool; below it an artifact fails rather than filling the filesystem |
| `storages.<name>.time_budget` | the time spent inside that storage's own calls in one run — uploads, listings, deletes, the survey — never a source's dump or a processor's compression |
| `run.storage_parallelism` | how many storages upload at once; with `1` the per-storage budgets add up against the same wall clock, so `run.deadline` must sit above their sum plus production |
| `run.lease_wait`, `run.lease_poll` | how long a scheduled run waits for a live writer lease, and how often it asks again; a manual run never waits |
| `run.retry`, `run.unclassified_error` | the engine's retry policy for every plugin call, including the class an error a plugin did not classify gets |
| `run.notifier_failure_is_fatal` | with the flag, a finished event no notifier delivered makes the outcome `failed`; a failed started notification only degrades it, a notifier that skips an event counts as neither delivered nor failed, and with the flag off neither event's failure changes the outcome |
| `run.progress_interval` | how often a running dump reports its bytes so far; 0 reports none |

An object goes to a storage in one request when it fits one, or part by part
through a storage that uploads in parts: the engine sends one part per call, so
a failed part is re-sent and the parts already accepted are not, and an upload
that keeps failing is aborted rather than left billed — on a context of its own,
bounded by a tenth of the storage's `request_timeout`, because an abort fires
exactly when the run's ending needs its seconds. A run starts by sweeping what a
killed predecessor left behind: every incomplete multipart upload under the
storage's own prefix, begun before `now − (run.deadline + run.finish_allowance)`
— older than the longest a live run can last, so an upload an overlapping run is
still writing is never taken — is aborted once per storage per run, and a sweep
that fails leaves the storage's occupancy unknown instead of guessing it.

### One writer

Runs that share storage roots are serialised by a cross-process writer lease the
catalog owns: an optional capability, absent as `errors.ErrUnsupported`. The
engine takes it before the first storage write — the stale sweep included — and
releases it at the end. A deployment's `concurrencyPolicy` governs only one
CronJob's own Jobs, so it does not stop a scheduled run and a hand-started one
from overlapping; the lease does. The lease is taken by `run`, after `stage`:
a `stage.dir` shared between pods — a hostPath or a PVC — lets another pod's
stage republish a staged pair between this pod's stage and its run,
which the lease cannot prevent, so keep `stage.dir` on the pod's own volume.

The claim records the holder's run id and kind, its acquisition token, and the
process identity the deployment fills from the downward API through the
environment variables `run.holder_namespace_env`, `run.holder_pod_env` and
`run.holder_uid_env` — the pod namespace, name and UID. A held claim is taken
over only on a fact that holds on this deployment:

- the kubelet's own terminal phase (`Succeeded`/`Failed`) of the holder's exact
  pod, with the recorded UID — a **scheduled** run takes over at once;
- the pod object absent or under a different UID **and** the claim older than
  `run.lease_hard_bound`, the longest any writer can live under the Job's
  `activeDeadlineSeconds` — either kind takes over.

Never on a clock alone. An absent pod alone proves nothing (a forced deletion
removes the object while the process may still run); a running or pending pod, a
partial identity, an absent capability, `errors.ErrUnsupported` and a failed
check all count as alive. A manual run refuses a live claim at once; a scheduled
run waits `run.lease_wait` and then gives up. Both end through the common finish:
the outcome is `failed`, the reason is in the report (`.Run.Lease` names the
holder on a refusal), the finished notification is sent and the catalog entry is
written. A catalog that cannot hold a lease at all leaves a manual run unable to
start (exit 1) and a scheduled run lease-free. `Release` drops only the claim
its own acquisition token names.

A takeover proves the holder's process ended, not that every request it had
already handed to a provider finished: Yandex Disk answers an upload with HTTP
202 before its asynchronous commit, and a delete the holder had already decided
may still be applied. The answer is bounded and deliberate. After a takeover
under the kubelet's phase the lease records a quarantine until the dead holder's
hard bound passes; `Release` keeps it, every later acquisition inherits the
latest one, and while it holds the successor treats occupancy as unknown — a
manual run is refused on every storage, a scheduled run is soft-capped anyway.
Retention never deletes another run's incomplete objects younger than the same
`deadline + finish_allowance` bound. The worst a late provider-side commit can
do is overshoot the scheduled cap transiently, or apply that holder's own
already-decided deletes.

### Retention

Retention is per storage, decided from the manifests that storage itself holds,
and **partitioned by run kind**: `storages.<name>.retention` is a map,
`{scheduled: {…}, manual: {…}}`; the engine hands each policy a homogeneous run
list, so a manual run never takes a scheduled slot and a policy never sees the
other kind. A storage with no `manual` partition refuses manual runs.

| Policy | Keeps |
|---|---|
| `gfs` | the newest run of each of the newest `daily` calendar days, `weekly` ISO weeks and `monthly` calendar months, in `timezone`, plus a floor of the newest `min_keep` runs |
| `keep_last` | the newest `count` runs, with a floor of the newest `min_keep` |

A **verified** copy is one whose run outcome is not `failed`, whose upload to
that storage verified, and whose manifest is readable. Within one day, week or
month the slot goes to the run this build can judge best: its newest verified
run, then one whose manifest it read, then the objects of a run that never wrote
one. `count_incomplete` decides whether a run without a verified manifest takes
a slot at all: with `true` it can take a period's slot when the period has
nothing better; with `false` such runs are kept in addition, outside the tiers.
A manifest whose run id and `kind` disagree is a problem run: it is never
deleted, because the survey cannot trust what it says about itself.

Two floors outrank every policy. On each storage, the newest run of a kind
holding a verified copy of an artifact there — identity `group/name`, with that
storage's own listing as the evidence — is never dropped, so a manual copy never
releases a scheduled pin; a manual run gets no pin beyond being the storage's
only verified copy of the artifact, so keep-last N and the scheduled run's
eviction can still reclaim it. Across everything surveyed, at least one verified
complete run stays. A name that stops appearing — a renamed or vanished
artifact, or one that became an exclusion — keeps its last run for as long as
the name is absent. The Yandex Disk storage removes the folders its deletes
empty, up to but never including the configured root.

The **byte cap** is the storage's `max_total_bytes`, owned by the engine, not the
policy, and covering every kind at once. Before every object write on a storage
— artifacts and the manifest — a manual run checks it against that storage's
real occupancy (its listing plus what the run has written or is writing); a
refusal stops uploads to **that** storage with the machine-readable
`cap_exceeded` reason and the storage's `cap_limit_bytes` and `cap_used_bytes`,
and deletes nothing. An unknown occupancy — a failed survey, a foreign multipart
upload nobody can count — is never free. A scheduled run never fails on the cap:
after its manifest is saved and verified it frees room in this order — an
inactive incomplete attempt first, then the oldest manual runs, then its own
history by the byte budget — and a leftover overflow, where the floor protects
more than the cap allows, is reported without failing the run.

## The log

Every step — the run starting and the storages it will write to, the previous
run, what the discovery found and how many objects it covered or ignored, each
artifact's source, each processor step with its bytes in and out, the spool,
every storage's upload, verification and retention, the catalog write, each
notification and the closing summary — is one line written when it happens, so
`kubectl logs -f` shows a run live. A line opens with a `[HH:MM:SS]` clock (UTC)
and, for anything a plugin did, the instance that did it:
`source postgres/app: dumping …`, `processor zstd/squeeze: 163.7 MiB → 48.2 MiB
(3.4x) in 1.2s`, `storage s3/offsite: ✓ …`. Sizes are human (`629.8 MiB`),
durations rounded (`2m40s`), and a failure is one `✗` line naming the artifact
and the reason.

The engine and every plugin report through the one `*slog.Logger` the engine
hands each instance; nothing writes a log line with a `Printf`, and a plugin
never writes to stdout. `--log-level debug` is where the machine detail lives:
the sha256 and the full object key are appended to the line, and each excluded
object is a debug-only line of its own. The exact values are always in the
manifest and the catalog, whatever the level. `stage` and the `restore`
subcommands report the same way; `restore list` stays a listing, one row per run
and storage, with no clock.

## Commands

`--config` is required everywhere it appears. No flag that changes what a
command does has a default: `--log-level` and `--output` only choose how a
command reports itself. A missing or invalid flag is a usage error naming it,
exit code 2; anything else that fails exits 1; `run` and `stage` exit with the
code their config names.

```
go-cluster-backup run     --config FILE --kind scheduled|manual [--dry-run]
go-cluster-backup stage   --config FILE
go-cluster-backup validate --config FILE [--preflight] [--instance KIND/NAME]...
go-cluster-backup config  defaults
go-cluster-backup config  effective --config FILE [--output yaml|json]
go-cluster-backup plugins [describe KIND/TYPE]
go-cluster-backup schema
go-cluster-backup doctor
go-cluster-backup restore list   --config FILE [--storage NAME]... [--run ID] [--output text|json]
go-cluster-backup restore fetch  --config FILE (--run ID | --run newest-ok) --artifact NAME --out PATH|-
                                 [--storage NAME]... [--raw] [--plaintext-to-file]
go-cluster-backup restore fetch  --config FILE --storage NAME --key KEY --out PATH|-
go-cluster-backup restore apply  --config FILE (--run ID | --run newest-ok) --artifact NAME
                                 --target discovered|NAME --mode side-by-side|in-place
                                 [--confirm ORIGIN] [--allow-foreign-origin]
go-cluster-backup restore verify --config FILE (--run ID | --run newest-ok) [--artifact NAME]
                                 --depth stored|decoded|probed [--target NAME|discovered]
```

- **run** — the whole backup. `--kind` names the run's identity (`scheduled` or
  `manual`) and has no default; it is recorded in the run id and the manifest,
  and retention partitions by it. `--dry-run` resolves discovery, pipelines and
  the secret invariant, reports the artifacts and exclusions the night would
  have, and writes nothing.
- **stage** — produces the sealed artifacts the run side reads back with the
  `staged` source; the file name is the identity, which is why a stage config
  has no run section and carries its own exit codes. Beside each artifact the
  stage publishes its chain record, so the manifest carries how the bytes were
  encoded and a restore decodes with exactly that. The stage's contract is
  admission, not cleanup: it publishes into an empty directory it owns and never
  deletes a file. When `stage.dir` holds any entry as the stage starts — a pair a
  previous stage left, an orphan, a stale temp, a subdirectory — the stage
  refuses before any source runs and exits `1`, whatever `stage.failed_exit_code`
  says, because a pair a previous stage left would read exactly like this one's
  and the run would upload yesterday's bytes as tonight's. A stage re-run on data
  a pod retained (a hostPath or a PVC that did not start empty) is refused the
  same way, fail-closed by design. Within one stage, an artifact whose file or
  chain-record name another artifact of the stage already claimed fails before it
  produces bytes, instead of replacing the other's pair. An ordinary artifact
  failure exits `stage.degraded_exit_code` for a best-effort artifact and
  `stage.failed_exit_code` for a required one. As a Job's initContainer a
  non-zero exit would fail the pod before the main container starts, so a
  deployment that wants the run to continue with a failed control-plane artifact
  sets that code to `0`; the stage logs a `✗ <artifact>: <reason>` line, and the
  run's `staged` source then fails its own check, failing exactly that artifact
  while every other artifact is still produced and uploaded. `stage.dir` must be
  an emptyDir of the one pod that runs both commands — the chart's `spool`, or
  one declared in `writer.volumes` — never a hostPath or a PVC shared between
  pods: such a directory holds the previous pair, which the stage refuses, and a
  directory shared between pods lets another pod's stage republish the pair
  between this pod's stage and its run.
- **validate** — offline: structure, types, templates, every reference between
  instances, every error at once with its path. It succeeds on a config whose
  secret files do not exist: a secret is a reference, and only a plugin's `Check`
  reads it. `--preflight` adds those checks for every declared instance and for
  every source a discovery finds, prints one line per instance — `ok`, `excluded
  (kind): reason` for an object a discovery deliberately does not back up, or
  `FAILED` with the reason — then the count of checked instances and a separate
  count of excluded objects, and exits non-zero on any failure. `--instance
  KIND/NAME` restricts it, for a pod that cannot reach every instance; a named
  discovery brings the sources it finds with it, the summary counts only what was
  checked, and a line names every configured instance the selection left out.
  Offline validation always reads the whole file.
- **config defaults / effective** — the embedded document as it is; the merged
  file with every defaulted value marked.
- **plugins** — kind/type, stability, required binaries and summary;
  `plugins describe KIND/TYPE` adds every field and the plugin's example.
- **schema** — the JSON Schema of a configuration file.
- **doctor** — every external binary any registered plugin declares is on `PATH`.
- **restore** — see [restore.md](restore.md).

## Template surface

Templates read Go field names of the report types and a fixed function set. That
surface is a SemVer API for this project: a deployment's configuration spells
those names, so a rename is a breaking change. It is pinned by tests
(`internal/plugin/template_surface_test.go`, `internal/engine/layout_surface_test.go`).

- **Fields:** `RunReport`, `ArtifactResult`, `Artifact`, `StorageResult`,
  `DumpStats`, `Exclusion`, `PreviousRun`, `PreviousFailure`, `ContainerFailure`,
  `ContainerTermination`, `LeaseState`, `Event`; and the layout data
  (`Run.ID`, `Run.StartedAt`, `Artifact.Group`, `Artifact.Name`, `Artifact.Ext`).
- **Functions:** the core set — `json`, `date`, `bytes`, `sibytes`, `duration`,
  `pick` — plus the notifier's own (`blank`, `byGroup`, `failed`, `sumBytes`,
  `pickdef`, `clip`) and the discovery's own (`label`, `envValue`, `envFile`,
  `port`, `volumePath`).

## Adding a plugin

See [CONTRIBUTING.md](../CONTRIBUTING.md). A plugin is one package under
`internal/plugins/<kind>/<name>` plus one registration line in
`internal/cli/register.go`.
