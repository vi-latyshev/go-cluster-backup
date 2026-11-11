# Configuration

`go-cluster-backup` reads one YAML document with no `version` key. `run`, `stage`
and `restore` share it. Every present section must validate, even when the
command does not use it. Runtime **checks** are scoped: `run` checks everything
except `stage.sources`, `stage` its sources and their processors, and `restore`
resolves storages and apply options on use. The chart mounts the file at
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

Deployment-specific fields — credentials, storage roots, retention tiers/byte
budgets and message text — must be supplied by the deployment. Other fields
come from `internal/plugins/configuration/defaults/defaults.yaml`, embedded in
the binary. Objects merge recursively; present scalars and lists replace earlier
values. `name[]` fills fields in declared list elements. Null is refused, and
defaults never create optional sections, instances or list elements. Plugins
receive complete config bytes and supply no defaults themselves.

`config defaults` prints the embedded document. `config effective --config FILE`
marks only actually defaulted values: YAML uses `# default`; JSON includes their
provenance pointers. Pointers escape IDs per RFC 6901: `source/postgres` becomes
`source~1postgres`, for example `/plugins/config/source~1postgres/format`.

### Plugins

Defaults and deployment config use the same root:
`plugins: {bundles, disabled, config}`. The `config` map holds partial settings
under factory IDs (`kind/type`) or application-provider IDs such as `reporting`.
Backup sections remain in this document. For example, this deployment fragment
sets type-wide compression settings and then overrides one instance:

```yaml
plugins:
  bundles: []
  disabled: []
  config:
    processor/zstd:
      concurrency: 2
    reporting:
      request_timeout: 10s
processors:
  compress:
    type: zstd
    config:
      concurrency: 3
```

Precedence is embedded defaults, deployment per-type settings, then
`instance.config`; the instance above receives `concurrency: 3`. Nested discovery
sources and retention use the same resolver. Required fields may be supplied
per-type or per-instance by the deployment, never embedded defaults. Settings do
not create instances or discovery results. Partial blocks are checked against
compiled schemas even when the plugin is disabled; full required-field and
combination checks follow assembly. Secret references are allowed, not printed
secret contents.

All compiled plugins start available. Bundles apply in list order, then
`plugins.disabled`; deployment lists replace default lists. `bundles: []` clears
bundles. Re-enable a plugin by removing its disable and any disabling bundle.
Floor includes `cli`, `workflow-backup` and the dependency closure of each;
these cannot be disabled. The flat Floor host lives in `internal/plugins/cli`
and registers only through the generated root. Library plugins are always active. Unknown IDs/bundles/members,
duplicate keys, nulls and invalid types are errors. Referencing a disabled role,
instance type or nested discovery source refuses operation before I/O.

The generic container is `internal/loader`; registration is generated from plugin
metadata. `assembly` is the construction boundary for Config and supplies typed
options to `workflow/backup`, `workflow/stage`, `workflow/restore` and their
collaborators. `plugins check` validates the compiled composition offline. `validate`
and `config effective` aggregate configuration, availability and dependency
diagnostics. Effective config prints a mergeable candidate on stdout, diagnostics
on stderr and exits non-zero on errors; unreadable or unmergeable input has no
candidate. A diagnostic candidate never permits operational execution.

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

### Sizing

The processor knobs trade CPU, memory and stored size, and nothing else here
costs as much as a badly chosen `zstd` window. These are measured numbers, not
estimates: the tool on one CPU, window 8 MiB, PostgreSQL 18 client, median of
three runs — stored size, the tool's own CPU seconds and wall seconds while
producing, and peak RSS. The wall includes `pg_dump`, so it moves with the
server as much as with the level.

| Corpus | Compressor | Stored MiB | CPU s | Wall s | RSS MiB |
|---|---|---|---|---|---|
| synthetic, 3.6 GiB, 8M-row COPY text | `fastest` | 885.6 | 13.46 | 14.37 | 36.1 |
| | `default` | 932.9 | 16.54 | 17.42 | 36.1 |
| | `better` | 766.6 | 21.76 | 22.28 | 40.1 |
| | native `zstd:1` | 863.4 | 8.51 | 9.09 | 16.8 |
| | native `zstd:6` | 882.4 | 30.16 | 30.67 | 17.0 |
| real, 2.1 GiB | `fastest` | 38.5 | 4.42 | 8.53 | 34.1 |
| | `default` | 36.4 | 4.01 | 8.12 | 37.9 |
| | `better` | 30.0 | 5.24 | 8.32 | 40.0 |
| | native `zstd:1` | 64.4 | 2.73 | 8.14 | 18.9 |
| | native `zstd:6` | 39.7 | 4.35 | 8.33 | 16.8 |
| real, 161 MiB | `fastest` | 136.5 | 0.54 | 0.82 | 36.0 |
| | `default` | 72.1 | 0.98 | 1.23 | 36.0 |
| | `better` | 70.6 | 1.24 | 1.43 | 40.1 |
| | native `zstd:1` | 70.6 | 0.42 | 0.62 | 16.8 |
| | native `zstd:6` | 74.7 | 1.91 | 2.04 | 16.8 |

`fastest` and `default` are different heuristics, not neighbouring points on
one scale: on the 161 MiB dump `fastest` produced about twice the bytes of
`default`, on the synthetic one fewer. `better` was smallest on every corpus,
at 1.3 to 2.3 times the cheapest level's CPU. The corpus reorders the middle
rows, so measure your own dump the same way — compress the same raw file at
each level and compare size and CPU — before changing a level.

- **`window_bytes`** — the encoder's memory is roughly `window_bytes` times
  `concurrency`, so a bigger window costs memory before it buys anything. Past
  8 MiB the gain on a database dump is small; the examples ship 8 MiB.
  `decode_window_bytes` is the restore's own ceiling: set it to the largest
  window any run still in retention was written with, and never lower it to
  match a later reduction.
- **`concurrency`** — above 1 it overlaps encoding with reading and writing;
  the match search inside a frame stays serial, so on one CPU it buys little
  and costs memory. Keep it no higher than the CPU the pod really gets (the
  chart's limit is one CPU). The writer is CPU-bound — its dump and its
  compression share the container's CPU limit — so on a busy node raise
  `resources.requests.cpu` toward the limit, and raise the limit itself to
  finish a run sooner.
- **Native `pg_dump` compression** — the `postgres` source passes
  `compression` to `pg_dump --compress`, and it defaults to `none` because the
  pipeline compresses. A deployment may let `pg_dump` compress instead
  (`zstd:3`, `lz4`, …) and declare no `zstd` processor for that artifact: one
  compressor, never both. The stored object is then already compressed, so its
  recorded raw size is the compressed size and a message's ratio reads about
  1.0×. No native setting dominated every corpus — `zstd:1` beat `fastest` on
  the synthetic and the 161 MiB dumps and lost badly on the 2.1 GiB one, and
  `zstd:6` stayed larger than `better` everywhere — so it is a choice, not a
  default.
- **Upload fan-out** — after the producers, uploads bound a run: they are
  network-bound, so `run.storage_parallelism` (4 by default) sends objects to
  several storages at once. With two storages across an emulated 400 Mbit/s
  link per storage, uploading one at a time left the second storage's time on
  the wall; running them together cut measured wall time by 34–39% on the
  synthetic dump and 8–13% on the 2.1 GiB one.
- **Storage scheme** — point an `s3` endpoint at `https://`. The plugin accepts
  `http://`, so nothing refuses it, but the credentials and every artifact byte
  then cross the network in clear text. `pg_dump` speaks TLS through libpq's
  own `PGSSLMODE`, set in the chart's `extraEnv`; the `postgres` source has no
  TLS field of its own.

### Artifact

One logical unit: a database dump, a cache snapshot, a file tree as one tar
stream, a sealed snapshot. One artifact is one object;
`importance: required|best_effort` decides what its failure means to the run.

### Unit

One artifact of an instance whose domain holds several (a database server's
databases). Such a source answers `source.Enumerator` with each unit's name and
the config that dumps exactly it; `planning` builds one source per unit of the
same type and names the artifact `<envelope>/<unit>` and the instance
`<instance>/<unit>`. An invalid name (empty, containing `/` or a control
character) is that unit's own failed artifact, and its siblings still
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
with it is a problem run the protection ledger never deletes.

### Manifest

The run report as the last object of the run, carrying `schema` and a required
`kind`. A missing or invalid kind, or one that disagrees with the run ID, makes
a problem run: never deleted, credited as verified protection or selected for
artifact restore. Its listed bytes still count towards occupancy. Only the
current strict format is accepted. Restore decodes the recorded processor chain,
never a pipeline looked up in the current config, through read-only
`storage.Reader` ports. `guard.Port` supplies that read-only view. Writes use
Handle/Uploader/Inventory under a live `guard.Authority`; zero, expired or
revoked handles refuse. Only the protection ledger holds `guard.Remover`.
Raw instances stay behind `internal/plugins/storage/internal`.

### Run outcome

A run that returns a report ends with exactly one outcome, and it decides the
exit code. The verdict reads the writer lease, every storage's verification,
every artifact's result and the started notification, and nothing else;
the backup workflow evaluates them in this order, and the first rule that
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
message always names the final outcome. A
notifier that skips an event — an unsubscribed one returns `notifier.ErrSkipped`
— counts as neither delivered nor failed, so a started-only or muted webhook
cannot mask another notifier's failed finished delivery. The started notification
is not symmetric — its failure is one more failure, so with the flag it degrades
a run that would otherwise be delivered, and without the flag either event's
failure changes nothing. The verdict reads no other input: a failed retention
pass, lease release or source close is recorded in the report or
the log and leaves the outcome alone.

The backup workflow marks a storage verified when the run tried it, it holds
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
exit code says `failed`. A run refused the writer lease writes no manifest.
The report the command prints carries the final verdict.

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
event name to a plain-text template; the backup events are `run_started` and
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
stated in `GB`; a cap stated in `GiB` is rendered by `bytes`. Protection's
`cap_exceeded` text carries the exact byte count in binary units, never a
rounded SI reading.
`payload` is one body template for **every** event, executed with `.Title`,
`.Lines` (each `.Text` and `.Code`), `.Text` (the whole message) and `.Event`;
its keys are the receiver's own. `body_format: yaml` drops the empty rendered
lines, reads the rest as a YAML document and sends its JSON encoding; `text`
sends the render as it is. `validate` renders every event and the payload
against a matrix of built-in sample runs — started without previous-run data;
finished delivered, degraded and failed, with and without previous-run data,
exclusions, artifacts and storages, both run kinds, a manual run refused by a cap
and one refused the writer lease — so a template that only fits one night is
refused offline. `.Run.Previous` is populated only for `run_finished`; it is
empty for `run_started`. The `reporting` plugin derives it from storage manifests
and the previous Job's status, including pre-start failures
(`.Run.Previous.Failure`). A storage's `CapExceeded`, `CapLimitBytes` and
`CapUsedBytes` and a lease refusal (`.Run.Lease`) reach a template too.

### Run knobs and budgeting

| Key | What it bounds |
|---|---|
| `run.deadline` | the whole run: the context every artifact and storage call runs under |
| `run.finish_allowance` | the ending beyond that deadline; the manifests take its first half and are written together, so a storage that stalls on its own manifest costs only its own record, while the finished notification keeps the second half |
| `run.max_artifact_bytes` | one artifact: a source that would produce more is stopped and that artifact fails, so one oversized volume cannot fill the spool |
| `run.spool_reserve_bytes` | free space kept free in the spool; below it an artifact fails rather than filling the filesystem |
| `restore.spool_reserve_bytes` | free space kept beyond the object being written to the restore spool, checked before the first byte and during every write |
| `storages.<name>.time_budget` | the wall time while at least one call to that storage is in flight in one run — uploads, listings, deletes, the survey; overlapping calls book their union once, never a source's dump or a processor's compression |
| `storages.<name>.call_parallelism` | the per-instance call ceiling, passed to factories as `storage.BuildInput{CallParallelism}`; the default is stated by `config defaults`. It bounds survey manifest reads, multipart parts and Yandex folder-listing fan-out. The ceiling is per process and per instance: a `restore list` or a `validate --preflight` hook beside a run, and two instances on one service, each hold their own |
| `run.storage_parallelism` | how many storages upload at once, `4` by default. Overlapping holds one `run.storage_parallelism` slot from the storage's first call to its last; a storage that gets no slot, or has no multipart capability, uploads after produce on the finished file. With overlap, `run.deadline` must sit above max(production, upload) plus the finishing tail, not above their sum |
| `run.lease_wait`, `run.lease_poll` | how long a scheduled run waits for a live writer lease, and how often it asks again; a manual run never waits |
| `run.retry`, `run.unclassified_error` | the `execution` retry policy for every plugin call, including the class an error a plugin did not classify gets |
| `run.notifier_failure_is_fatal` | with the flag, a finished event no notifier delivered makes the outcome `failed`; a failed started notification only degrades it, a notifier that skips an event counts as neither delivered nor failed, and with the flag off neither event's failure changes the outcome |
| `run.progress_interval` | how often a running dump reports its bytes so far; 0 reports none |

An object goes to a storage in one request when it fits one, or part by part
through a storage that uploads in parts: up to
`storages.<name>.call_parallelism` parts are in flight at once, so a failed part
is re-sent and the parts already accepted are not, and an upload that keeps
failing is aborted rather than left billed. S3 abort uses the caller's context,
without detaching it or adding a separate abort timeout. SIGTERM cleanup uses the
shared detached context described in [Shutdown](#shutdown). Only after the
producer exited successfully and every part was accepted is Complete sent,
with the parts by number whatever
order they finished in. At the first failed part `transfer` stops issuing new
ones; a neighbour already in flight is cancelled, but its bytes may reach the
service after the abort — that residue stays billed, is invisible to both the
listing and the multipart inventory, and is at most `call_parallelism − 1` parts
per failed upload. A part must also fit the service's `request_timeout` on the
deployment's link: with up to `call_parallelism` parts sharing the channel,
`request_timeout` must cover `part_bytes × call_parallelism / bandwidth`, so a
slow link is a reason to lower `call_parallelism` to 1 there. An object past
10,000 parts is refused at part 10,001 — more than 156 GiB at the default 16 MiB
part. A run starts by sweeping what a killed predecessor left behind: every
incomplete multipart upload under the storage's own prefix, begun before `now −
(run.deadline + run.finish_allowance)` — older than the longest a live run can
last, so an upload an overlapping run is still writing is never taken — is
aborted once per storage per run by `transfer.Transfer.Sweep`. Sweep also reads the full
multipart inventory, including young and undated uploads. Any upload in that
inventory, or a failed inventory read, makes occupancy unknown: a stale-only view
cannot prove the storage empty. Stale selection decides only which uploads to
abort. `AbortOwn` instead tracks exact IDs opened by this invocation; it does not
enumerate a successor's uploads. A SIGKILL can leave uploads for a later sweep.

An artifact is not held back until its dump ends: a storage with the multipart
capability begins sending parts as the spool grows past one part and sends
Complete only when the artifact is whole, while `local` and `yandex_disk`
upload after produce, on the finished file. The spool still holds the whole
artifact either way — a failed storage is replayed from it and the later
storages read it — so `run.spool_dir` must hold `run.max_artifact_bytes`
whatever the overlap. While the size is unknown, an overlapping upload reserves
`run.max_artifact_bytes` against the storage's `max_total_bytes`; when the
artifact is whole the reservation is lowered to the exact size before Complete.
A manual run whose bound does not fit does not overlap: it uploads after
produce and is admitted or refused on the exact size, as before. Overlapping
holds one `run.storage_parallelism` slot from the storage's first call to its
last; a storage that gets no slot, or has no multipart capability, uploads
after produce on the finished file. With overlap, `run.deadline` must sit above
max(production, upload) plus the finishing tail, not above their sum. Across
storages the survey phase lists every storage at once — one listing and up to
`call_parallelism` manifest reads per storage — so it costs roughly the slowest
storage's listing plus its manifest reads, and holds at most
`storages × call_parallelism` reads in flight; the upload phase holds at most
min(storages, `run.storage_parallelism`) × `call_parallelism` parts in flight.

Yandex listing uses `internal/plugins/treewalk` to walk folders in parallel,
bounded by the same `call_parallelism`, without a nested shared semaphore or
another concurrency knob. A failed List attempt exposes no partial page; its
retry replays the full traversal from the input token, without checkpoints.

### Shutdown

`assembly` owns invocation instances in `lifecycle.Scope`. On SIGTERM, the backup
workflow starts `Shutdown` while the operation unwinds, in this order:

1. `CancelRequests`: close admission, retries and Complete; cancel producers and
   request contexts.
2. `Join`: wait for every request goroutine, including surveys, followers,
   uploaders and manifests. Incomplete join leaves the claim held and skips
   Release, Abort and Close.
3. `Release`: CAS-release the lease with durable uncertainty, only after full join.
4. `Abort`: call `AbortOwn` for each storage, using only this invocation's exact IDs.
5. `Close`: close clients in reverse construction order.

The whole sequence shares one detached `CleanupContext`, created with
`context.WithoutCancel` and bounded by `RequiredGrace`; all storages spend that
same deadline, not a fresh timeout per abort. `plugins.config.lifecycle` supplies
`join`, `abort_total`, `close` and `margin`; release uses `run.finish_allowance`.
With no preStop hook, RequiredGrace rounds their sum up to seconds: shipped
values are 1m + 2m + 2m + 30s + 30s = 360s.
The chart defaults `terminationGracePeriodSeconds` to 360 and passes the same
literal to `validate --termination-grace-seconds`. The gate rejects a smaller
value than effective config requires; raise chart grace when raising budgets.
For a manual validate, only an omitted flag skips this check; an explicit 0 is
checked and fails against any nonzero bound. Negative values are usage errors.
A grace period cannot force an uncooperative goroutine to stop.

### Catalog

The `kubernetes_configmap` catalog holds only the writer lease in a ConfigMap
created by the deployment, never run history. Its settings are:

- `namespace` and `name`: required ConfigMap namespace and name.
- `api_host_env` and `api_port_env`: environment variables naming the API host
  and port.
- `token_file` and `ca_file`: ServiceAccount token and API CA bundle paths.
- `request_timeout`: timeout of one API request.
- `lease_key`: ConfigMap data key for the lease; defaults to `lease.json`.

Previous-run information belongs to `reporting` (see [Message](#message)).

For rollout from a catalog that stored history, stop scheduled and manual writers
and wait until none is running. Delete the old catalog ConfigMap once, then let
the upgrade recreate it empty, or recreate it through its external manifest when
`catalog.create: false`. Never delete a live writer's lease. The replacement
holds only the lease; do not declare its `data` in deployment manifests.

### One writer

Runs that share storage roots are serialised by a cross-process writer lease the
catalog owns: an optional capability, absent as `errors.ErrUnsupported`.
`protection` acquires it before the first storage mutation — the stale sweep
included — and the backup workflow releases it at the end. A deployment's `concurrencyPolicy` governs only one
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
holder on a refusal), and the finished notification is sent. A catalog that
cannot hold a lease at all leaves a manual run unable to start (exit 1) and a
scheduled run lease-free. `Release` drops only the claim its own acquisition
token names.

A takeover proves the holder's process ended, not that provider-side requests
settled: Yandex Disk may answer HTTP 202 before its asynchronous commit. Durable
uncertainty survives `Release` and is inherited by later acquisitions. The current
holder clears inherited uncertainty after two equal consecutive full-survey
digests across every storage. A failed or partial survey invalidates the prior
observation; elapsed time never clears uncertainty. A stale token cannot clear
another holder's state, and failed durable updates leave occupancy unknown.
While occupancy is unknown, manual admission stays closed and unsafe reclamation
is forbidden; scheduled runs retain soft-cap/reporting behaviour. Retention also
protects another run's incomplete objects until `deadline + finish_allowance`.

### Retention

Retention is per storage, decided from the manifests that storage itself holds,
and **partitioned by run kind**: `storages.<name>.retention` is a map,
`{scheduled: {…}, manual: {…}}`; `protection` hands each policy a homogeneous run
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
deleted, because the survey cannot trust what it says about itself. The same
holds for a manifest the listing holds but that reads back absent, or that
cannot be read, parsed or accepted: the survey cannot see the copies it holds,
and an absent answer is never taken for "this run has no manifest".

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

Before policy or cap decisions, the protection ledger credits the current run
from its verified artifact copies and saved-manifest results, so a fresh verified
copy can release an older pin. `MarkUncertain` protects the uncertain run's
prefix, makes occupancy unknown and closes cap eviction.

The **byte cap** is the storage's `max_total_bytes`, owned by the protection
ledger, not a retention policy, and covering every kind at once. Before every object write on a storage
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
every storage's upload, verification and retention, the writer lease, each
notification and the closing summary — is one line written when it happens, so
`kubectl logs -f` shows a run live. A line opens with a `[HH:MM:SS]` clock (UTC)
and, for anything a plugin did, the instance that did it:
`source postgres/app: dumping …`, `processor zstd/squeeze: 163.7 MiB → 48.2 MiB
(3.4x) in 1.2s`, `storage s3/offsite: ✓ …`. Sizes are human (`629.8 MiB`),
durations rounded (`2m40s`), and a failure is one `✗` line naming the artifact
and the reason.

Every plugin reports through the scoped `*slog.Logger` assembly supplies;
nothing writes a log line with a `Printf`, and a plugin
never writes to stdout. `--log-level debug` is where the machine detail lives:
the sha256 and the full object key are appended to the line, and each excluded
object is a debug-only line of its own. The manifest retains exact values
regardless of log level. `stage` and the `restore` subcommands report the same
way; `restore list` stays a listing, one row per run and storage, with no clock.

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
                                 [--termination-grace-seconds SECONDS]
go-cluster-backup config  defaults
go-cluster-backup config  effective --config FILE [--output yaml|json]
go-cluster-backup plugins [check | describe KIND/TYPE]
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
- **config defaults / effective** — the embedded document; the effective
  candidate with default provenance and aggregated diagnostics (see [Plugins](#plugins)).
- **plugins** — compiled IDs, stability, required binaries and summary;
  `plugins describe KIND/TYPE` adds fields and an example. `plugins check` validates
  loader kinds, Floor, effective dependencies, cycles and availability offline,
  without constructing instances or doing I/O.
- **schema** — the JSON Schema of a configuration file.
- **doctor** — every external binary any registered plugin declares is on `PATH`.
- **restore** — see [restore.md](restore.md).

## Template surface

Templates read Go field names of the report types and a fixed function set. That
surface is a SemVer API for this project: a deployment's configuration spells
those names, so a rename is a breaking change. It is pinned by tests
(`internal/plugins/configuration/template_surface_test.go`, `internal/plugins/layout/layout_surface_test.go`).

- **Fields:** `RunReport`, `ArtifactResult`, `Artifact`, `StorageResult`,
  `DumpStats`, `Exclusion`, `PreviousRun`, `PreviousFailure`, `ContainerFailure`,
  `ContainerTermination`, `LeaseState`, `Event`; and the layout data
  (`Run.ID`, `Run.StartedAt`, `Artifact.Group`, `Artifact.Name`, `Artifact.Ext`).
- **Functions:** the core set — `json`, `date`, `bytes`, `sibytes`, `duration`,
  `pick` — plus the notifier's own (`blank`, `byGroup`, `failed`, `sumBytes`,
  `pickdef`, `clip`) and the discovery's own (`label`, `envValue`, `envFile`,
  `port`, `volumePath`).

## Adding a plugin

See [CONTRIBUTING.md](../CONTRIBUTING.md). Declare literal `var Spec` in
`plugin.go`; libraries have no `Apply`. Factory contributors use
`instances.FactorySpec` or `storage.FactorySpec`. `make generate` emits
`internal/plugins/all/{zz_generated.go,floor.json}` and
`internal/plugins/all/components/zz_generated.go`. Hostless `components.Compose`
registers non-host plugins; `all.Register` adds components and hosts over fresh
factories. `all.Compose` builds that full graph with caller-owned factories.
`make rules`, included in `make lint`, checks freshness and structural rules.
