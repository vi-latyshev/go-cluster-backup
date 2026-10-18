# Configuration

`go-cluster-backup` reads one YAML document. `run`, `stage` and the `restore`
subcommands read the same file; each answers for the part it uses. The chart
mounts it at `/etc/go-cluster-backup/config.yaml`.

Reference configurations:

- [examples/config/minimal.yaml](../examples/config/minimal.yaml) — Postgres by
  label, one S3-compatible storage.
- [examples/config/k3s-single-node.yaml](../examples/config/k3s-single-node.yaml)
  — adds the k3s datastore stage and file volumes.
- `cmd/go-cluster-backup/testdata/config.yaml` — every field of every plugin,
  validated by a test.

## Concepts

- **Kind** — a seam: `source`, `processor`, `storage`, `retention`, `discovery`,
  `catalog`, `notifier`. A kind's interface grows only by a NEW optional
  interface; a capability is discovered by calling (`errors.ErrUnsupported`),
  never by widening an interface that already has implementations.
- **Instance** — one configured plugin: the kind's envelope plus `{type, config}`.
  Every field of an instance is required — the file or a defaults document
  states it, and no plugin supplies a value — and an unknown member is an error
  with its JSON-pointer path.
- **Defaults** — the fields no deployment may choose (a credential, a storage's
  root, a retention tier or byte budget, a message text) are the deployment's to
  state, and every other field has one value in the service's own document,
  `internal/defaults/defaults.yaml`, embedded into the binary. The engine merges
  it under the configuration file before a byte reaches a plugin: objects merge
  key by key, a value the file carries replaces the default whole, `name[]` fills
  the elements of the list `name` the file declares, `null` is refused, and a
  default never creates an optional section, an instance or a list element. A
  plugin still receives complete bytes and treats every field as required.
  `config defaults` prints the document; `config effective --config FILE` prints
  the merged file with every defaulted value marked.
- **Size units** — a whole number of bytes, or digits with a unit: `KB`, `MB`,
  `GB`, `TB` are powers of 1000; `KiB`, `MiB`, `GiB`, `TiB` powers of 1024.
  Nothing else decodes: a bare `"1024"` and a lowercase `5gb` are refused by name.
- **Pipeline** — an ordered list of processor instances an artifact flows
  through. Processors are stream wrappers: `zstd` compresses, `age` seals. An
  artifact with `sensitivity: secret` whose pipeline does not seal (and whose
  source does not deliver sealed bytes) is refused before the first byte. A
  source whose bytes arrive already encoded — `staged`, reading what `stage` left
  — declares no pipeline: it carries the chain of the process that produced its
  bytes, and the manifest records that chain.
- **Artifact** — one logical unit: a database dump, a cache snapshot, a file tree
  as one tar stream, a sealed snapshot. One artifact is one object;
  `importance: required|best_effort` decides what its failure means to the run.
- **Unit** — one artifact of an instance whose domain holds several (a database
  server's databases). Such a source implements `source.Enumerator` and answers
  one unit per artifact; the engine builds one source per unit and names the
  artifact `<envelope>/<unit>` and the instance `<instance>/<unit>`.
- **Coverage** — a discovery decides about every object it sees, exclusively. A
  rule renders an object into an artifact; an object the exclusion marker names,
  or one another artifact already carries, becomes an **exclusion** — recorded in
  the report and the manifest, nothing built for it. An undeclared claim is ONE
  failed artifact, never a silent absence.
- **Layout** — key templates. Artifacts land under
  `runs/<run id>/<group>/<name><ext>`, the manifest at `manifest.json` beside
  them. The literal head of `layout.run_prefix` and the whole
  `layout.manifest_key` are survey anchors: renaming either orphans every run
  already written. A manual run's id is the scheduled one plus
  `layout.manual_id_suffix` (default `-manual`), so a survey tells a run's kind
  from its key alone.
- **Manifest** — the run report as the last object of the run, carrying `schema`.
  A restore decodes with the chain the manifest records, never with a pipeline
  looked up in the current config. **The manifest's JSON keys and its schema
  version are a stored format: they never change.**
- **Retention** — per storage, decided from that storage's own manifests, and
  partitioned by run kind (`{scheduled: {…}, manual: {…}}`), so a manual run never
  takes a GFS slot. The byte cap (`storages.<name>.max_total_bytes`) belongs to
  the engine and covers every kind at once: a manual run is admitted against it
  and refuses; a scheduled run frees room instead. The engine's guard refuses any
  deletion that would leave no verified complete run.
- **One writer** — a cross-process writer lease held through the catalog's
  optional capability. A held claim is taken over only on the kubelet's terminal
  phase of the holder's exact pod (scheduled runs), or an absent/changed pod plus
  `run.lease_hard_bound`. A manual run refuses a live claim at once; a scheduled
  run waits `run.lease_wait` and then fails through the common finish — the
  finished notification is still sent and the run is recorded as failed.
- **Run outcome** — `delivered`, `degraded`, `failed`; the exit codes are
  `run.degraded_exit_code` and `run.failed_exit_code` (delivered is 0).
- **Message** — the `webhook` notifier's texts are config, not the plugin's:
  `events` maps `run_started`/`run_finished` to a plain-text template; a line that
  renders empty is dropped; a pair of literal backticks marks a line's monospace
  part. `payload` is one body template per event, executed with `.Title`,
  `.Lines`, `.Text` and `.Event`; `body_format` says what the receiver gets.
  `validate` renders every event against a matrix of sample runs, so a template
  that only fits one night is refused offline. `.Run.Previous.Failure` carries the
  previous run's failure even when it died before the tool started.

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
  `staged` source; the file name is the identity. Beside each artifact the stage
  publishes its chain record, so the manifest carries how the bytes were encoded
  and a restore decodes with exactly that.
- **validate** — offline: structure, types, templates, every reference between
  instances, every error at once with its path. It succeeds on a config whose
  secret files do not exist: a secret is a reference, and only a plugin's `Check`
  reads it. `--preflight` adds those checks for every declared instance and for
  every source a discovery finds, prints one line per instance, and exits
  non-zero on any failure. `--instance KIND/NAME` restricts it, for a pod that
  cannot reach every instance.
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
