# go-cluster-backup

Backup tool where everything is a plugin: sources, processors, storages,
retention, discovery, catalog, notifier. One run makes one object per artifact,
uploads it to every storage, writes the run manifest, then applies per-storage
retention; `restore` lists, fetches, applies and verifies. `README.md` is the
project's front door; `docs/configuration.md` and `docs/restore.md` are its long
form. This file is how to work in this repository. `CONTRIBUTING.md` is the short
path for a contributor adding a plugin.

## Tech Stack

- **Language**: Go 1.27.2, one module (`github.com/vi-latyshev/go-cluster-backup`).
  Lint and vulnerability scanning are pinned as tool dependencies in `tools/go.mod`
  (a separate module, never linked into the binary).
- **Config**: YAML on disk, converted to JSON in `internal/cli` (one dependency,
  `go.yaml.in/yaml/v3`); the engine and every plugin see JSON bytes only. That
  dependency has exactly one other reader: the webhook notifier, whose
  `body_format: yaml` payload is a document the receiver takes as JSON.
- **Image**: `docker.io/library/golang:1.27.2-alpine` builder (cross-compiling
  from the build platform) → `docker.io/library/alpine:3.24.2` runtime with
  `postgresql18-client`, `redis`, `sqlite`, `tzdata`, `ca-certificates`; the
  binary lands at `/usr/local/bin/go-cluster-backup` and `doctor` runs as the
  last build step.
- **Runtime**: Kubernetes CronJobs, one-shot processes, installed by the Helm
  chart in `charts/go-cluster-backup/`.

## Layout and ownership

```
cmd/go-cluster-backup/                            the only source outside internal/
internal/defaults/                                the service's defaults: the one embedded document
internal/plugin/  internal/model/  internal/engine/  internal/spool/   framework: the seams, the registry, the engine facade
internal/<kind>/  internal/<kind>/*test/           the seven kind interfaces and their conformance kits
internal/plugins/<kind>/<type>/                    first-party plugins: local, s3, yandex_disk, postgres,
                                                   redis, sqlite, files, staged, zstd, age, gfs, keep_last,
                                                   k3s_state, kubernetes_labels, kubernetes_configmap, webhook
internal/cli/  internal/kubeapi/                   command layer; the Kubernetes API client (stdlib only)
internal/human/                                    the log's own rendering: sizes, durations, the clock
charts/go-cluster-backup/                          the Helm chart: values, templates, generated README
examples/                                          example configs, values, Flux and Argo CD manifests
docs/                                              README's long form: configuration, restore
test/chart/                                        chart tests (a real `helm template` run)
tools/                                             the pinned lint/vuln/chart tool module
.github/                                           CI, release and dependency automation
```

- **All Go sources live under `internal/`; only `cmd/go-cluster-backup/` is
  outside it.** There is no public Go API: the module is consumed as a binary,
  a configuration schema and a chart, and a package rename is free to break any
  importer. Third-party plugins arrive as pull requests to this repository, not
  as external modules — one package under `internal/plugins/<kind>/<name>` plus
  one registration line.
- **This is a public repository: nothing of one deployment or one cluster may
  live anywhere in it** — no cluster names, domains, namespaces, IPs, tokens,
  label names or receiver payload shapes. Examples use `backup.example/` for
  labels and `example.com` for hosts. The hygiene grep is part of review and
  must print nothing.
- `internal/engine/`, `internal/plugin/`, `internal/model/` and the kind
  packages are the framework's frozen surface; a plugin depends on them, never
  the other way round.
- A plugin is one package plus one registration line: `internal/cli/register.go`.

## Releasing

Releases are computed from Conventional Commits: the publish job runs git-cliff
(`cliff.toml`) for the bumped version, claims the tag `v<version>`, builds the
image and chart with that version, and publishes git-cliff's notes as the GitHub
Release.

- **Chart.yaml holds placeholders** (`0.0.0`, image tag `master`); the publish
  job sets the real version at packaging. Never bump either by hand.
- **`Release-As: X.Y.Z[-pre]`** in any commit trailer forces the next version.
  A docs-only push runs no workflow, so carry the trailer on a non-docs change.
- **Never push a tag by hand.**
- **SemVer:** a breaking change to the configuration schema or the chart is a
  major release; the stored backup format never breaks.

## The chart

- **`config` is a string, never rendered through `tpl`.** The tool's own `{{ }}`
  are its template syntax; Helm must not expand them.
- **No defaults for a field the tool requires.** The chart states how the
  CronJobs run the tool, never what is backed up; a tool-required field comes
  from the configuration document.
- **Path contract:** the config is mounted at
  `/etc/go-cluster-backup/config.yaml` from the `<fullname>-config` ConfigMap;
  the catalog ConfigMap defaults to `<fullname>-catalog` and is created with
  `helm.sh/resource-policy: keep` (`catalog.create: false` to manage it
  yourself). A consumer binds to these names.
- **Every container sets `terminationMessagePolicy: FallbackToLogsOnError`**, so
  a pre-run failure carries its last log lines into the previous-run report.
- **A node hostPath reaches one pod, never all of them**: `writer.volumes`/
  `writer.volumeMounts` for the writer pods (the `stage` initContainer shares
  the pod), `restore.volumes`/`restore.volumeMounts` for the restore pod;
  `extraVolumes`/`extraVolumeMounts` reach every pod, the config-gate hook
  included, so only an innocuous shared volume belongs there.
- **Changing `values.yaml` means running `make docs`** — the chart README is
  generated from the value comments, and CI fails on drift.
- The chart's own long form is `charts/go-cluster-backup/README.md`; the release
  README points at it rather than restating values.

## Git

Conventional Commits: `type(scope): description`, single line preferred, subject
≤70 characters; a body only when the change is unintelligible without one.


## Binding rules

- **The service's defaults live in one document; a plugin holds none.**
  `internal/defaults/defaults.yaml` is embedded into the binary and is the only
  place a default exists. The engine merges it under the user's file before any
  bytes cross a plugin boundary — maps key by key, a present scalar or list
  replaces the default, `null` is refused, and a default never creates an
  optional section, an instance or a list element. A plugin still receives
  complete bytes and still treats every field as required; the engine merges a
  plugin's block as an opaque object and never reads a field of it. Every config
  field is exactly one of two things, and startup, `doctor` and a test prove it:
  marked `required:"true"` in its struct — the deployment must state it, and a
  default for it is an error — or present in a defaults document. A
  `plugin.Secret` field is always required, tagged or not, and no document may
  carry a credential. A field is `required` when no value would suit every
  deployment, or when a wrong silent value would delete data, leave it
  unprotected or expose it: a storage's root or prefix, a credential, a
  retention tier or byte budget, a delete mode, an artifact's sensitivity, a
  coverage rule, the exclusion marker, a budget sized for one deployment, a
  message text, every `restore.apply` option. A new plugin is one package, one
  registration line and its block in `internal/defaults/defaults.yaml`. No
  default lives in a constructor, in the engine's Go code or in a CLI flag: the
  only flags carrying one choose how a command reports itself (`--log-level`,
  `--output`), never what the backup does. `config defaults` prints the document
  as it is; `config effective --config FILE` prints the merged file with every
  defaulted value marked.
- **A stored-format constant is a named class of default, and it is pinned.**
  A default whose value a later run reads back — the storage layout
  (`layout.*`, `run.id_layout`), a catalog's `key`, the format an already-written
  dump is read with, a plugin's own type name as a manifest records it — stays a
  default (no deployment has a reason to state it) and is pinned by a test in
  `internal/defaults/defaults_test.go` that fails on any change, with the reason
  in its message. Nothing at run time notices such a change: a survey lists the
  old prefix and sees nothing, retention stops deleting, a restore finds
  nothing, and every run already written is orphaned at once. A value that is
  not read back, and not one of the required classes above, is an ordinary
  default.
- **What the defaults check cannot see.** It decodes every value of a document
  through its field's type, so a bad duration, size, template or secret
  reference is an error at startup — but a value only the plugin's own `Validate`
  understands (an enum, a range, a combination) is a plain string there. `doctor`
  closes that gap for the documents: it merges each registered type's own
  complete example with its block in the defaults document and builds the
  instance, so a bad enum in a defaults document fails the image build, and
  `validate` does the same for every instance a config declares. What remains
  uncaught is a value in a place no instance is built from: a discovery rule's
  source config is checked for shape, nulls and a registered type, never built,
  so a bad enum inside a rule's own `source.config` is refused by the run that
  builds it, not by `validate`.
- **One attempt per plugin call.** Retries, backoff, budgets and timeouts are
  the engine's, read from the config. A plugin that retries on its own breaks
  the budget accounting.
- **Preflight is scoped.** A failed instance `Check` fails that instance's own
  work — its source, its storage, its discovery — and the rest of the run
  continues; a storage is out for the run only when its failure is permanent, a
  transient one costs a retry. `validate --preflight` checks every declared
  instance and every source a discovery finds (built and checked the way a run
  would build it), reports one line per instance and exits non-zero; a discovered
  object carrying a problem is one such line. Only a config error or a missing
  `Spec.Requires` binary stops a run before it starts. A plugin implements
  `Checker` for everything `validate --preflight` ought to catch: credentials,
  reachability, a mount.
- **A plugin writes its temporary files into its configured `scratch_dir`**, a
  directory the deployment mounts for it — never the process temp dir, which is
  a small `emptyDir` in the cluster; and it removes what it wrote on every exit
  path.
- **Artifact bytes land on disk 0600, or by an explicit config choice.** The run
  spool and every `restore fetch` output are 0600 whatever the artifact's label
  says — a dump is as sensitive as a secret one. The staged artifact and its
  chain record are 0644 because the file crosses into another container and the
  image's own default user is unprivileged (uid 1000); a plugin that restores
  with a configurable mode (sqlite's `file_mode`) uses the configured one.
- **A kind interface grows only by a NEW optional interface** (a capability is
  discovered by call, `errors.ErrUnsupported`). Never add a method to an
  existing kind interface.
- **The engine names what a plugin enumerates.** A source that answers
  `source.Enumerator` returns `Unit.Name` and the config the unit is dumped
  with, nothing more: the engine builds the unit's source, names the artifact
  `<envelope>/<unit>` and the instance `<instance>/<unit>` (one engine helper,
  `internal/engine/units.go`), and decides what a failure of the enumeration
  means. A plugin never spells an artifact name for a unit and never constructs
  its own child.
- **The manifest is a stored format.** `model.RunReport`'s JSON keys and
  `model.ManifestSchema` are frozen; artifacts live in storages for weeks, so a
  Go field rename must never change a key. A restore decodes with the chain the
  manifest records, never with a pipeline looked up in the current config. The
  `kind` key is additive and its absence is defined: a manifest without one is a
  scheduled run (`model.NormalizeKind`), which is the one owner-approved
  data-format compatibility this service carries.
- **A run's kind is its identity, and retention partitions by it.** `run --kind`
  has no default; the kind is in the run id (`layout.manual_id_suffix`, a pinned
  stored format) and the manifest, and a survey derives it from the id and
  refuses a manifest that disagrees. The engine — never a policy — partitions
  runs by kind and hands each policy a homogeneous list, so `retention.Decide`
  and its implementations stay kind-blind.
- **The byte cap belongs to the engine, on the storage envelope.** A storage's
  `max_total_bytes` covers every kind at once; a manual run is admitted against
  it before each object write and refuses, a scheduled run never fails on it and
  frees room instead. No retention policy carries a byte budget.
- **Cross-process writes are serialised by a lease the catalog owns.** The
  single-writer claim is an optional catalog capability (`catalog.Leaser`,
  discovered by call, `errors.ErrUnsupported` meaning absent); the engine takes
  it before the first storage write and releases it at the end. A held claim is
  taken over only on a fact that holds on this deployment: the kubelet's terminal
  phase of the holder's exact pod (scheduled only), or an absent/changed pod plus
  `run.lease_hard_bound` — the deployment's statement of the longest a writer can
  live under `activeDeadlineSeconds`. Never on a clock alone; an absent pod alone
  is not proof, and an absent capability, `ErrUnsupported` or a failed check
  counts as alive. A catalog without the capability leaves manual runs refused
  and scheduled runs lease-free. A scheduled run whose bounded wait for a live
  holder expires does not return a bare error: it ends through the common finish
  — outcome failed, the lease reason in the report, the finished notification
  sent — exactly as a manual refusal does. Retention never deletes another run's
  incomplete objects before `deadline + finish_allowance`; a takeover under the
  kubelet's phase records a quarantine in the lease itself — Release keeps it and
  later acquisitions inherit the latest — so occupancy stays unknown until the
  dead holder's hard bound passes, whoever holds the claim by then.
- **A previous run that failed before the tool ran is surfaced, never guessed.**
  A failure before the tool starts — a rejected initContainer config, an
  `ImagePullBackOff`, a PSA `FailedCreate`, a `Pending` pod that hits
  `DeadlineExceeded`, an OOMKill, a SIGKILL — leaves no catalog record, so
  `.Run.Previous` alone would never show it. At run start the catalog's optional
  `PreviousRunInspector` capability reads the previous Job of the caller's own
  CronJob (own pod from `run.holder_namespace_env`/`holder_pod_env` → owner Job →
  owner CronJob → the newest other Job of it) and reports its failure: the Job
  condition's reason and message, and each container's terminated reason, exit
  code and `terminated.message` (the chart sets `terminationMessagePolicy:
  FallbackToLogsOnError`, so the message carries the last log lines), or its
  waiting reason. It is readable data only; a missing RBAC grant degrades to a
  visible "previous run status unavailable" and never fails the run.
- **The config file is strict.** Unknown members, missing fields, bad enums and
  template syntax errors are all reported with JSON-pointer paths, all at once.
- **Process hygiene.** `log/slog` only; `signal.NotifyContext`, `os.Exit` and
  `log.Fatal` exist only in `cmd/go-cluster-backup/main.go`; the engine never
  exits a process and a run is repeatable in one process.
- **The template-visible surface is a SemVer API.** A message or payload
  template reads Go field names of `model` types and a fixed function set (see
  `plugin.CoreFuncs` and each notifier's `TemplateFuncs`). That surface is
  pinned by a test (`internal/plugin/template_surface_test.go`): renaming an
  exported field or a template function fails CI, because a deployment's config
  file spells those names and a rename would break a message at 04:20.
- **Recording an owner exception (discipline Rules 4/5, one implementation per
  seam):** the kinds `discovery`, `catalog` and `notifier` ship with exactly one
  implementation each (`kubernetes_labels`, `kubernetes_configmap`, `webhook`).
  The owner's ruling is "everything-is-a-plugin (mandatory)" — the seam stays a
  seam so a second implementation costs one package, and the owner explicitly
  asked for this exception to be recorded here.

## Commands

The Makefile is the entry point; CI calls the same targets.

```bash
make check       # vet test lint vuln chart — the whole gate
make vet         # go vet ./...
make test        # unit-cover + test-gate: the unit suite, gated at 100%
make unit-cover  # go test -race -timeout=5m -coverprofile=coverage.out ./... (no gate)
make test-gate   # gate an existing coverage.out — no second test run
make cover       # gate the unit+e2e union (needs coverage.out and coverage-e2e.out)
make lint        # golangci-lint, then the hard `//nolint` grep gate
make vuln        # govulncheck, pinned in tools/go.mod
make chart       # helm lint --strict, kubeconform (1.31 and 1.36), helm-docs drift
make docs        # regenerate charts/go-cluster-backup/README.md from values.yaml
make e2e         # k3d end-to-end suite: E2E_IMAGE=<image loaded into the cluster>
make image       # docker build -t go-cluster-backup .
```

CI runs two jobs: `make vet lint vuln chart`, and `make test e2e cover`.
`make e2e` builds `go-cluster-backup:e2e` with `--build-arg COVER=1` unless
`E2E_IMAGE` names an image already loaded, and the suite creates and deletes its
own k3d cluster (k3d v5.9.0, `rancher/k3s:v1.37.1-k3s1`, name `E2E_CLUSTER`,
default `gcb-e2e`). It runs 7 scenarios — install + validate hook, manual run,
restore round-trip, previous-run failure, writer lease, rejected upgrade,
k3s_state — writes counters to `coverage-e2e/` and converts them to
`coverage-e2e.out`; `make cover` then gates the unit+e2e union at 100%.
Prerequisites: `docker`, `k3d`, `helm`. Details in `test/e2e/README.md`.

Narrow a fix cycle to the affected package (`go test -race ./internal/<pkg>/...`),
then run `make check` once before handing the work over.

## Coverage and error rules

- **Coverage is 100%, and it is measured.** `make test` gates the unit profile,
  `make cover` the unit+e2e union; both must be 100.0% of statements, deterministic.
  A block counts once, and is covered when any suite executes it.
- **`//nolint` is banned, with no exception list.** Fix the finding or change the
  code; `make lint` greps for the directive and fails. `nolintlint` stays enabled
  at its strictest as a backstop.
- **Impossible errors → `must.Do`** (`internal/must`); reachable errors are
  handled, never wrapped. An OS failure a test cannot provoke goes behind one
  unexported package-level seam variable defaulting to the real call, overridden
  only in tests. A conformance kit's `t.Fatal` path is exercised through
  `fatalReporter` or a subprocess, never suppressed. No build tags, no
  `-coverpkg`, no assertion-free tests to move the number; test files are named
  by subject.

## Tests

- Command-level tests live in `internal/cli`: they drive the real command layer
  with fakes for the cluster (a synthetic source, a fault-injecting storage from
  `internal/storage/storagetest`), no network and no credentials.
  `cmd/go-cluster-backup` tests its own registration through the same entry
  point.
- `cmd/go-cluster-backup/testdata/config.yaml` is the reference config and is
  validated by a test: change a plugin's config shape and that test fails until
  the file follows. `cmd/go-cluster-backup`'s `main` test also proves the
  defaults document classifies every registered field, so a new field with
  neither a tag nor a block fails there.
- `examples/config/*.yaml` are validated by the same test, so a shipped example
  never drifts from the schema.
- The s3 live test stays skipped; the offline suite is the gate.

<!-- vi-kit-begin -->

## Routing

Zone-to-specialist routing for this repository lives in `.vi-kit/routing.yaml`.
The orchestrator resolves zones from that file; refine it with `vi-kit init` or by hand.
<!-- vi-kit-end -->
