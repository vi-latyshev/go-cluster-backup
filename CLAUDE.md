# go-cluster-backup

Backup/restore with one object per artifact, multiple storages, discovery,
retention and notifications. Start at `README.md`; details: `docs/configuration.md`
and `docs/restore.md`. This guide governs work; `CONTRIBUTING.md` covers plugins.

## Tech Stack

- **Language:** Go 1.27.3, module `github.com/vi-latyshev/go-cluster-backup`.
  The Makefile derives `GOTOOLCHAIN` from go.mod. Separate `tools/go.mod` pins
  lint/vulnerability/chart tools and Helm v4, never linked into the binary.
  Targets/tests use `go tool`, never Helm from `PATH`. Tidy with
  `cd tools && go mod tidy`, never root `-modfile`.
- **Config:** `internal/plugins/configuration` converts YAML to JSON with
  `go.yaml.in/yaml/v4`; domain code and plugins use JSON. The webhook also reads
  YAML but sends JSON for `body_format: yaml`. `internal/plugins/kubeapi` is
  standard-library-only.
- **Image:** `docker.io/library/golang:1.27.3-alpine` cross-compiles from the
  build platform into `docker.io/library/alpine:3.24.3`, with
  `postgresql18-client`, `redis`, `sqlite`, `tzdata`, `ca-certificates`.
  `/usr/local/bin/go-cluster-backup` runs `doctor` as the last build step.
- **Runtime:** one-shot Kubernetes CronJobs from `charts/go-cluster-backup/`.

## Layout and ownership

```
cmd/go-cluster-backup/                         thin loader + generated-root entry point
internal/plugins/configuration/defaults/      the one embedded defaults document
internal/loader/                              generic container
internal/plugins/all/  internal/plugins/all/components/  generated full and hostless composition
internal/plugins/cli/                         flat Floor command host
internal/plugins/assembly/                    construction and typed option projection
internal/plugins/workflow/{backup,stage,restore}/  application workflows
internal/plugins/{instances,configuration,errors,io}/  factory mechanics, config, errors, streams
internal/plugins/<kind>/                      seven kind contracts, factories and <kind>test/ kits
internal/plugins/<kind>/<name>/                first-party backend implementations
internal/plugins/{model,human,must,spool,extcmd,kubeapi,sqlitecopy}/  shared libraries
charts/go-cluster-backup/                      chart, values and generated README
examples/  docs/  test/chart/                  examples, long-form docs and real Helm tests
tools/  .github/                              pinned tooling and automation
```

- Application behavior lives in `internal/plugins/`. Outside it are the generic
  `internal/loader`, build-time `tools/gen` and `tools/rules`, and thin
  `cmd/go-cluster-backup`. Main's project imports are `loader` and `all`; it builds
  no factories or defaults. There is no public Go API: plugins arrive as repository
  pull requests, not external modules. Renames must update every caller.
- Backend type names come from metadata, not Go folder names.
- This is a public repository. No deployment-specific cluster names, domains,
  namespaces, IPs, tokens, labels or receiver payload shapes belong in it.
  Examples use `backup.example/` labels and `example.com` hosts. The review's
  deployment-hygiene grep must print nothing.
- `AGENTS.md` is a symlink to `CLAUDE.md`. Keep one physical file.

### Runtime composition

`internal/loader` uses only the standard library for memory-only registration,
generic kinds/services/roles, availability, dependency health and global lifecycle.
No backup phases, domain policy, YAML, schemas, semantic IDs or commands belong
there. Plugins own application work:

- `assembly` builds from Config and projects typed options to collaborators.
- `execution` owns retries, panic boundaries and budgets.
- `layout` builds keys; `inventory` owns surveys and StoredRuns.
- `planning` owns discovery, units and preflight.
- `pipeline` owns production, processor chains and Dump.
- `transfer` owns whole/multipart uploads, full-inventory Sweep and AbortOwn.
- `protection` owns the lease and cap/retention ledger, the sole deleter.
- `storage/guard` supplies read ports, guarded writes and ledger-only removal.
- `lifecycle` supplies invocation scopes and ordered shutdown.
- `workflow/backup` holds the Floor skeleton with typed Options.
- `workflow/stage` and `workflow/restore` own their operations.
- `reporting` derives finished-event previous-run data.

- **Registration is generated.** Put a literal `loader.Spec` in `plugin.go`.
  Library plugins have no `Apply`; contributing plugins use it to register
  factories, services or roles. Factory metadata uses `instances.FactorySpec`
  or `storage.FactorySpec`; `var Spec` is the plugin identity. `make generate`
  writes `internal/plugins/all/{zz_generated.go,floor.json}` and
  `internal/plugins/all/components/zz_generated.go`. Hostless `components.Compose`
  registers non-host plugins. `all.Register` adds components and hosts over fresh
  factories; `all.Compose` uses caller-owned factories. The flat Floor host `cli`
  registers only through this root; its `Apply` contributes roles inside the root's
  `loader.Add`, never calls `loader.Add` itself. `make rules` in `make lint`
  checks freshness. Composition roots are not plugins.
  Generated production imports for supported platforms plus
  loader `Requires` form one graph; `Requires` never names binaries. Derive factory
  IDs once as `kind/type`; other providers declare a stable ID.
- **Floor is a closure.** Declared Floor plugins, including `cli` and
  `workflow-backup`, and their transitive dependencies cannot be disabled.
  Library plugins are always active. Compression and encryption remain optional,
  not Floor members.
- **One document controls plugins and backup.** No config `version` key exists.
  Defaults and deployment share `plugins: {bundles, disabled, config}`. Partial
  settings use factory `kind/type` or provider IDs in `plugins.config`.
  Precedence is defaults, deployment per-type, then `instance.config`, also for
  nested discovery and retention. Settings never create instances; deployment
  per-type values may supply required fields. Provenance uses RFC 6901 pointers,
  such as `/plugins/config/source~1postgres/format`.
- **Compiled knowledge is not active availability.** Start with all compiled
  plugins. Apply merged bundles in order, then `plugins.disabled`; deployment
  lists replace defaults. `bundles: []` clears bundles. Remove disables and
  disabling bundles to re-enable, subject to Floor/dependency constraints.
  Check partial settings even for disabled plugins. Reject unknown IDs/bundles/
  members, duplicate keys, nulls and invalid types; check required fields and
  combinations after assembly. Schema hooks must not construct discovery clients.
  Offline validation checks present roles, nested sources, dependencies and unique
  services before I/O.
- **Diagnostics do not grant permission.** `plugins check` validates composition
  offline. `validate` and `config effective` aggregate config, availability and
  dependency errors. Effective config prints a mergeable candidate to stdout,
  diagnostics to stderr and exits non-zero on errors. Unreadable/unmergeable input
  has no candidate; operations fail closed.
- **Storage owns its construction input.** Factories get scalar
  `storage.BuildInput{CallParallelism}` from `call_parallelism`, not a semaphore.
  Yandex imports `internal/plugins/treewalk` for bounded parallel folder walks.
  Failed List attempts return no page; retry the full traversal from the input
  token. Keep sibling caller contexts, worker-local panic recovery, joins, one
  classified cause, deterministic pages and root/prefix-confined strict cursors.
  No checkpoint or separate concurrency/page-size knob.
- **Current data boundaries are strict.** Restore uses `storage.Reader`, with
  no Put/Delete methods. A manifest needs a valid `kind` matching its run ID;
  missing/wrong kinds are problem runs, never deleted or credited as protection.
  Strictly rejected manifests are not artifact-restore candidates, but their
  listed bytes count against occupancy. Keep the stale cutoff for manifestless
  attempts, explicit-key raw fetch, multipart sweep and Yandex folder pruning.
- **The catalog holds only the CAS lease.** `reporting` fills `.Run.Previous` and
  last-good from storage manifests and previous Job status only for finished
  events. Follow the [catalog rollout](docs/configuration.md#catalog) on upgrade.
- **Inherited uncertainty is not a timer.** Release preserves it and Acquire
  inherits it. The current holder clears it after two equal consecutive full-survey
  digests; a failed survey invalidates the earlier observation. A stale holder,
  HTTP join, timeout or Yandex HTTP 202 cannot clear it. Failed durable updates
  leave occupancy unknown.

### Safety and lifecycle

Workflows receive typed options and direct collaborators, never Config or a
service locator.

- **Floor holds authority.** Its non-disableable application driver keeps a fixed
  skeleton: acquire, deferred release, refusal branch, complete survey and
  collision checks, admitted writes, verified manifests, retention, common finish.
  Keep the run deadline separate from the outer context and the two
  `finish_allowance / 2` manifest/finish windows. A refused run skips survey,
  writes, manifests and retention but reaches finish. `After`/`Needs` can order
  contributions only inside named slots; they cannot replace this skeleton or
  grant mutation rights.
- **Handles enforce mutation rights.** `guard.Port` is read-only.
  `guard.Handle`, `guard.Uploader` and `guard.Inventory` require a live
  `guard.Authority`, issued after acquisition, collision and admission checks.
  Zero, expired or revoked handles refuse. Only the protection ledger holds
  `guard.Remover`. The raw instance stays behind `storage/internal` and import
  rules. `transfer.Transfer.AbortOwn` cleans up only this run's exact upload IDs, including
  after authority revocation. This is a boundary for trusted Go, not a sandbox.
- **Scopes own instances.** `assembly` owns clients in `lifecycle.Scope`.
  Normal completion joins requests, releases the lease, then the role closes
  clients in reverse construction order, not reverse `Requires`. SIGTERM runs
  production `Shutdown`: `CancelRequests`, `Join`, `Release`, `Abort`, `Close`.
  Release requires full join. Incomplete join leaves the claim held and skips
  release, abort and close. Abort calls `AbortOwn` for each storage. Preserve the
  primary failure; cleanup failures are diagnostic. Global Stop follows scopes;
  a timeout cannot stop an uncooperative goroutine.
- **Shutdown has one bound.** `plugins.config.lifecycle` supplies `join`,
  `abort_total`, `close` and `margin`; release uses `run.finish_allowance`.
  `RequiredGrace` rounds their sum up to seconds, with no preStop. Production
  uses one `CleanupContext`, detached with `context.WithoutCancel` and bounded
  by RequiredGrace, for the entire cancel sequence and every storage's AbortOwn.
  Backends use the caller's context, never a fresh per-upload abort timeout.
  Shipped values are 1m + 2m + 2m + 30s + 30s = 360s. The chart sets
  `terminationGracePeriodSeconds: 360` and passes that literal to
  `validate --termination-grace-seconds`; the gate rejects insufficient grace.
- **Only the current format is supported.** Remove replaced APIs, decoders,
  aliases and compatibility-only pins with their callers. Do not add versioned
  package names, keys, prefixes or layouts. New backups must round-trip with the
  new binary. Mark potentially asynchronous writes before issuing them; durable
  uncertainty must survive cancellation and release.

## Releasing

Conventional Commits drive the existing pipeline: `image` pushes architecture
images by digest; publish merges them under `master`, uses git-cliff
(`cliff.toml`) to choose the version, claims `v<version>`, tags the image,
packages the chart and publishes release notes.

- `Chart.yaml` keeps placeholders (`0.0.0`, image tag `master`). Never bump them
  or push a tag by hand.
- `Release-As: X.Y.Z[-pre]` in a commit trailer forces the next version. A
  docs-only push runs no workflow; carry it on a non-docs change.
- Breaking configuration, chart or stored-format changes require a major release.
  The plugin-runtime task ships all existing capabilities in one final major
  release; no compatibility layer or intermediate release is required.

## The chart

- `config` is a string, never rendered through `tpl`; its `{{ }}` belong to the
  tool. The chart supplies execution settings, not defaults for required backup
  fields. Those come from the configuration document.
- The config mounts at `/etc/go-cluster-backup/config.yaml` from
  `<fullname>-config`. The catalog defaults to `<fullname>-catalog`, annotated
  `helm.sh/resource-policy: keep`; `catalog.create: false` manages it externally.
  Consumers bind to these names.
- Every container sets `terminationMessagePolicy: FallbackToLogsOnError` so
  pre-run failures carry their last log lines into the previous-run report.
- Node hostPaths reach only the intended pod: `writer.volumes` and
  `writer.volumeMounts` for writers and their stage initContainer;
  `restore.volumes` and `restore.volumeMounts` for restore. `extraVolumes` and
  `extraVolumeMounts` reach every pod, including the config gate; only innocuous
  shared volumes belong there.
- A `values.yaml` change requires `make docs`. The generated chart README must
  not drift. Its long form is `charts/go-cluster-backup/README.md`; the release
  README links there rather than duplicating values.

## Git

Conventional Commits: `type(scope): description`, single line preferred, subject
≤70 characters; a body only when the change is unintelligible without one.

## Binding rules

- **Defaults live in one document, never a plugin constructor.** It is
  `internal/plugins/configuration/defaults/defaults.yaml`. Merge before crossing
  a plugin boundary: objects recursively, present scalars/lists replace, null is
  refused. Defaults never create optional sections, instances or list elements;
  list-element templates use the same merge primitive. Plugins receive complete
  JSON and require every field. Startup, `doctor` and tests prove every field is
  either `required:"true"` or defaulted. `configuration.Secret` is always required.
  Embedded defaults contain neither required values nor credentials; deployment
  config holds secret references, never printed secret bytes. Required means no
  value suits every deployment, or a wrong value risks deletion, lost protection
  or exposure: storage roots/prefixes, credentials, retention tiers/byte budgets,
  delete modes, sensitivity, coverage/exclusion rules, deployment-sized budgets,
  message text and every `restore.apply` option. Only reporting flags
  (`--log-level`, `--output`) carry defaults. `config defaults` prints the document;
  `config effective --config FILE` marks only actually defaulted values.
- **Shape checks do not prove semantics.** Typed defaults reject malformed
  durations, sizes, templates and secret references at startup. `doctor` builds
  examples with defaults to check enums, ranges and combinations; `validate`
  builds declared instances. Nested discovery sources get shape, null and type
  checks without construction; the run checks semantics when building them.
- **One attempt per backend call.** `execution` owns retries, backoff, budgets
  and timeouts from config. A backend never retries inside that accounting unit.
- **Preflight is scoped.** Failed `Check` affects only its instance. Permanent
  storage failure excludes it for the run; transient failure spends a retry.
  `validate --preflight` builds/checks declared instances and discovered sources
  as a run would, reports each instance/problem and exits non-zero on failure.
  Config errors, missing binaries and unavailable required plugins refuse before
  operational I/O. Implement `Checker` for credentials, reachability and mounts.
- **Temporary files belong in configured `scratch_dir`.** Never use process temp
  space, a small cluster `emptyDir`. Remove files on every exit path.
- **Artifact files are 0600 unless config explicitly chooses otherwise.** Spool
  and `restore fetch` outputs are 0600 regardless of sensitivity. Staged bytes
  and their chain record are 0644 to cross containers to uid 1000. Restore modes
  such as sqlite's `file_mode` use the configured value.
- **A kind grows only through an optional interface.** Kind packages own contracts
  and factories, not all consumers. Discover capabilities by call and
  `errors.ErrUnsupported`; never widen an existing kind interface.
- **Planning names enumerated units.** `source.Enumerator` returns `Unit.Name`
  and its dump config, not child instances or artifact names. The planning owner
  builds each child, names the artifact `<envelope>/<unit>` and instance
  `<instance>/<unit>`, and decides enumeration failures in
  `internal/plugins/planning/units.go`.
- **Restore follows the recorded chain, never the current pipeline.** Keep
  manifest JSON keys explicit so Go renames do not silently alter stored data.
  A disabled required decoder gives an explicit refusal. Only the current format
  is supported; compatibility-only pins are not binding.
- **Run kind is identity.** `run --kind` has no default. ID and manifest kinds
  must agree. The inventory/protection owner partitions runs before handing a
  homogeneous list to `retention.Decide`; policies remain kind-blind.
- **The cap belongs to storage-envelope protection, not retention policies.**
  `max_total_bytes` covers every kind. Admit manual writes on exact size or
  refuse; scheduled runs reclaim and report overflow without failing on the cap.
  While producing, reserve `run.max_artifact_bytes`. Multipart may overlap
  production; `CompleteMultipart` requires producer exit 0 and every accepted
  part. Unknown occupancy is never zero. The protection ledger credits the
  current run's verified copies before policy/cap decisions. `MarkUncertain`
  protects the uncertain run's prefix, marks occupancy unknown and closes cap
  eviction.
- **A catalog lease serialises writes.** Discover `catalog.Leaser` by call,
  with `errors.ErrUnsupported` meaning absent. Acquire before any storage mutation.
  Without it, manual runs refuse and scheduled runs are lease-free. Take over
  only on the exact holder pod's terminal phase (scheduled), or absent/changed
  pod plus `run.lease_hard_bound` under `activeDeadlineSeconds`. A clock or absent
  pod alone is not proof. Missing capability, `ErrUnsupported` or failed liveness
  checks count as alive. A bounded scheduled wait that expires and a manual live
  holder refusal both reach common finish: failed report, lease reason, finished
  notification. Retention cannot delete another run's incomplete objects before
  `deadline + finish_allowance`. Inherited uncertainty follows the full-survey
  digest rule above, never elapsed time.
- **Surface failures before the tool starts.** From the exact caller pod, follow
  owner Job, owner CronJob, then its newest other Job, using
  `run.holder_namespace_env`/`holder_pod_env`. Report Job reason/message and each
  container's terminated reason, exit code, `terminated.message` or waiting reason:
  rejected initContainers, image pulls, PSA, pending deadlines, OOMKill and SIGKILL
  must surface. This is read-only; missing RBAC reports
  "previous run status unavailable" without failing the run. Reporting combines
  this with storage manifests only for the finished event.
- **Configuration is strict.** Report all errors together with JSON-pointer paths.
- **Process hygiene:** `log/slog` only. `signal.NotifyContext`, `os.Exit` and
  `log.Fatal` exist only in `cmd/go-cluster-backup/main.go`. Domain operations
  never exit the process and remain repeatable within it.
- **Templates are a SemVer API.** Report fields and the fixed function set
  (`configuration.CoreFuncs`, notifier `TemplateFuncs`) are deployment-visible.
  Test the current surface in
  `internal/plugins/configuration/template_surface_test.go`. This task may break
  labels and fields; remove superseded callers rather than adding aliases.
- **Rules 4/5 exception:** the mandatory everything-is-a-plugin architecture
  keeps the `discovery`, `catalog` and `notifier` seams even with one backend each
  (`kubernetes_labels`, `kubernetes_configmap`, `webhook`).

## Commands

Use CI's Make targets. `make check` needs Go; Linux `-race` also needs a C compiler.

```bash
make check       # vet test lint vuln chart — the whole gate
make vet         # go vet ./...
make test        # unit-cover + test-gate: the unit suite, gated at 100%
make unit-cover  # go test -race -trimpath -timeout=5m -coverprofile=coverage.out ./... (no gate)
make test-gate   # gate an existing coverage.out — no second test run
make cover       # gate the unit+e2e union (needs coverage.out and coverage-e2e.out)
make lint        # rules, app/tools lint, then the hard `//nolint` grep gate
make rules       # structural plugin rules and generated freshness
make generate    # regenerate full/hostless composition and the Floor manifest
make vuln        # govulncheck, pinned in tools/go.mod
make chart       # helm lint --strict, kubeconform (1.31, 1.36 and 1.37), helm-docs drift
make docs        # regenerate charts/go-cluster-backup/README.md from values.yaml
make e2e         # k3d end-to-end suite: E2E_IMAGE=<image in the local docker daemon>
make image       # docker build -t go-cluster-backup .
make hooks       # opt in to .githooks/commit-msg, the commit-message check
```

`make rules` enforces the import graph, runtime Requires, factory IDs, loader
purity, role/command disjointness, the raw-storage boundary, eagerinit and
`plugin.go` presence. It also checks generated freshness and runs in `make lint`.

CI has two jobs: `make lint vuln chart` and `make test e2e cover`. Lint's govet
covers CI vet; keep `make vet` in the local gate. The first job checks every
incoming commit with `.githooks/commit-msg`. Each job has its own Go cache.
For buildkit-cache-dance, `ci.yml`'s `cache-map` must match the Dockerfile's
`go-mod`/`go-build` cache-mount IDs.

`make e2e` builds `go-cluster-backup:e2e` with `--build-arg COVER=1`, or uses
`E2E_IMAGE` from the local daemon. It requires `docker`, `k3d`, `kubectl` and
creates/deletes a cluster with k3d v5.9.0 and `rancher/k3s:v1.37.1-k3s1`.
`E2E_CLUSTER` defaults to `gcb-e2e`; concurrent runs need distinct names because
each run deletes that name first. Seven scenarios cover install/validate hook,
manual run, restore round-trip, previous-run failure, writer lease, rejected
upgrade and k3s_state. Counters in `coverage-e2e/` become `coverage-e2e.out` for
the 100% union gate. Details: `test/e2e/README.md`.

Narrow a fix cycle to the affected package, then run `make check` once before
handover. In a shared task, only the named wave runner runs aggregate gates on
a quiescent snapshot. Application and tools code are gated at 100%; source
writers own their checks, and the docs
writer alone runs `make docs` and updates documentation in each wave.

## Coverage and error rules

- **Coverage is 100%, measured and deterministic.** `make test` gates unit
  coverage; `make cover` gates the unit+e2e union. Each statement block counts
  once when either suite executes it.
- **`//nolint` is banned, with no exception list.** Fix the finding or change the
  code. `make lint` greps for the directive; `nolintlint` remains at its strictest.
- **Impossible errors use `must.Do`** (`internal/plugins/must`); handle reachable
  errors, never wrap them. An unprovokable OS failure gets one unexported
  package-level seam defaulting to the real call, overridden only in tests.
  Exercise conformance `t.Fatal` paths through `fatalReporter` or a subprocess.
  No build tags, `-coverpkg` or assertion-free tests to move coverage. Name test
  files by subject.

## Tests

- Command tests live in flat `internal/plugins/cli`, using synthetic sources and
  `internal/plugins/storage/storagetest`, no network or credentials. White-box
  host tests use `components.Compose` plus `_test.go`-only `AddHost`; never import
  `internal/plugins/all` from package `cli` tests. External tests use `all.Compose` with
  caller-owned factories; cmd tests drive the production entry. Subject tests
  stay private beside implementations; cross-provider proofs use the command
  harness, not production exports added only for tests.
- `cmd/go-cluster-backup/testdata/config.yaml` is the tested reference config.
  A plugin shape change must update it. The main test proves every registered
  field has a required tag or defaults block. The same test validates
  `examples/config/*.yaml`.
- Keep the s3 live test skipped; the offline suite is the gate.
- Use `testing/synctest` for time waits unless a socket is involved. An httptest
  fake releases a held request on `r.Context().Done()`, never a fixed sleep.
