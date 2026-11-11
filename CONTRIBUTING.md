# Contributing

## Development setup

The Makefile derives `GOTOOLCHAIN` from go.mod's `go` directive and downloads
that toolchain once if needed. A newer `tools/go.mod` is rejected. That separate
module pins lint, vulnerability, chart tools and Helm to CI's versions; use
`go tool`, never those tools from `PATH`.

`make check` needs only Go and a network for the first download, plus a C
compiler on Linux for `-race`.

```bash
go build ./...
go run ./cmd/go-cluster-backup --help
```

## The gate

```bash
make check
```

`make check` is `vet test lint vuln chart`; run it before opening a pull
request. A change to runtime behaviour also runs `make e2e` and then
`make cover` — the union of the unit and e2e profiles — because the coverage
gate is 100% of statements in both. Narrow a fix cycle to one target or one
package (`go test -race ./internal/<pkg>/...`).

Two rules the gate enforces mechanically:

- **`//nolint` is banned, with no exception list.** Fix the finding or change the
  code; `make lint` greps for the directive and fails, and `nolintlint` stays at
  its strictest as a backstop.
- **Coverage must be 100.0%, not "about".** `make test` gates the unit profile,
  `make cover` the unit+e2e union. Nothing may move the number by construction:
  no `-coverpkg`, no build tags, no assertion-free tests.

`make rules` checks the import graph, Requires, factory IDs, loader purity,
role/command disjointness, raw-storage access, eagerinit, `plugin.go` presence
and generated freshness. It runs inside `make lint`. Regenerate tracked outputs
with `make generate`, never by editing them.

Its go/types pass loads every import through `go list` for linux/amd64,
linux/arm64 and darwin/arm64 with `CGO_ENABLED=0`, and refuses an incomplete type
model: an unresolved import, a type error, or a file no checked platform builds.
A platform-specific file for another GOOS requires extending `platforms` in
`tools/rules/typeinfo.go`, and `make rules` needs the root module's dependencies
in the module cache (or network).

`make e2e` needs `docker`, `k3d` and `kubectl`: it builds the `COVER=1` image
itself (or takes `E2E_IMAGE`), the suite creates and deletes its own k3d
cluster, and `make cover` then gates the union. See
[test/e2e/README.md](test/e2e/README.md).

## Commits

Conventional Commits: `type(scope): description`, single line preferred; a body
only when the change is unintelligible without one. Types: `feat`, `fix`,
`refactor`, `perf`, `test`, `docs`, `chore`, `ci`, `build`, `style`, `revert`.

Run `make hooks` to install the commit-msg hook; its rules live in `.commitlint.yaml`.

The type decides the release: `feat` is a minor, `fix`, `perf`, `revert` and
`fix(deps)` are patches, and `type!:` or a `BREAKING CHANGE:` footer is a major;
`docs`, `test`, `ci`, `chore`, `refactor`, `style` and `build` release nothing.
The subject is the line users read in the release notes; a non-Conventional
commit is left out of them.

## Adding a plugin

The loader composes plugins. `assembly` builds from Config; workflow plugins
receive typed options and direct collaborators. Plugins own backup, stage,
restore and all supporting application services.

1. **Where it lives.** `internal/plugins/<kind>/<name>/`, one package, in the
   repository. There is no public Go API — a plugin is a pull request here, not
   an importable module.

2. **Registration.** Declare the plugin identity as literal `var Spec =
   loader.Spec{...}` in `plugin.go`. Libraries have no `Apply`; contributors use
   it for factories, services or roles. Factory metadata uses
   `instances.FactorySpec` or `storage.FactorySpec`, not the identity `Spec`.
   Factory IDs derive from `kind/type`; application providers declare a stable ID.
   Run `make generate` and include `internal/plugins/all/{zz_generated.go,floor.json}`
   and `internal/plugins/all/components/zz_generated.go`. Hostless
   `components.Compose` adds non-host plugins; `all.Register` adds components and
   hosts over fresh factories. `all.Compose` uses caller-owned factories.
   Flat `internal/plugins/cli` is a Floor host registered only by the generated
   root; its `Apply` adds roles, never calls `loader.Add`. Floor roots (`cli`,
   `workflow-backup`) and their dependency closures cannot be disabled.
   Library plugins are always active.

3. **Config.** The config file is strict: an unknown member, a missing field and
   a bad enum are all errors, reported with JSON-pointer paths. Every field of
   the plugin's `Config` struct is exactly one of two things, and a test proves
   it:
   - marked `required:"true"` — the deployment must state it; or
   - present in `internal/plugins/configuration/defaults/defaults.yaml` under
     `plugins.config.<kind/type>` or `plugins.config.<id>`.

   Defaults and deployment config share `plugins: {bundles, disabled, config}`;
   there is no document `version` key. Precedence is defaults, deployment per-type
   settings, then instance config. This also covers nested discovery sources and
   retention. Partial settings are checked even for disabled plugins; settings
   never create instances. A required value may come from deployment per-type or
   instance settings, never embedded defaults. `configuration.Secret` is always
   required and holds a file reference, never a literal. No default lives in Go.
   Provenance escapes IDs per RFC 6901: `/plugins/config/source~1postgres/format`.

4. **Conformance kit.** Each kind ships a kit that proves the contract: a
   `Check` runs at preflight, `Describe` names the artifact, and the kit drives
   the plugin through it. Import the kind's kit package (for example
   `internal/plugins/source/sourcetest`) and run it against the new implementation.

5. **Stored format.** Keep explicit JSON keys and restore from the recorded
   processor chain, never the current pipeline. Missing or wrong manifest `kind`
   makes a protected problem run, never disposable data or a verified copy.
   Restore depends on read-only `storage.Reader`. Storage factories receive
   `storage.BuildInput{CallParallelism}` from the instance envelope; a backend
   must not add a second concurrency limit.

6. **Mutation authority.** Workflow consumers read through `guard.Port` and
   write through Handle/Uploader/Inventory under a live Authority. Only the
   protection ledger holds Remover and deletes; raw instances stay behind
   `internal/plugins/storage/internal`. Transfer Sweep uses the full multipart
   inventory; AbortOwn cleans up only this invocation's exact upload IDs.

## Layout

Runtime behavior lives in `internal/plugins/`, including the flat CLI host.
Outside it are the generic `internal/loader`, thin `cmd/go-cluster-backup/` and
build-time tooling (`tools/gen`, `tools/rules`) in the separate tools module.
The root holds `go.mod`/`go.sum`, `examples/`, `charts/`, `test/`, `Dockerfile`,
`LICENSE` and docs.
