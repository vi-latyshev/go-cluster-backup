# Contributing

## Development setup

The Go version is the one in `go.mod` (the `go` directive). The linter, the
vulnerability scanner and the chart tools are pinned as tool dependencies in
`tools/go.mod`, so a checkout lints with the versions CI runs — never with a
`golangci-lint` from `PATH`. The chart gate needs `helm` on `PATH`
(`make chart` runs `helm lint`, `kubeconform` and `helm-docs`).

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

`make e2e` needs `docker`, `k3d` and `helm`: it builds the `COVER=1` image itself
(or takes `E2E_IMAGE`), the suite creates and deletes its own k3d cluster, and
`make cover` then gates the union. See [test/e2e/README.md](test/e2e/README.md).

## Commits

Conventional Commits: `type(scope): description`, single line preferred; a body
only when the change is unintelligible without one. Types: `feat`, `fix`,
`refactor`, `perf`, `test`, `docs`, `chore`, `ci`, `build`, `style`, `revert`.

The type decides the release: `feat` is a minor, `fix`, `perf`, `revert` and
`fix(deps)` are patches, and `type!:` or a `BREAKING CHANGE:` footer is a major;
`docs`, `test`, `ci`, `chore`, `refactor`, `style` and `build` release nothing.
The subject is the line users read in the release notes; a non-Conventional
commit is left out of them.

## Adding a plugin

Everything is a plugin: sources, processors, storages, retention, discovery,
catalog, notifier. A new plugin is one package plus one registration line.

1. **Where it lives.** `internal/plugins/<kind>/<name>/`, one package, in the
   repository. There is no public Go API — a plugin is a pull request here, not
   an importable module.

2. **Registration.** Add one line to the list in `internal/cli/register.go`
   (`Generic`). That is the whole cost of adding it to the distribution.

3. **Config.** The config file is strict: an unknown member, a missing field and
   a bad enum are all errors, reported with JSON-pointer paths. Every field of
   the plugin's `Config` struct is exactly one of two things, and a test proves
   it:
   - marked `required:"true"` — the deployment must state it; or
   - present in `internal/defaults/defaults.yaml` under
     `plugins/<kind>/<type>` — the one place a default lives.

   A `plugin.Secret` field is always required and holds a file reference, never
   a literal. No default lives in a constructor or in Go code.

4. **Conformance kit.** Each kind ships a kit that proves the contract: a
   `Check` runs at preflight, `Describe` names the artifact, and the kit drives
   the plugin through it. Import the kind's kit package (for example
   `internal/source/sourcetest`) and run it against the new implementation.

5. **Stored format.** A plugin's type name and its stored-format defaults (a
   layout key, a catalog key, the format a written dump is read with) are part
   of what runs already on disk carry. They are never renamed: a survey lists
   the old prefix and sees nothing, retention stops deleting, a restore finds
   nothing. Such a value is a default pinned by a test in
   `internal/defaults/defaults_test.go`; changing one is a deliberate break of
   every existing run.

## Layout

All Go sources live under `internal/`; only `cmd/go-cluster-backup/` is outside
it. The root keeps non-source files: `go.mod`/`go.sum`, `tools/`, `examples/`,
`charts/`, `test/`, `Dockerfile`, `LICENSE`, `README.md` and this file.
