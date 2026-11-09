# go-cluster-backup

Backup and restore for Kubernetes clusters: label-driven discovery, one object per artifact, any number of storages, retention, notifications and restore.

[![CI](https://github.com/vi-latyshev/go-cluster-backup/actions/workflows/ci.yml/badge.svg)](https://github.com/vi-latyshev/go-cluster-backup/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/vi-latyshev/go-cluster-backup)](https://github.com/vi-latyshev/go-cluster-backup/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## Everything is a plugin

The generic `internal/loader` composes compiled plugins. The flat Floor host
`internal/plugins/cli` is registered by the generated root. `assembly` builds from
Config; workflow plugins run backup, stage and restore over planning, execution,
pipeline, transfer, inventory and protection plugins. Storage writes are guarded;
only the protection ledger deletes ([architecture](CLAUDE.md#runtime-composition)).
One document, without a `version` key, combines backup settings with
[`plugins`](docs/configuration.md#plugins) selection and per-type settings.
`plugins check` validates the compiled composition offline.

## Plugin catalogue

Type names are exactly as written in the configuration.

### Databases

| Type | Purpose | Restore |
|---|---|---|
| `postgres` | one PostgreSQL database, `pg_dump` | fetch · apply |
| `redis` | one Redis instance, `redis-cli --rdb` | fetch |
| `sqlite` | one SQLite database, sqlite3 online `.backup` | fetch · apply |

### Files and cluster state

| Type | Purpose | Restore |
|---|---|---|
| `files` | one directory tree as a tar stream | fetch · apply |
| `staged` | a sealed artifact an earlier `stage` left | fetch |
| `k3s_state` | a k3s sqlite datastore plus the server token | fetch |

### Processors

| Type | Purpose |
|---|---|
| `zstd` | streaming zstd compression |
| `age` | streaming age encryption (sealing) |

### Storages

| Type | Purpose |
|---|---|
| `s3` | any S3-compatible endpoint |
| `yandex_disk` | the Yandex Disk REST API, scoped to one folder; bounded parallel listing |
| `local` | a local directory |

### Retention

| Type | Purpose |
|---|---|
| `gfs` | newest run per day, week and month |
| `keep_last` | newest N runs |

### Discovery

| Type | Purpose |
|---|---|
| `kubernetes_labels` | labelled pods and PVCs, listed from the Kubernetes API |

### Catalog

| Type | Purpose |
|---|---|
| `kubernetes_configmap` | writer lease in one ConfigMap; no run history |

The `reporting` plugin derives previous-run information from storage manifests
and the previous Job's status only for finished notifications.

### Notifications

| Type | Purpose |
|---|---|
| `webhook` | posts an event to an HTTP endpoint; text and body are config templates |

`fetch` covers `restore list`, `fetch` and `verify`; `apply` writes a decoded
artifact back — [docs/restore.md](docs/restore.md).

## How it works

Discovery feeds sources, processors and storages. The catalog leases writes;
retention manages stored runs, and notifications report results.

## Install

```console
helm install backup oci://ghcr.io/vi-latyshev/charts/go-cluster-backup --namespace backup --create-namespace --set-file config=backup.yaml
```

Version, release notes, chart version and image tag: [latest release](https://github.com/vi-latyshev/go-cluster-backup/releases/latest).
GitOps: [Flux](examples/flux/) · [Argo CD](examples/argocd/application.yaml).

## Quickstart

```console
kubectl create namespace backup
kubectl -n backup create secret generic cluster-backup-s3 \
  --from-file=access-key-id=./access-key-id --from-file=secret-access-key=./secret-access-key
cp examples/config/minimal.yaml backup.yaml        # edit endpoint, bucket, credentials
helm install backup oci://ghcr.io/vi-latyshev/charts/go-cluster-backup \
  --namespace backup --set-file config=backup.yaml -f examples/values/minimal.yaml
kubectl -n app label pod postgres-0 backup.example/engine=postgres backup.example/instance=postgres
job=manual-$(date +%s)
kubectl -n backup create job --from=cronjob/backup-go-cluster-backup-manual "$job"
kubectl -n backup logs -f "job/$job"
```

## Compatibility

| Component | Supported |
|---|---|
| Kubernetes | ≥ 1.31 |
| Helm | ≥ 3.8 (OCI registry) |
| Platforms | `linux/amd64`, `linux/arm64` |
| PostgreSQL client | 18 — `pg_dump`/`pg_restore` refuse a different server major |
| File volumes | local/hostPath PersistentVolumes on the Job's node |
| `volumePath`, `k3s_state` | single node; sqlite/kine datastore, not embedded etcd |
| S3 credentials | static keys read from mounted files |
| Pod Security | defaults pass `restricted`; `hostPath` features need `privileged` |

## Docs and security

[Configuration](docs/configuration.md) · [restore procedures](docs/restore.md) ·
[chart values](charts/go-cluster-backup/README.md) · [security](SECURITY.md).

Both the image and the chart are signed keyless by this repository's CI.
Replace `X.Y.Z` with the release you install:

```console
cosign verify ghcr.io/vi-latyshev/go-cluster-backup:X.Y.Z \
  --certificate-identity https://github.com/vi-latyshev/go-cluster-backup/.github/workflows/ci.yml@refs/heads/master \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

cosign verify ghcr.io/vi-latyshev/charts/go-cluster-backup:X.Y.Z \
  --certificate-identity https://github.com/vi-latyshev/go-cluster-backup/.github/workflows/ci.yml@refs/heads/master \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Versioning, third party, license

Releases follow Semantic Versioning: breaking configuration, chart or
stored-format changes require a major release. Only the
[current strict backup format](docs/configuration.md#manifest) is supported.
The image ships `redis-cli` from Alpine's `redis` package (AGPL-3.0-only OR
SSPL-1.0) as an external client only.

[Apache-2.0](LICENSE).
