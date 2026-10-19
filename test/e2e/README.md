# End-to-end suite

The shipped image and the shipped Helm chart, on a real k3s cluster (k3d — the
tool's primary target). Unit tests prove the code; this proves the artifact a
deployment installs: a real install, a real manual run, a real restore into
Postgres, the previous-run report, the writer lease, a rejected upgrade, and the
k3s datastore stage.

Off unless `GCB_E2E=1`, so `go test ./...` stays fast and needs no cluster.

## Environment

| Variable | Required | Meaning |
|---|---|---|
| `GCB_E2E` | yes | `1` runs the suite; anything else skips every test |
| `GCB_E2E_IMAGE` | yes | the image under test, in the local docker daemon, e.g. `go-cluster-backup:e2e` |
| `GCB_E2E_CLUSTER` | no | k3d cluster name, default `gcb-e2e` |
| `GCB_E2E_NAMESPACE` | no | namespace prefix, default `gcb-e2e` |
| `GCB_E2E_COVERDIR` | no | **absolute** host directory for coverage data |
| `GCB_E2E_K3S_IMAGE` | no | k3s image, default `rancher/k3s:v1.37.1-k3s1` |

The suite creates the cluster itself when it is missing, and deletes it again
when it created it — so a local run and a CI run use the same flags. It never
reads or writes the caller's kubeconfig: it asks `k3d kubeconfig get` for the
named cluster and writes a temp file, and `k3d cluster create` runs with
`--kubeconfig-update-default=false --kubeconfig-switch-context=false`. Setup
takes a checksum of `~/.kube/config` and teardown fails the run if it changed.

## Running it locally

```console
docker build -t go-cluster-backup:e2e .
GCB_E2E=1 GCB_E2E_IMAGE=go-cluster-backup:e2e \
GCB_E2E_COVERDIR=/tmp/gcb-e2e-coverage \
go test -count=1 -timeout=20m ./test/e2e/...
```

The cluster is created as

```console
k3d cluster create "$GCB_E2E_CLUSTER" --image rancher/k3s:v1.37.1-k3s1 --wait \
  --volume "$GCB_E2E_COVERDIR:/var/lib/gocover@server:*"
```

(the `--volume` only when `GCB_E2E_COVERDIR` is set). The suite then loads
`$GCB_E2E_IMAGE` and the four dependency images into the node's containerd
itself — see *Images* below.

Point `GCB_E2E_COVERDIR` at a directory the docker daemon can share (`$TMPDIR`
is fine on Docker Desktop); the suite creates it and chmods it `0777`, because
`fsGroup` is not applied to a `hostPath`.

The directory must exist when the cluster is created and must **not** be deleted
or replaced while it lives: the node's bind mount names the inode, so a fresh
directory of the same name leaves every pod failing with `StartError: ... mount
src=/var/lib/gocover ... no such file or directory`. Delete the cluster before
clearing the directory.

## What it deploys

One k3d cluster, two Helm releases (the main lifecycle and the k3s stage), and
per namespace: Postgres 18, Redis, SeaweedFS (S3) and an HTTP echo receiver, all
pinned by multi-arch digest. MinIO is gone from the public registries (Docker
Hub removed it, quay.io requires a login), so the S3 endpoint is SeaweedFS — a
real S3 server, not a mock.

## Images

Every dependency is pinned by the digest of its multi-arch index, so the same
bytes run on every CI architecture. The pin is what is pulled; each is then
loaded under a tag derived from that same digest — `postgres@sha256:77f5…`
becomes `postgres:e2e-77f585114c32` — and that tag is what the pod specs name,
with `imagePullPolicy: IfNotPresent`, so nothing is pulled from the internet
inside the cluster.

The load is the suite's own `docker save` piped into the node's
`ctr images import`, not `k3d image import`. Two things go wrong in k3d's
`image import`: its image lookup cannot resolve a digest-only reference against
a clean daemon, and its tools mode imports with `--all-platforms`, which fails
on a multi-arch index whose non-host blobs `docker save` never wrote — while
k3d reports success regardless, so the failure is swallowed and surfaces
minutes later as an `ImagePullBackOff`. (A pod can be given a digest reference;
the tag is only what the load path can carry.) After the load the suite confirms
every ref is in the node's containerd, so a dropped image fails at setup with
its name.

## Scenarios

| Subtest | Proves |
|---|---|
| `install` | the chart installs; the pre-install `validate` hook ran; every object exists |
| `manual_run` | a manual Job completes; the catalog records it `delivered`; the receiver got `run_finished` |
| `restore` | `restore list` shows the run; `restore apply --mode side-by-side` puts the rows in a database beside the original |
| `previous_run_failure` | a Job that failed before the tool started (exit 7) is reported by the next run's notification |
| `writer_lease` | with a claim left behind by a killed holder, a second manual run is refused, never a second writer |
| `upgrade_rejected` | an invalid config fails at the hook and the installed release is untouched |
| `TestK3sState` | the `stage` initContainer snapshots the node's k3s datastore and the run uploads it |

## Coverage

With `GCB_E2E_COVERDIR` set, the suite gives every pod `GOCOVERDIR` and a
`hostPath` volume through the chart's `extraEnv`/`extraVolumes`/
`extraVolumeMounts`, which the writer, restore, `stage` initContainer and
`validate` hook containers all honour. Every process that exits normally writes
its counters there.

Coverage data is flushed at exit: a Job killed with `SIGKILL` (eviction,
`--force` deletion, an OOM kill) loses its counters, so a Job that runs to a
`Succeeded`/`Failed` pod phase is what produces data. A container killed by
`activeDeadlineSeconds` gets `SIGTERM` first, which the tool handles gracefully,
so that path still flushes.

Convert and merge:

```console
go tool covdata textfmt -i="$GCB_E2E_COVERDIR" -o=coverage-e2e.out
```

`make test`'s coverage step already merges by block key, so appending
`coverage-e2e.out` to the merged profile needs no new logic. A green run writes
one `covcounters.*` file per tool process that exited.

## CI contract

The suite owns the cluster, so CI needs one command:

```console
GCB_E2E=1 \
GCB_E2E_IMAGE=go-cluster-backup:e2e \
GCB_E2E_CLUSTER=gcb-e2e \
GCB_E2E_COVERDIR="$PWD/coverage-e2e" \
go test -count=1 -timeout=20m ./test/e2e/...
```

- **k3d** v5.9.0 (any v5.x works). **k3s** `rancher/k3s:v1.37.1-k3s1`.
- **image**: `docker build -t go-cluster-backup:e2e --build-arg COVER=1 .`, where
  `COVER=1` builds with `-cover -covermode=atomic`. The suite pulls the four
  dependency images by digest, re-tags each and loads all five into the node's
  containerd itself (see *Images*), then verifies each ref is present.
- **coverage**: `GCB_E2E_COVERDIR` is a host directory bind-mounted into the
  node at `/var/lib/gocover`; the pods write there directly, so nothing is
  copied out. `make e2e` then runs
  `go tool covdata textfmt -i="$GCB_E2E_COVERDIR" -o=coverage-e2e.out` — the
  suite never writes the profile itself.
- **timeout**: 20 minutes. Measured: ~2 minutes with the cluster and images
  already present, ~8 minutes cold (cluster create, four image pulls, five image
  imports, two releases, nine Jobs).
