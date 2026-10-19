# Restore

A restore is deliberate: the tool never restores on its own. In-cluster, the
suspended `<fullname>-restore` CronJob runs whatever `restore.args` names — its
default is an inert `validate --preflight`. A real restore is a change of that
value followed by one hand-started Job:

```console
kubectl create job --from=cronjob/<fullname>-restore restore-$(date +%s) -n backup
```

Everything below also works from a laptop: copy the configuration out of the
`<fullname>-config` ConfigMap, repoint the secret file paths at local files, and
run the same `go-cluster-backup` binary.

## What you need

- The configuration file the tool reads.
- Storage credentials — in-cluster Secrets, or local files.
- The `age` private key for any **sealed** artifact (an artifact whose pipeline
  includes `seal`). A restore decodes the chain the manifest records, so a sealed
  artifact needs the identity the `age` processor instance names.
- `pg_restore` of the **same major** as the server for database restores; `zstd`
  and `age` only for manual unpacking.

## List runs

```console
go-cluster-backup restore list --config backup.yaml
go-cluster-backup restore list --config backup.yaml --output json
```

`list` prints one line per run and storage — `run <id> storage=<name>
kind=<scheduled|manual> manifest=yes outcome=… started=… duration=… artifacts=<n>`
and then the artifact names, exactly as `--artifact` takes them. A run without a
manifest is listed as `manifest=no`, with no outcome — it started and never
finished, or was killed. `--run newest-ok` selects, **per artifact**, the newest
run where that artifact is `ok`: a degraded run may still hold the copy you need.

## Fetch an artifact

```console
go-cluster-backup restore fetch --config backup.yaml \
  --run newest-ok --artifact app/postgres --out db.dump
```

The `--artifact` value is the name as `restore list` prints it: the group is not
part of it, and a name may itself contain `/`. `--out -` streams the artifact on
stdout (reports go to stderr). Stored bytes are **decoded** by default — the
processor chain is recorded in the manifest; `--raw` writes them as stored. A
decoded `secret` artifact goes to a file only with `--plaintext-to-file` — prefer
`--out -` and a pipe.

Manual unpacking applies to stored bytes (`--raw`, or `--storage … --key …`); the
`.zst`/`.age` members are single streams:

| Stored object | By hand |
|---|---|
| `*.dump.zst`, `*.rdb.zst`, `*.tar.zst` | `zstd -d < object` (then `tar -xpf -` for the tar ones) |
| `*.db.zst.age` | `age -d -i identity.txt < object \| zstd -d > file.db` |
| `*.tar.zst.age` | `age -d -i identity.txt < object \| zstd -d \| tar x -C /tmp/restore` |

A post-rollout check without restoring anything:

```console
go-cluster-backup restore verify --config backup.yaml \
  --run <run-id> --artifact app/postgres --depth probed --target discovered
```

## One Postgres database

1. **Stop the writers** — scale the Deployments that use the database to 0 (and
   suspend whatever reconciler would bring them back).
2. **Apply side-by-side** — the tool creates `<database>_restore_<run-id>` beside
   the live database. Set `restore.args` to a `restore apply` with `--target
   discovered --mode side-by-side` (in-cluster; `--target discovered` resolves the
   target through the config's discovery rules), or `fetch` the artifact from a
   laptop and run `pg_restore` by hand.
3. **Verify the copy** — row counts, `\dt`, whatever the database needs.
4. **Swap the names** — stop the remaining writers, then e.g.
   `ALTER DATABASE app RENAME TO app_broken; ALTER DATABASE app_restore_<run> RENAME TO app;`.
5. **Resume** the Deployments.
6. **Drop the old database** once satisfied.

**A database the server no longer has** cannot be a `--target discovered` target:
discovery matches a unit among the live enumeration only. `apply` says so and
names what the instance does hold. Create the empty database first, let it be
discovered, then apply.

**`in-place`** exists for when the volume has no room for a second copy: it
replaces the database in one transaction (`pg_restore --clean --if-exists
--single-transaction`) and demands `--mode in-place` **and** `--confirm` equal to
the target's origin verbatim (the origin is in the manifest; `restore list
--output json` shows it). Prefer side-by-side. In-place requires `jobs: 1` — the
tool refuses it otherwise before anything runs.

## PVC file tree

A file tree is restored **on the node**, by hand: the restore pod mounts nothing
under the node's storage root and its root filesystem is read-only, so `restore
apply --target discovered` resolves a volume artifact and then fails against that
unmounted path.

1. Stop the owning workload.
2. As root on the node, extract straight into the volume (the tar contains the
   tree itself, no leading date directory). The bound PV's `local.path` is in
   `kubectl get pv`:

   ```console
   go-cluster-backup restore fetch --config backup.yaml \
     --run newest-ok --artifact app/uploads --out - | tar -xpf - -C "$TARGET"
   ```

   `fetch` reverses the compression chain by default, so the stream is already
   the plain tar. From a laptop, fetch to a file first, copy it over, then
   `tar -xpf file.tar -C "$TARGET"` — `-p` as root preserves owners and hardlinks.
3. Start the owner again.

## Redis

Fetch the `.rdb.zst` and unpack it to `dump.rdb`. Then, depending on how the
instance is configured:

- **`appendonly yes`** (the instance ignores `/data/dump.rdb`): stop the pod,
  stage `dump.rdb` at `/data/dump.rdb`, remove `/data/appendonlydir`, start with
  `--appendonly no`, then `redis-cli -a "$PASSWORD" CONFIG SET appendonly yes` to
  rebuild the AOF, restore the original args and verify with `DBSIZE`.
- **A cache with no snapshot** (`emptyDir`, no persistence) has nothing to
  restore: a restart loses it by design, and the run never backed it up.

## k3s control plane

The `k3s_state` source snapshots the k3s sqlite datastore and the server token as
one tar stream. It is a **sqlite/kine** datastore: a cluster running embedded
etcd is not covered.

1. Stop the writers you care about, and stop k3s (or restore the datastore while
   the service is down).
2. Fetch and unpack the artifact to a scratch directory:

   ```console
   go-cluster-backup restore fetch --config backup.yaml \
     --run newest-ok --artifact k3s-state --out - \
     | tar -x -C /tmp/restore-control-plane
   ```

   A sealed artifact needs the `age` identity mounted where the config names it.
3. Replace the datastore and the token from the extracted `state.db` and `token`,
   preserving ownership and mode, then start k3s.
4. Verify the cluster answers (`kubectl get nodes`) before restarting workloads.

## Unreachable storage

`restore list` walks `restore.storage_order`. A storage that cannot be read is
reported on its own line and the walk continues, so a fetch can still be served
from another storage. If **no** storage holds the object, the answer names the
newest run a storage *can* read and what that run holds — never a bare "not
found". Narrow the walk with `--storage NAME` (repeatable).

## Incomplete or failed run

- **Incomplete** (`manifest=no`): the run started and never wrote a manifest.
  Its objects exist but nothing describes them, so retention never judges them
  and a restore cannot resolve an artifact from them. They are swept by a later
  run on the same storage.
- **Failed**: the manifest exists with `outcome: failed`. A failed artifact has
  no verified copy, so `--run newest-ok` skips it and finds the newest run where
  the artifact is `ok`. `--run <id>` on a failed run still fetches the copy it
  holds, if any.

When a night fails before the tool even starts (a rejected init container, an
`ImagePullBackOff`, a deadline, an OOMKill), there is no run to list at all: the
next run's notification reports that failure from the cluster, via
`.Run.Previous.Failure`.
