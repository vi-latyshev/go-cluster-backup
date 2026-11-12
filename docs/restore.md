# Restore

A restore is deliberate: the tool never restores on its own. In-cluster, the
suspended `<fullname>-restore` CronJob runs whatever `restore.args` names — its
default is an inert `validate --preflight`. A real restore is a change of that
value followed by one hand-started Job:

```console
kubectl create job --from=cronjob/<fullname>-restore restore-$(date +%s) -n backup
```

On a laptop, copy config from `<fullname>-config`, use local secret paths and
run the same binary.

## What you need

- The configuration file the tool reads.
- Storage credentials — in-cluster Secrets, or local files.
- The `age` private key for any **sealed** artifact (an artifact whose pipeline
  includes `seal`). A restore decodes the chain the manifest records, so a sealed
  artifact needs the identity the `age` processor instance names.
- `pg_restore` of the **same major** as the server for database restores — the
  image ships suites for 15–18 and the tool selects the server's exact major —
  and a `psql` of a matching major for the raw globals procedure; `zstd` and
  `age` only for manual unpacking.

## Restore without the original cluster

The original cluster is not required: restore reads storage objects and the
manifests beside them, and the restore path takes no writer lease.

- `restore list`, `fetch` and `verify --depth stored|decoded` work from any
  machine that holds the storage credentials. `verify --depth probed` needs the
  source's own prober — e.g. `pg_restore` — but still no server.
- `restore apply --target discovered` needs a live cluster and the config's
  discovery rules: on a new cluster, label the target workload as the old one was
  labelled, because `discovered` resolves against the live enumeration, never a
  stored one.
- A PVC file tree is extracted on the node by hand
  ([PVC file tree](#pvc-file-tree)); a restore pod mounts no node path.
- A k3s control plane and every vendor snapshot are operator procedures
  ([k3s control plane](#k3s-control-plane), [vendor snapshots](#vendor-snapshots)).
- A run with no manifest is fetchable only by explicit storage and key, as
  stored and unverified.

A manifest written by 2.2.0 or later records optional members an older strict
reader refuses (`changed_files`, coded warnings, `writer_identity`, `elsewhere`
exclusions). Listing, fetching, verifying and applying those runs needs a 2.2.0
or newer binary; a downgrade does not read them.

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
Missing or wrong manifest `kind` excludes a problem run from artifact restore,
retention deletion and protection credit. Restore reads through `guard.Port`,
which implements `storage.Reader` without write or delete methods.

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
`--out -` and a pipe. A payload stored in a cold class (`ICE`, `COLD`, …) must be
retrieved by the provider first: a fetch is a plain `Get` and does not trigger or
wait for a provider-side restore.

Everything a fetch can refuse without the object — the run, the name, the
storage, the plaintext rule — is settled before a byte moves. The request is
resolved first and the plan printed (run, artifact, sensitivity, stored and raw
sizes, and every storage of the walk with whether it holds a copy) before a
transfer that may take minutes starts; a refused request downloads nothing and
writes nothing. A file destination is written as `<out>.partial`, mode 0600
whether or not the artifact is labelled secret, since a dump is as sensitive as
one, and renamed only after the bytes matched the manifest's size and sha256 — a
failure never leaves a half-written file at `<out>`. A name the selected run
does not hold is refused with the names it does hold; when the artifact failed
in that run, the refusal names the nearest run where it is `ok`. A run with no
manifest is not fetchable by artifact at all: only `--storage NAME --key KEY`
reads its objects, as stored and explicitly unverified. An unreadable storage
does not stop the walk — see [Unreachable storage](#unreachable-storage).

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

| Depth | Proves | Needs |
|---|---|---|
| `stored` | a storage — the first in `restore.storage_order` that holds the object — has it at the size the manifest records; the sha256 is compared only where that storage's `Stat` exposes one | storage credentials |
| `decoded` | the processor chain the manifest records reverses | the `age` identity for a sealed artifact |
| `probed` | the named source's `Prober` reads the artifact — `pg_restore` walks the archive without a server | `--target NAME`, or `discovered` |

Every depth resolves `newest-ok` itself, prints the run id it checked and the
storage that served the copy, and exits 1 on a failed verification; a name the
selected run does not hold is a command error, while a verification that ran
and failed keeps its own row. `stored` proves one readable copy, so use
`decoded` or `probed` when the bytes themselves are in question.

`decoded` and `probed` stream: the stored object is downloaded and decoded on
the way through, no decoded copy is written to the restore spool and nothing is
left on disk. A retry downloads the copy again — a stream cannot be rewound —
and one copy spends one `restore.retry` budget across the download, the decode
and the probe. A processor that answers `errors.ErrUnsupported` is not retried:
this config cannot decode the artifact. Every `✓` line of a `decoded` or
`probed` check ends with what an apply of that record would need on the spool.

### Spool sizing

`fetch` needs the stored object (`Bytes`) plus the reserve; `apply` needs
`Bytes + RawBytes` plus the reserve (a staged artifact needs more: its
`RawBytes` is the sealed size); `verify` needs no spool and prints what an
apply would need. The chart mounts the restore spool as an `emptyDir` whose
`sizeLimit` statfs cannot see: exceeding it evicts the pod.

## What a verification trusts

`restore verify` reads the bytes before it decides, but it answers `ok` only
when they match the manifest's sha256; a `stored` `ok` is a readable copy, not
a faithful one. The sha256 sits next to the object, in the same storage, and
the `age` processor does not authenticate a sender — it only decrypts. Whoever
can write to a storage therefore decides what `restore apply` puts on a target.
Bound that with prefix-scoped least privilege: a credential limited to the
deployment's own prefix that grants List, Stat, Get, Put and Delete. All five
are load-bearing — preflight lists the prefix, an upload is verified with a
Stat, the survey reads manifests back, and retention deletes under the same
prefix — so a read-only or write-only credential cannot complete a run, and one
scoped to the prefix keeps the damage of a leaked key inside it. Keep
versioning or object lock on the bucket, and run a restore with a role that is
not the deployment's superuser. `SECURITY.md` states the trust model.

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
refusal is in the postgres restorer, after the artifact was fetched and verified
and before `pg_restore` runs.

**A dump carries one database, not the cluster.** `pg_dump` runs per database:
the dump holds that database's schema and data, and the object owners and
grants it references, but no roles, role memberships or other cluster globals
(`pg_dumpall` territory). A restore reproduces the database only, and every role
an object names must already exist on the target server; a role created by hand
on the source and never recreated by bootstrap is not in the backup. The cluster
half is a separate fetch-only artifact: [PostgreSQL globals](#postgresql-globals).

## PostgreSQL globals

The `postgres_globals` source stores the source cluster's complete
`pg_dumpall --globals-only` output as one artifact that must be sealed: roles,
memberships, role settings and tablespaces. The tool never applies it — no
`restore apply`, no SQL rewriting, no regex substitution, and the probed check
proves only the artifact's completeness, never its SQL. **Treat the archive as
arbitrary SQL from a superuser-capable source.**

The procedure needs an isolated, empty target of the same PostgreSQL major. Roles
and memberships come first, then the per-database restores, then owners, ACLs
and logins:

1. **Fetch and decode** the archive. `--out` decodes the recorded chain; a
   sealed artifact needs the `age` identity. An artifact the config marks
   `sensitivity: secret` — the reference config marks the globals artifact so —
   cannot be written decoded to a file without `--plaintext-to-file`; that flag
   writes the decoded SQL as a 0600 file. `--raw` writes the stored
   (compressed/sealed) bytes for manual unpacking.

   ```console
   go-cluster-backup restore fetch --config backup.yaml \
     --run newest-ok --artifact cluster/globals \
     --out globals.sql --plaintext-to-file
   ```

   To keep the plaintext off the disk, fetch to stdout instead:
   `--out - | psql …` (the recipe below reviews the SQL first, so a file is the
   usual choice for that step).

2. **Review the SQL before running it.** The archive contains `CREATE ROLE` and
   `ALTER ROLE … PASSWORD '<hash>'` for every role, including the role you may
   be connected as; `ALTER ROLE … SET` statements that can hold secrets; and
   `CREATE TABLESPACE` with the source's paths. Pre-create every tablespace
   directory on the target host: `CREATE TABLESPACE` cannot run inside a
   transaction block, so this archive is never wrapped in `--single-transaction`.

3. **Restore with a distinct bootstrap admin** — a role that is not the role the
   archive alters, typically a fresh superuser created for the restore. Running
   the archive as the account whose password it rewrites can lock the operator
   out of the target.

   ```console
   psql -X -v ON_ERROR_STOP=1 -h "$TARGET" -U "$BOOTSTRAP_ADMIN" -d postgres \
     -f globals.sql
   ```

   `ON_ERROR_STOP=1` makes `psql` exit non-zero at the first error. Never read
   past an error and never label a run with an ignored error a complete restore.

4. **Resolve expected bootstrap conflicts explicitly.** A role the target's own
   bootstrap already created (the administrative role, for example) makes its
   `CREATE ROLE` fail. Review that block and either remove it by hand or create
   the roles beforehand and review the resulting `ALTER`s; never rewrite the
   archive with a regex, and never skip an ACL because it fails.

5. **Preserve or explicitly reassign the grantor.** A membership's historical
   grantor must still exist and hold `ADMIN OPTION` on the role (or be the
   bootstrap superuser), otherwise `GRANTED BY` fails. Keep the historical
   grantor where the deployment still has it; otherwise reassign the grant
   deliberately after review and record that decision. A silently skipped
   grant is not a restored ACL.

6. **Check the result.** Compare owners and ACLs on a sample of relations,
   functions and schemas against the source (`\dt+`, `\df+`, `\dn+`, or a query
   over `pg_class`/`pg_proc`/`pg_namespace`), then log in as each application
   role and run its normal query. With `role_hashes: false` the archive carries
   no password hashes: set the passwords through the deployment's own rotation
   and verify a login; the roles are otherwise unable to authenticate.

The archive is only as trustworthy as its source: restore a copy you reviewed,
on a target you isolated, and keep the source cluster reachable only for
comparison, never for a rewrite. The tool never runs this procedure and cannot
vouch for its result; the operator's checked output — exit status, owner/ACL
comparison and login checks — is the restore's receipt.

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

Two sources cover the two k3s datastores. Restore the one that matches the
cluster, and stop k3s while its datastore is replaced: a copy of a live
datastore is not a snapshot.

### k3s_state (sqlite/kine)

`k3s_state` packs the k3s sqlite datastore and the server token as one tar
stream. It is a **sqlite/kine** datastore: a cluster running embedded etcd needs
the next recipe.

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

### k3s_etcd (embedded etcd)

`k3s_etcd` packs the newest finished **uncompressed** etcd snapshot exactly as
k3s saved it (`snapshot.db`, including etcd's trailing SHA256) and the server
token as one tar stream. A compressed snapshot, a short or corrupt one, or one
changed under the read fails the artifact; there is no fallback to an older
snapshot.

1. Stop the workloads you care about and stop k3s on the target server(s).
2. Fetch and unpack to a scratch directory — a sealed artifact needs the `age`
   identity:

   ```console
   go-cluster-backup restore fetch --config backup.yaml \
     --run newest-ok --artifact k3s-etcd --out - \
     | tar -x -C /tmp/restore-etcd
   ```

3. **Restore the server token first.** The embedded-etcd snapshot's bootstrap
   data is encrypted with the server token, so the reset that restores it needs
   the original token already in place — or the same value passed with
   `--token` on the reset command. A server restored with a different token
   cannot decrypt the bootstrap data.

   ```console
   install -o root -g root -m 0600 /tmp/restore-etcd/token \
     /var/lib/rancher/k3s/server/token
   ```

4. Restore the snapshot through k3s's own embedded-etcd procedure — never copy
   files over a live etcd data directory:
   - **Single server:** place the extracted snapshot under
     `/var/lib/rancher/k3s/server/db/snapshots/` (owner `root`, mode 0600) and
     run k3s once with
     `--cluster-reset --cluster-reset-restore-path=<absolute snapshot path>`.
     That run resets the datastore and exits; start k3s normally afterwards.
   - **Multiple servers:** run the cluster reset on one server, then follow
     k3s's embedded-etcd documentation to remove the other servers' member data
     and rejoin them. The vendor owns that ordering; a half-reset cluster must
     not be started as if it were whole.
5. Verify `kubectl get nodes` and the API before restarting workloads. If the
   target ships `etcdutl`, its `snapshot status` is an additional check of the
   extracted snapshot.

A control-plane restore is the vendor's procedure, not an automated `apply`;
this repository's gates do not execute it. Rehearse it on an isolated cluster
before relying on it.

## Vendor snapshots

`collected` ingests a raw file a vendor collector published after its own
semantic check, with the data file's digest in `<path>.sha256`. The source
proves byte identity and freshness only; the vendor tool owns both the
consistency and the restore. Fetch the artifact, decode the recorded chain, and
restore with the vendor's own tool on an isolated target.

- **Vault Raft.** The collector runs `vault operator raft snapshot save` and
  `vault operator raft snapshot inspect` (the inspect must pass) before it
  publishes the data and the marker. Restore on an isolated Vault of the same
  version with `vault operator raft snapshot restore -force <snapshot>`, then
  unseal and check auth methods, policies, mounts and a sample of secrets.
- **ClickHouse.** The vendor `BACKUP` command produces the archive; its own
  prepare/`RESTORE` owns schema and dependency ordering. Restore into an
  isolated server of the recorded version and check table counts and schema.
- **NATS JetStream, natscli v0.5.1.** Pin that CLI version: `nats backup stream
  NAME DIR` snapshots one stream into a directory (consumer snapshots default to
  on), `nats backup validate DIR` checks it, and the collector tars the whole
  **validated directory** into the single `collected` file before publishing the
  marker. Restore: fetch and decode the artifact, untar it, and run `nats backup
  restore stream DIR` into a target stream that does not exist. Do not use an
  assumed older "stream backup" syntax. An account backup can warn and skip
  memory streams: enforce the deployment's declared required stream set, and
  withhold the marker — fail the artifact — if any required stream is missing;
  never publish a silently incomplete account snapshot.

A digest mismatch is a collector publishing its data and then its marker: the run
treats it as transient, retries and reads the finished pair. The collector must
publish the data file completely first and the marker afterwards, never the
reverse; a stale marker (older than `max_age`) is permanent and fails the
artifact — a stopped collector is a stopped backup.

These are operator procedures with vendor-owned semantics. This repository's
automated gates do not execute them; rehearse each on scratch data with the
deployment's pinned vendor versions before relying on it.

A collector runs as an init container of the writer pod (the chart's
`writer.initContainers`): it sees the pod's auto-mounted ServiceAccount token and
the pod's RBAC, because Kubernetes offers no per-container opt-out, so run no
untrusted tool there. The chart merges `terminationMessagePolicy:
FallbackToLogsOnError` into every writer init container, which is what puts a
failed collector's last log lines into `.Run.Previous` and the notifications —
a collector must never print credentials or argv tokens, and a vendor CLI that
does must be wrapped so it does not. Mount collector data through the writer's
own `writer.volumes`; `extraVolumes` reaches every pod, the config-gate hook
included.

## Unreachable storage

`restore list` is all-or-nothing: it surveys every storage in
`restore.storage_order`, and one that cannot be read fails the command, naming
that storage. Narrow it with `--storage NAME` (repeatable) to list the ones that
answer.

`restore fetch` and `restore verify` walk the same order storage by storage, so
an unreadable one costs only its own turn and a copy is still served from
another — the report names the storage that served it. A name the selected run
does not hold is refused with what that run holds; when `newest-ok` finds no run
holding it at all, the answer names the newest run with a manifest that a
readable storage holds, and what that run holds, so it is never a bare "not
found" while some storage answers.

## Incomplete or failed run

- **Incomplete** (`manifest=no`): the run started and never wrote a manifest.
  Its objects exist but nothing describes them, so a restore cannot resolve an
  artifact from them. Retention does judge them: both `keep_last` and `gfs`
  count a run without a verified manifest under `count_incomplete` when it is
  `true`, and with it `false` keep such runs in addition, outside the tiers. A
  scheduled run's cap eviction takes an inactive incomplete run first, and
  retention never deletes another run's incomplete objects younger than
  `run.deadline + run.finish_allowance` — the bound after which its writer
  cannot be alive.
- **Failed**: the manifest exists with `outcome: failed`. A failed artifact has
  no verified copy, so `--run newest-ok` skips it and finds the newest run where
  the artifact is `ok`. `--run <id>` on a failed run still fetches the copy it
  holds, if any.

When a night fails before the tool even starts (a rejected init container, an
`ImagePullBackOff`, a deadline, an OOMKill), there is no run to list at all: the
next run reports that failure from the cluster — in its `run_started` and its
`run_finished` — via `.Run.Previous.Failure`, and the failed earlier attempts of
its own retried Job via `.Run.Previous.Attempts`.

Reports are `run_started` and `run_finished`; no heartbeat or periodic self-check
runs. Both events carry the cluster half of `.Run.Previous` — read once right
after the writer-lease decision — while the storage half (the previous run id,
unfinished and last-good, from storage manifests) exists only for
`run_finished`. The catalog holds only the lease. Monitor Jobs independently.
