

Scheduled Kubernetes cluster backups. Renders go-cluster-backup as CronJobs
(scheduled, manual, restore), the configuration ConfigMap the tool reads, the
catalog ConfigMap it writes, and the least-privilege RBAC its discovery,
catalog and previous-run inspection need.

**Homepage:** <https://github.com/vi-latyshev/go-cluster-backup>

## Source Code

* <https://github.com/vi-latyshev/go-cluster-backup>

## Requirements

Kubernetes: `>=1.31.0-0`

## What it installs

| Object | Name | Notes |
|---|---|---|
| CronJob | `<fullname>` | the scheduled run, `schedule` (default `0 3 * * *`) |
| CronJob | `<fullname>-manual` | never scheduled; start one Job from it by hand |
| CronJob | `<fullname>-restore` | never scheduled; its args come from `restore.args` |
| ConfigMap | `<fullname>-config` | the configuration, mounted at `/etc/go-cluster-backup/config.yaml` |
| ConfigMap | `catalog.name` (default `<fullname>-catalog`) | the writer lease the tool writes; no `data`, annotated `helm.sh/resource-policy: keep` |
| ServiceAccount | `<fullname>` | |
| ClusterRole + binding | `<fullname>` | `list` on pods, PVCs and PVs |
| Role + binding | `<fullname>` | `get`/`list` on pods and Jobs, `list` on Events, in the release namespace (lease liveness and the previous-run report) |
| Role + binding | `<fullname>-catalog` | `get`/`patch` on the catalog ConfigMap alone |
| Role + binding | `<fullname>-secrets` | `get` on the Secrets in `rbac.secretNames` |
| ClusterRole | `<fullname>-workload-access` | `get` on Secrets and ConfigMaps; **not bound** until a namespace is listed in `rbac.workloadNamespaces` |
| Job (hook) | `<fullname>-validate` | `pre-install,pre-upgrade` config gate |

`<fullname>` is the release name plus the chart name unless `fullnameOverride`
says otherwise. Bind your NetworkPolicies and RoleBindings to these names; they
change only when `fullnameOverride` changes.

## Install

The chart is published to GHCR as an OCI artifact:

```console
helm install backup oci://ghcr.io/vi-latyshev/charts/go-cluster-backup \
  --namespace backup --create-namespace \
  --set-file config=backup.yaml
```

Without `--version`, Helm installs the newest release; pin the version taken
from the [latest release](https://github.com/vi-latyshev/go-cluster-backup/releases/latest).

`config` is required and carries the tool's own YAML configuration document —
the same file `go-cluster-backup validate --config` reads. It is mounted
verbatim, so its own template syntax is never expanded by Helm. Install into
the namespace the configuration's `catalog.config.namespace`
names, because the catalog ConfigMap is created in the release namespace.

A pre-install/pre-upgrade hook Job runs `go-cluster-backup validate` against the
new configuration before anything else is applied. It is offline — a secret
reference is resolved, never read — so it needs no secret mount. The chart sets
`terminationGracePeriodSeconds: 360` by default and passes the same literal as
`validate --termination-grace-seconds` to check the effective cleanup budget.
Raise chart grace when raising lifecycle budgets; the gate rejects a value below
the computed bound. When validation fails, install or upgrade aborts; with Flux's
`upgrade.remediation.strategy: rollback` the previous release stays in place.

### Upgrading: the image and the configuration move together

The tool reads the configuration with a strict decoder: any member the running
image does not know, or any field it demands and the document lacks, is an error
at startup, before a single plugin is built — and that run produces no backup.
When `values.config` and the image tag are updated by different paths (a values
change merged now, an image automation or a manual `image.tag` bump later), a
pod can run the previous image against the new document and fail instantly.
Update both together, or pin `image.tag` and confirm it matches the release
before the first scheduled run. The pre-upgrade hook proves only that the new
document suits the image the hook itself ran; it cannot see a pod that still
carries an older tag.

## Example values and manifests

- `examples/values/minimal.yaml` with `examples/config/minimal.yaml`
- `examples/values/k3s-single-node.yaml` with `examples/config/k3s-single-node.yaml`
- `examples/flux/` — OCIRepository + HelmRelease (version-pinned, rollback on failure)
- `examples/argocd/application.yaml` — the same chart as an Argo CD Application

## Pod Security

The default values pass the `restricted` Pod Security Standard: non-root, no
capabilities, a read-only root filesystem and a `RuntimeDefault` seccomp
profile. Two things move a deployment out of it:

- `stage.enabled: true` reads host paths owned by root and runs as root (the
  `k3s-single-node` example). No standard below `privileged` admits a hostPath
  volume — `baseline` rejects it just as `restricted` does — so that deployment
  needs a namespace labelled
  `pod-security.kubernetes.io/enforce=privileged`, or exempted from enforcement.
- A storage root readable only by root needs `securityContext.runAsUser: 0` and
  `securityContext.runAsNonRoot: false`. That alone still passes `baseline`.

### Where a node hostPath goes

A hostPath holding node state or secrets reaches one pod, never all of them:

- `writer.volumes` / `writer.volumeMounts` — the scheduled and manual writer
  pods only. The `stage` initContainer runs inside them, so its own
  `stage.volumeMounts` may reference `writer.volumes`.
- `restore.volumes` / `restore.volumeMounts` — the restore pod only, for a
  `files` restore that reads a PersistentVolume root.
- `extraVolumes` / `extraVolumeMounts` reach **every** pod that runs the tool —
  the writer, the restore Job and the config-gate hook Job alike — so only an
  innocuous shared volume (a coverage directory, a CA bundle) belongs there.

The writer's `securityContext` is the writer container's alone. The restore and
hook containers carry `restore.securityContext` and `hooks.securityContext`,
which default to the same restricted non-root shape, so a root writer never
makes a restore or a config gate run as root.

## Restores

The restore CronJob is suspended and its default args are inert (`validate
--preflight`). A restore is a deliberate change of `restore.args` — a `restore
fetch`, `restore apply` or `restore verify` invocation with an explicit run and
artifact — followed by one hand-started Job:

```console
kubectl create job --from=cronjob/<fullname>-restore restore-$(date +%s) -n <namespace>
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image | object | `{"pullPolicy":"IfNotPresent","repository":"ghcr.io/vi-latyshev/go-cluster-backup","tag":""}` | Image repository of the go-cluster-backup image. |
| image.tag | string | `""` | Image tag. Empty means the chart's appVersion, so chart and image versions never drift. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| imagePullSecrets | list | `[]` | Image pull secrets for the image above. |
| config | string | `""` | REQUIRED. The tool's YAML configuration document, as a string: `--set-file config=backup.yaml`. It is rendered verbatim into a ConfigMap and mounted at /etc/go-cluster-backup/config.yaml; it is never passed through `tpl`, because its `{{ }}` are the tool's own template syntax. The chart fails to render when this is empty. |
| nameOverride | string | `""` | Override the chart name used in object names. |
| fullnameOverride | string | `""` | Override the full object-name prefix. Every generated object name derives from this, so a consumer binding a NetworkPolicy or a RoleBinding to a generated name must keep it stable. |
| component | string | `"backup"` | Value of the app.kubernetes.io/component label on every object and on every CronJob pod template. |
| labels | object | `{}` | Extra labels added to every object and to every CronJob pod template. |
| terminationGracePeriodSeconds | int | `360` | Seconds Kubernetes gives a writer or restore pod between SIGTERM and SIGKILL (the stage initContainer shares the writer pod, so the same value covers it). The tool's shutdown needs `G = join + finish_allowance + abort_total + close + margin`, read from the configuration's `run.finish_allowance` and `plugins.config.lifecycle`; at the shipped defaults G = 1m + 2m + 2m + 30s + 30s = 360s. The config-gate hook passes this same value to `validate --termination-grace-seconds`, which refuses a configuration whose own G exceeds it — raise this when the configuration raises any input, never lower it below the effective G. |
| schedule | string | `"0 3 * * *"` | Cron schedule of the scheduled run. |
| timeZone | string | `""` | IANA time zone of the schedule. Empty omits the field (the cluster's UTC). |
| suspend | string | `""` | Suspend the scheduled CronJob. Left out of the manifest when empty, so a pause applied to the live object is not reset by an upgrade. The manual and restore CronJobs are always suspended. |
| activeDeadlineSeconds | int | `8400` | Seconds a scheduled Job may live before Kubernetes kills it. The chart refuses to render when this exceeds the config's run.lease_hard_bound: the lease takeover assumes a writer cannot outlive that bound. |
| backoffLimit | int | `1` | Retries of a failed scheduled Job. |
| podFailurePolicy | object | `{"rules":[{"action":"Count","onPodConditions":[{"status":"True","type":"DisruptionTarget"}]},{"action":"FailJob","onExitCodes":{"containerName":"backup","operator":"In","values":[1,2]}}]}` | podFailurePolicy of the scheduled Job, rendered as is. Its onExitCodes rule must name the writer container (`backup`). |
| successfulJobsHistoryLimit | int | `1` | Completed Jobs kept by each CronJob. |
| failedJobsHistoryLimit | int | `3` | Failed Jobs kept by each CronJob. |
| concurrencyPolicy | string | `"Forbid"` | concurrencyPolicy of every CronJob. |
| manual.enabled | bool | `true` | Render the manual-run CronJob `<fullname>-manual`. It never fires on its own: an operator starts one Job from it by hand. |
| restore.enabled | bool | `true` | Render the restore CronJob `<fullname>-restore` (always suspended). |
| restore.args | list | `["validate","--preflight","--config","/etc/go-cluster-backup/config.yaml"]` | Args of the restore container. The default is inert: a read-only preflight of the configuration. A real restore is a deliberate change of this list (restore fetch/apply/verify with an explicit run and artifact). |
| restore.activeDeadlineSeconds | int | `3600` | Seconds a restore Job may live before Kubernetes kills it. |
| restore.spoolSize | string | `"8Gi"` | fetch needs the stored object (Bytes) plus the reserve; apply needs Bytes + RawBytes plus the reserve (a staged artifact needs more: its RawBytes is the sealed size); verify needs no spool and prints what an apply would need. The chart mounts the restore spool as an emptyDir whose sizeLimit statfs cannot see: exceeding it evicts the pod. This value is that sizeLimit, for the restore pod's spool (the config's restore.spool_dir, mounted at /spool). |
| restore.resources | object | `{"limits":{"cpu":"1","ephemeral-storage":"9Gi","memory":"512Mi"},"requests":{"cpu":"200m","ephemeral-storage":"4Gi","memory":"256Mi"}}` | Resources of the restore container. |
| restore.podSecurityContext | object | `{}` | pod securityContext of the restore pod. Empty inherits the top-level `podSecurityContext`; a non-empty value is a FULL replacement, never a merge, so a `fsGroup`-only override drops the `seccompProfile` and the `restricted` Pod Security Standard then rejects the pod — state the complete context:   podSecurityContext:     runAsNonRoot: true     runAsUser: 1000     runAsGroup: 1000     fsGroup: 1000     fsGroupChangePolicy: OnRootMismatch     seccompProfile: {type: RuntimeDefault} Set it when the restore pod's fsGroup must differ from the writer's: a root writer leaves the pod fsGroup at 0, and a non-root restore could then not write its spool. |
| restore.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000}` | container securityContext of the restore container, independent from the writer's `securityContext`: a root writer does not make the restore run as root. The default passes the `restricted` Pod Security Standard; a files restore reading a root-owned volume root sets `runAsUser`/`runAsGroup` 0 and `runAsNonRoot` false here. |
| restore.volumes | list | `[]` | Volumes only the restore pod declares, on top of the shared ones. A files restore's PersistentVolume root belongs here, never in `extraVolumes`, which reaches the hook pod too. |
| restore.volumeMounts | list | `[]` | VolumeMounts only the restore container mounts, on top of the shared ones. Pair them with `restore.volumes`. |
| writer.volumes | list | `[]` | Volumes only the writer pods declare, on top of the shared ones. A node hostPath holding node state or secrets — the k3s datastore, a VPN database — belongs here, never in `extraVolumes`, which reaches the restore and hook pods too. |
| writer.volumeMounts | list | `[]` | VolumeMounts only the writer container mounts, on top of the shared ones. Pair them with `writer.volumes`; the restore container never sees them. |
| writer.initContainers | list | `[]` | Init containers of the scheduled and manual writer pods, rendered before the stage initContainer and the writer container; an entry may mount any pod volume, including `writer.volumes`. The chart merges `terminationMessagePolicy: FallbackToLogsOnError` into every entry: a failed collector's last log lines then reach the previous-run report instead of a bare exit code. An explicit conflicting policy fails the render; stating FallbackToLogsOnError is accepted. The chart grants these containers no ServiceAccount rights of their own, but Kubernetes auto-mounts the pod's service account token into every container of the pod, so a collector sees that token and the pod's RBAC — Kubernetes offers no per-container opt-out; run no untrusted tool there, and never log credentials or argv tokens (vendor CLIs can print them, and a log tail can enter a notification). Example:   initContainers:     - name: sqlite-snapshot       image: example.com/collector:1       command: ["/collector", "/node/state"]       volumeMounts:         - name: writer-node           mountPath: /node |
| stage.enabled | bool | `false` | Render the `stage` initContainer on the writer pod. The config's `stage.dir` must be an emptyDir of this one pod — the chart's `spool`, or one declared in `writer.volumes` — never a hostPath or a PVC shared between pods. A stage publishes into an empty directory it owns and never deletes; it refuses a non-empty `stage.dir` (a retained pair makes it re-run against data a previous stage left). The writer lease is taken by `run`, after `stage`, so a shared directory also lets another pod's stage republish the pair between this pod's stage and its run. |
| stage.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsGroup":0,"runAsNonRoot":false,"runAsUser":0}` | securityContext of the stage initContainer. It reads host paths owned by root, so it defaults to root without capabilities. |
| stage.resources | object | `{"limits":{"cpu":"500m","memory":"512Mi"},"requests":{"cpu":"50m","memory":"128Mi"}}` | Resources of the stage initContainer. |
| stage.volumeMounts | list | `[]` | Extra volumeMounts of the stage initContainer, on top of the shared ones. Pair them with writer.volumes, which the stage container's pod declares. |
| serviceAccount.create | bool | `true` | Create a ServiceAccount for the CronJobs. |
| serviceAccount.name | string | `""` | Name of the ServiceAccount. Empty uses the fullname when create is true, `default` otherwise. |
| serviceAccount.annotations | object | `{}` | Annotations of the ServiceAccount. |
| rbac.create | bool | `true` | Create the ServiceAccount's RBAC. The ClusterRole lists every pod, claim and volume cluster-wide and gets nodes by name, which the node-local discovery needs to prove it runs on the node a hostPath volume lives on. With create false, carry both grants yourself: a missing nodes get fails the affected pinned or local artifacts with a warning, never a silent read of another node's directory. |
| rbac.workloadNamespaces | list | `[]` | Namespaces whose Secrets and ConfigMaps the tool may read, through one RoleBinding each onto the `<fullname>-workload-access` ClusterRole. That ClusterRole is not bound to anything when this list is empty. |
| rbac.secretNames | list | `[]` | Names of Secrets in the release namespace the tool may get (the credentials its configuration's templates resolve). |
| catalog.create | bool | `true` | Create the catalog ConfigMap the tool keeps its writer-lease record in. It is created without `data` and annotated `helm.sh/resource-policy: keep`, so an uninstall never takes a live claim away. Set to false when the ConfigMap is managed by the consumer's own manifests — such a manifest must never declare `data` either: every reconcile of it would revert the lease record the tool wrote. |
| catalog.name | string | `""` | Name of the catalog ConfigMap. Empty uses `<fullname>-catalog`, and the chart refuses to render when the config names a different one. |
| secretMounts | list | `[]` | Secrets mounted read-only into every container, for the credential files the configuration references. |
| extraVolumes | list | `[]` | Extra volumes of every pod — the writer, the restore Job and the config-gate hook Job alike. Only an innocuous shared volume belongs here (a coverage directory, a CA bundle); a node hostPath holding state or secrets goes in `writer.volumes` or `restore.volumes`, which one pod alone declares. |
| extraVolumeMounts | list | `[]` | Extra volumeMounts of every container that runs the tool, the stage initContainer and the config-gate hook included. Pair them with `extraVolumes`. |
| extraEnv | list | `[]` | Extra environment variables of every container that runs the tool, the stage initContainer and the config-gate hook included. Each entry must use valueFrom (configMapKeyRef or secretKeyRef); an inline `value` fails the render, because a deployment setting belongs in the configuration document. |
| volumes | object | `{"discovered":"16Mi","scratch":"512Mi","spool":"4Gi","tmp":"64Mi"}` | Size limits of the emptyDir volumes the tool works in. |
| volumes.spool | string | `"4Gi"` | Spool: one post-processor artifact at a time (the config's run.spool_dir). |
| volumes.scratch | string | `"512Mi"` | Scratch: the working directory a plugin writes temporary files into. |
| volumes.tmp | string | `"64Mi"` | Temp: the process temp directory. |
| volumes.discovered | string | `"16Mi"` | Discovered: a tmpfs the discovery writes resolved credentials into. |
| podSecurityContext | object | `{"fsGroup":1000,"fsGroupChangePolicy":"OnRootMismatch","runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000,"seccompProfile":{"type":"RuntimeDefault"}}` | pod securityContext of the writer pods, and the default the restore pod inherits while `restore.podSecurityContext` is empty. The config-gate hook pod carries its own (`hooks.podSecurityContext`) and never inherits this one, so a root writer does not make the hook pod root. |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000}` | container securityContext of the writer container. A deployment whose storage root is only readable by root overrides this; the restore and hook containers carry their own (`restore.securityContext`, `hooks.securityContext`) and never inherit it. |
| resources | object | `{"limits":{"cpu":"1","ephemeral-storage":"5Gi","memory":"512Mi"},"requests":{"cpu":"200m","ephemeral-storage":"1Gi","memory":"256Mi"}}` | Resources of the writer container. |
| podLabels | object | `{}` | Extra labels of every CronJob pod template. |
| podAnnotations | object | `{}` | Annotations of every CronJob pod template. |
| priorityClassName | string | `""` | priorityClassName of every pod. A silently skipped backup is worse than a delayed dev pod, so a production deployment should raise this. |
| nodeSelector | object | `{}` | nodeSelector of every pod. |
| tolerations | list | `[]` | Tolerations of every pod. |
| affinity | object | `{}` | Affinity of every pod. |
| hooks.enabled | bool | `true` | Render the config-gate hook Job. |
| hooks.backoffLimit | int | `0` | backoffLimit of the hook Job. |
| hooks.ttlSecondsAfterFinished | int | `600` | ttlSecondsAfterFinished of the hook Job. |
| hooks.resources | object | `{"limits":{"cpu":"100m","memory":"128Mi"},"requests":{"cpu":"10m","memory":"32Mi"}}` | Resources of the hook container. |
| hooks.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000}` | container securityContext of the hook container, independent from the writer's `securityContext`. The offline check reads no node path, so it must never run as root; the default passes the `restricted` Pod Security Standard. |
| hooks.podSecurityContext | object | `{"fsGroup":1000,"fsGroupChangePolicy":"OnRootMismatch","runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000,"seccompProfile":{"type":"RuntimeDefault"}}` | pod securityContext of the hook pod. Independent from the top-level `podSecurityContext`: the hook reads no node path, so a root writer must not make its pod root. The default passes the `restricted` Pod Security Standard. |
