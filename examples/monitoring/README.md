# Monitoring a go-cluster-backup deployment

Two independent signals, because they fail independently:

1. **The run's own report, pushed to a Prometheus Pushgateway** by the webhook
   notifier in [../config/monitoring.yaml](../config/monitoring.yaml). It covers
   the outcome, failed artifacts, coded warnings, retention failures, cap
   evictions and unknown occupancy — things only the process knows.
2. **The cluster's Job, pod and CronJob state, through kube-state-metrics**
   ([prometheusrule.yaml](prometheusrule.yaml)). It covers the runs that never
   started: a rejected initContainer, a pod that never scheduled, an eviction, an
   OOMKill, a CronJob whose Job creation failed.

The tool reports `run_started` and `run_finished`; there is no heartbeat. A run
that dies between the two is visible as a stuck `gcb_run_started` (or a stale
push), never as silence you have to interpret.

## Install the rules

```console
kubectl apply -f examples/monitoring/prometheusrule.yaml
```

Set the rule namespace, the `release` label your Prometheus rule selector
matches, the release namespace (`backup` below) and the writer label (`node-a`
below) to the deployment's own. The push assertions name one writer; duplicate
them for every writer that pushes, or template them from a list. `absent()`
cannot prove a series a rule does not name.

The rules are an example, not a proof against the deployment's Prometheus. They
are exercised by [prometheusrules_test.yaml](prometheusrules_test.yaml) with a
pinned promtool. `promtool test rules` reads plain rule groups, so the command
extracts the PrometheusRule's `spec` into a scratch copy inside the container and
tests that; the shipped CRD is the only source:

```console
docker run --rm -v "$PWD/examples/monitoring:/src:ro" --entrypoint sh \
  prom/prometheus:v3.6.0 -c '
    cd /tmp &&
    awk "f{print} /^spec:/{f=1}" /src/prometheusrule.yaml > rules.extracted.yaml &&
    cp /src/prometheusrules_test.yaml . &&
    promtool test rules prometheusrules_test.yaml'
```

Rehearse them against the Prometheus version the deployment runs before relying
on them; the repository does not start a monitoring stack.

## Pushed series

The example config pushes one Prometheus text body to the grouping key
`job="go-cluster-backup", writer="<this writer>"` on every event. The notifier
uses `PUT`, which replaces the **whole** grouping key: a series absent from the
latest push is gone, and that is what lets a vanished source page. `POST` only
replaces metrics of the same name and would keep a vanished artifact's old
series forever — do not switch the method.

| Series | Meaning |
|---|---|
| `gcb_run_started` | 1 between `run_started` and `run_finished`; 0 after a finished push |
| `gcb_run_outcome{outcome}` | 1 for this run's outcome: delivered, degraded or failed |
| `gcb_run_kind{kind}` | scheduled or manual |
| `gcb_run_artifacts_total`, `gcb_run_artifacts_failed` | artifact counts |
| `gcb_run_warnings_total` | coded **run-level** warnings: catalog mark/settle/clear failures, a stage record's carried warning; an artifact's warnings are not copied here |
| `gcb_artifact_present{artifact}`, `gcb_artifact_ok{artifact}` | 1 when the artifact is in this run's report; `ok` is 1 only for a successful artifact. `artifact` is the manifest identity `<group>/<name>` |
| `gcb_artifact_warnings{artifact}` | that artifact's own coded warnings, e.g. `source.credentials.unsealed` for plaintext credential bytes |
| `gcb_storage_retention_error{storage}` | 1 when the retention pass failed on that storage |
| `gcb_storage_cap_overflow_bytes{storage}` | bytes left above the cap after the scheduled sweep |
| `gcb_storage_cap_evicted_runs{storage}` | runs the cap deleted on that storage |
| `gcb_storage_occupancy_unknown{storage}` | 1 when the ledger cannot prove the occupancy |

Pushgateway also exposes `push_time_seconds` for each grouping key; freshness
alerts read it rather than any value in the report.

Storage names and artifact identities become metric label values, JSON-quoted by
the payload. Keep the configuration's storage names and artifact `<group>/<name>`
identities to letters, digits, `_`, `-`, `.` and `/` — other characters can make
the Pushgateway parser reject the payload (the notification then fails like any
other receiver failure). A failing artifact is also counted in the aggregate,
and its name is in the notification text.

## Expected sources

A discovery with `min_matches: 0` can stop matching a required object while every
other artifact is still uploaded: the run stays `delivered`, and no aggregate
count names the gap. Every source the deployment explicitly expects therefore
needs its own alert on its artifact identity:

- `GoClusterBackupExpectedSourceMissing` asserts
  `absent(gcb_artifact_present{…artifact="volumes/uploads"})`; copy it for every
  expected artifact and writer.
- Under `PUT`, the vanished artifact is absent from the latest push while a
  healthy sibling's series keeps updating, so the alert names the missing
  identity — not a writer problem, which the freshness rules already cover.
- The `unless … gcb_run_started == 1` clause keeps the rule quiet while a run is
  in progress: the started push carries no artifact series. The finished push is
  where presence is judged.

[prometheusrules_test.yaml](prometheusrules_test.yaml) proves both directions: a
fresh writer with one healthy sibling and one missing expected artifact fires
the alert, the same input with the artifact present does not, and a run in
progress suppresses it.

## What the pushed series cannot show

- **No run at all.** Pushgateway keeps the last push forever, so the absence of
  a new push is the only signal. `GoClusterBackupReportStale` and
  `GoClusterBackupNeverReported` read `push_time_seconds`; never alert on the
  last reported success, which stays green after the writer stops.
  `.Run.Previous.LastGoodAt` is the whole run's storage half, not a per-source
  freshness signal: it may name an older run's success, and it must not be
  turned into a "last successful backup" alert.
- **A run killed before it reported.** `gcb_run_started == 1` stays set; the Job
  and pod rules name the failure.
- **A release failure after the finished event.** The catalog release runs after
  the final notification (the finish window keeps a reserve only for removing
  this run's pending mark), so its failure cannot appear in an event already
  sent. It is a structured log line (`lease.release.failed`); alert on it from
  the log store, or accept the next run's lease report as the signal.
- **A process that never starts.** A rejected initContainer, a `FailedCreate`, a
  quota refusal or a Pod Security rejection produces no pod to report from. The
  Job/CronJob rules above are the external signal; the next run's
  `.Run.Previous` then names the previous Job's failure in its own event.

## Kubernetes signals used

`kube_job_status_failed{reason}`, `kube_pod_container_status_terminated_reason`,
`kube_cronjob_status_last_schedule_time` and
`kube_cronjob_status_last_successful_time` from
[kube-state-metrics](https://github.com/kubernetes/kube-state-metrics). The
CronJob names assume the chart's default `<fullname>` for a release named
`backup`; change them with the release name.
