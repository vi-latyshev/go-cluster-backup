{{/* The chart name, overridable. */}}
{{- define "go-cluster-backup.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The object-name prefix. Every generated object name is `<fullname>` plus a
suffix, so a consumer binding a NetworkPolicy or a RoleBinding to one of them
must treat this as stable.
*/}}
{{- define "go-cluster-backup.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* The image reference: repository plus tag, the tag defaulting to appVersion. */}}
{{- define "go-cluster-backup.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/* The ServiceAccount the CronJobs run as. */}}
{{- define "go-cluster-backup.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "go-cluster-backup.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The catalog ConfigMap name. */}}
{{- define "go-cluster-backup.catalogName" -}}
{{- default (printf "%s-catalog" (include "go-cluster-backup.fullname" .)) .Values.catalog.name -}}
{{- end -}}

{{/* Path the configuration document is mounted at, inside every container. */}}
{{- define "go-cluster-backup.configPath" -}}
/etc/go-cluster-backup/config.yaml
{{- end -}}

{{/* Labels on every object. */}}
{{- define "go-cluster-backup.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "go-cluster-backup.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: {{ .Values.component }}
{{- with .Values.labels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Labels on every CronJob pod template. All three CronJobs carry the same set,
so one NetworkPolicy matches every run.
*/}}
{{- define "go-cluster-backup.selectorLabels" -}}
app.kubernetes.io/name: {{ include "go-cluster-backup.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .Values.component }}
{{- end -}}

{{/* A secretName turned into a valid volume name. */}}
{{- define "go-cluster-backup.secretVolumeName" -}}
{{- printf "secret-%s" (.secretName | lower | replace "." "-" | replace "_" "-" | trunc 50 | trimSuffix "-") -}}
{{- end -}}

{{/*
Shared volumeMounts of the writer, restore and stage containers: the config,
the working directories and every configured secret. Takes the root context.
*/}}
{{- define "go-cluster-backup.sharedVolumeMounts" -}}
- name: config
  mountPath: /etc/go-cluster-backup
  readOnly: true
- name: spool
  mountPath: /spool
- name: scratch
  mountPath: /scratch
- name: tmp
  mountPath: /tmp
- name: discovered
  mountPath: /run/discovered
{{- range .Values.secretMounts }}
- name: {{ include "go-cluster-backup.secretVolumeName" . }}
  mountPath: {{ .mountPath }}
  readOnly: true
{{- end }}
{{- end -}}

{{/*
Shared volumes of a pod. Takes a dict: `root` is the root context and
`spoolSize` the size limit of the spool (the restore pod sizes its own).
*/}}
{{- define "go-cluster-backup.sharedVolumes" -}}
{{- $root := .root -}}
- name: config
  configMap:
    name: {{ include "go-cluster-backup.fullname" $root }}-config
- name: spool
  emptyDir:
    sizeLimit: {{ default $root.Values.volumes.spool .spoolSize }}
- name: scratch
  emptyDir:
    sizeLimit: {{ $root.Values.volumes.scratch }}
- name: tmp
  emptyDir:
    sizeLimit: {{ $root.Values.volumes.tmp }}
- name: discovered
  emptyDir:
    medium: Memory
    sizeLimit: {{ $root.Values.volumes.discovered }}
{{- range $root.Values.secretMounts }}
- name: {{ include "go-cluster-backup.secretVolumeName" . }}
  secret:
    secretName: {{ .secretName }}
    {{- with .optional }}
    optional: {{ . }}
    {{- end }}
{{- end }}
{{- with $root.Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
The pod-level fields every writer, restore and hook pod shares. Takes a dict:
`root` is the root context and `podSecurityContext` the pod's securityContext,
empty falling back to the top-level one (a root writer's pod shape is not forced
on the restore pod).
*/}}
{{- define "go-cluster-backup.podSpecCommon" -}}
{{- $root := .root -}}
{{- $podSecurityContext := default $root.Values.podSecurityContext .podSecurityContext -}}
restartPolicy: Never
serviceAccountName: {{ include "go-cluster-backup.serviceAccountName" $root }}
{{- with $root.Values.priorityClassName }}
priorityClassName: {{ . }}
{{- end }}
{{- with $root.Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
securityContext:
  {{- toYaml $podSecurityContext | nindent 2 }}
{{- with $root.Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $root.Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $root.Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
The writer pod template (metadata and spec) of the scheduled and manual
CronJobs. Takes a dict: `root` is the root context, `args` the container's args.
*/}}
{{- define "go-cluster-backup.writerPodTemplate" -}}
{{- $root := .root -}}
metadata:
  labels:
    {{- include "go-cluster-backup.selectorLabels" $root | nindent 4 }}
    {{- with $root.Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with $root.Values.podAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- include "go-cluster-backup.podSpecCommon" (dict "root" $root) | nindent 2 }}
  {{- if $root.Values.stage.enabled }}
  initContainers:
    - name: stage
      image: {{ include "go-cluster-backup.image" $root }}
      imagePullPolicy: {{ $root.Values.image.pullPolicy }}
      args: ["stage", "--config", {{ include "go-cluster-backup.configPath" $root | quote }}]
      terminationMessagePolicy: FallbackToLogsOnError
      securityContext:
        {{- toYaml $root.Values.stage.securityContext | nindent 8 }}
      resources:
        {{- toYaml $root.Values.stage.resources | nindent 8 }}
      {{- with $root.Values.extraEnv }}
      env:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      volumeMounts:
        {{- include "go-cluster-backup.sharedVolumeMounts" $root | nindent 8 }}
        {{- with $root.Values.extraVolumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
        {{- with $root.Values.stage.volumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
  {{- end }}
  containers:
    - name: backup
      image: {{ include "go-cluster-backup.image" $root }}
      imagePullPolicy: {{ $root.Values.image.pullPolicy }}
      terminationMessagePolicy: FallbackToLogsOnError
      args:
        {{- toYaml .args | nindent 8 }}
      env:
        - name: BACKUP_POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: BACKUP_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: BACKUP_POD_UID
          valueFrom:
            fieldRef:
              fieldPath: metadata.uid
        {{- with $root.Values.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      securityContext:
        {{- toYaml $root.Values.securityContext | nindent 8 }}
      resources:
        {{- toYaml $root.Values.resources | nindent 8 }}
      volumeMounts:
        {{- include "go-cluster-backup.sharedVolumeMounts" $root | nindent 8 }}
        {{- with $root.Values.extraVolumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
        {{- with $root.Values.writer.volumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
  volumes:
    {{- include "go-cluster-backup.sharedVolumes" (dict "root" $root) | nindent 4 }}
    {{- with $root.Values.writer.volumes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}

{{/*
The restore pod template. Takes a dict: `root` is the root context and `args`
the restore container's args. NOTES renders this same pod as the one-off
preflight Job, with the preflight args, so the two cannot describe different
mounts.
*/}}
{{- define "go-cluster-backup.restorePodTemplate" -}}
{{- $root := .root -}}
metadata:
  labels:
    {{- include "go-cluster-backup.selectorLabels" $root | nindent 4 }}
    {{- with $root.Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with $root.Values.podAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- include "go-cluster-backup.podSpecCommon" (dict "root" $root "podSecurityContext" $root.Values.restore.podSecurityContext) | nindent 2 }}
  containers:
    - name: restore
      image: {{ include "go-cluster-backup.image" $root }}
      imagePullPolicy: {{ $root.Values.image.pullPolicy }}
      terminationMessagePolicy: FallbackToLogsOnError
      args:
        {{- toYaml .args | nindent 8 }}
      {{- with $root.Values.extraEnv }}
      env:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      securityContext:
        {{- toYaml $root.Values.restore.securityContext | nindent 8 }}
      resources:
        {{- toYaml $root.Values.restore.resources | nindent 8 }}
      volumeMounts:
        {{- include "go-cluster-backup.sharedVolumeMounts" $root | nindent 8 }}
        {{- with $root.Values.extraVolumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
        {{- with $root.Values.restore.volumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
  volumes:
    {{- include "go-cluster-backup.sharedVolumes" (dict "root" $root "spoolSize" $root.Values.restore.spoolSize) | nindent 4 }}
    {{- with $root.Values.restore.volumes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}

{{/*
A Go duration in seconds, for the run.lease_hard_bound the lease invariant
reads. The grammar is time.ParseDuration's, for a non-negative value: an
optional leading `+`, then a sequence of decimal numbers — digits with an
optional fraction, or a fraction alone, but not both empty — each followed by a
unit (ns, us, µs, μs, ms, s, m, h), with `0` alone accepted. The match is
anchored over the whole string, so `1.5h` is 5400 seconds and is never read as
the `5h` a token scan would find inside it, and `.5h`, `3.h` and `+3h` — all of
which Go accepts — are read too. A negative bound, or anything else, fails the
render rather than being read as some other duration.
*/}}
{{- define "go-cluster-backup.durationSeconds" -}}
{{- $duration := . -}}
{{- if not (regexMatch `^\+?0$|^\+?(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h))+$` $duration) -}}
{{- fail (printf "run.lease_hard_bound %q is not a non-negative duration: an optional '+', then a sequence of decimal numbers with an optional fraction, each followed by a unit (ns, us, µs, ms, s, m, h), such as 9000s or 2h30m" $duration) -}}
{{- end -}}
{{- $total := 0.0 -}}
{{- range $token := regexFindAll `([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)` $duration -1 -}}
{{- $scale := 0.0 -}}
{{- if hasSuffix "ms" $token -}}
{{- $scale = 0.001 -}}
{{- else if hasSuffix "us" $token -}}
{{- $scale = 0.000001 -}}
{{- else if hasSuffix "µs" $token -}}
{{- $scale = 0.000001 -}}
{{- else if hasSuffix "μs" $token -}}
{{- $scale = 0.000001 -}}
{{- else if hasSuffix "ns" $token -}}
{{- $scale = 0.000000001 -}}
{{- else if hasSuffix "h" $token -}}
{{- $scale = 3600.0 -}}
{{- else if hasSuffix "m" $token -}}
{{- $scale = 60.0 -}}
{{- else -}}
{{- $scale = 1.0 -}}
{{- end -}}
{{- $number := regexFind `^[0-9]+(\.[0-9]*)?|^\.[0-9]+` $token -}}
{{- $total = addf $total (mulf (float64 $number) $scale) -}}
{{- end -}}
{{- $total -}}
{{- end -}}

{{/*
Render-time checks of the values and of the configuration document they carry.
Called from templates/configmap-config.yaml, which is always rendered, so a bad
input fails the whole `helm template`/`helm install` before anything is applied.
*/}}
{{- define "go-cluster-backup.validateInput" -}}
{{- if not .Values.config -}}
{{- fail "config is required: pass the tool's YAML configuration with --set-file config=backup.yaml" -}}
{{- end -}}
{{- range .Values.extraEnv -}}
{{- if hasKey . "value" -}}
{{- fail "extraEnv entries must use valueFrom (configMapKeyRef or secretKeyRef): a deployment setting belongs in the config document, not in an inline env value" -}}
{{- end -}}
{{- end -}}
{{- $cfg := fromYaml .Values.config -}}
{{- if not $cfg -}}
{{- fail "config is not a YAML document" -}}
{{- end -}}
{{- $bound := dig "run" "lease_hard_bound" "" $cfg -}}
{{- if not $bound -}}
{{- fail "config must set run.lease_hard_bound (the longest any writer can live)" -}}
{{- end -}}
{{- $boundSeconds := include "go-cluster-backup.durationSeconds" (toString $bound) | float64 -}}
{{- if lt $boundSeconds (float64 .Values.activeDeadlineSeconds) -}}
{{- fail (printf "run.lease_hard_bound (%s) is below activeDeadlineSeconds (%d): the lease takeover assumes a writer cannot outlive the bound, so the Job would be killed while its lease still looks alive" (toString $bound) (int .Values.activeDeadlineSeconds)) -}}
{{- end -}}
{{- if .Values.catalog.create -}}
{{- $catalogName := dig "catalog" "config" "name" "" $cfg -}}
{{- if and $catalogName (ne (toString $catalogName) (include "go-cluster-backup.catalogName" .)) -}}
{{- fail (printf "the config names catalog ConfigMap %q but the chart creates %q: set catalog.name to %q, or catalog.create=false to leave the ConfigMap to your own manifests" (toString $catalogName) (include "go-cluster-backup.catalogName" .) (toString $catalogName)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
