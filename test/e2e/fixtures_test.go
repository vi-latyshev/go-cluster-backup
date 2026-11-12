package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Dependency images, pinned by the digest of their multi-arch index so the same
// bytes run on every CI architecture. MinIO is gone from the public registries
// (Docker Hub removed it, quay.io requires a login), so the S3 endpoint is
// SeaweedFS — a real S3 server, not a mock.
//
// A pin is a registry digest. The load path carries a tag, not a digest, so
// each pin is pulled by digest and re-tagged with pinnedTag before the cluster
// ever sees it.
const (
	pinPostgres   = "postgres@sha256:10a5c69a83035ab5239f5620d708520e8b28f5aa1f2b20143e6024532edadfb5"
	pinRedis      = "redis@sha256:896f241f438ab6a210cfb24f573b84bea721a4cbe67f1b4fd07eb638b3b02a7b"
	pinSeaweedFS  = "chrislusf/seaweedfs@sha256:f7a49e3f3972ebed5be2ed4befa4a0c68eb7bdddba6e692d42729ceb545134b8"
	pinEchoServer = "mendhak/http-https-echo@sha256:c4a81744e620ad3cfbffa97374c9c0e545bb3d132b07ebc7745cce6ddee745a1"
)

// pinnedTag is the tag a pin is loaded under: the repository plus the first 12
// hex of its digest, e.g. postgres@sha256:10a5… becomes postgres:e2e-10a5c69a8303.
// It is derived, never hand-written, so a pin and its tag cannot drift.
func pinnedTag(ref string) string {
	repo, digest := splitImage(ref)
	return repo + ":e2e-" + strings.TrimPrefix(digest, "sha256:")[:12]
}

// The tags the pods reference, one per pin.
var (
	imagePostgres   = pinnedTag(pinPostgres)
	imageRedis      = pinnedTag(pinRedis)
	imageSeaweedFS  = pinnedTag(pinSeaweedFS)
	imageEchoServer = pinnedTag(pinEchoServer)
)

// dependencyPins are the registry digests; dependencyImages are the tags the
// cluster runs them under. The two are index-aligned.
var (
	dependencyPins   = []string{pinPostgres, pinRedis, pinSeaweedFS, pinEchoServer}
	dependencyImages = []string{imagePostgres, imageRedis, imageSeaweedFS, imageEchoServer}
)

const (
	secretS3    = "gcb-s3"
	secretPG    = "gcb-postgres"
	secretRedis = "gcb-redis"
	bucket      = "cluster-backup"
	prefix      = "cluster-backup"
	// The database seeded in Postgres, and the table whose rows the restore
	// scenario looks for.
	appDatabase = "app"
	appTable    = "widgets"
)

// installNamespace creates the namespace and the Secrets the configuration's
// credential files resolve.
func (e *Env) installNamespace(t *testing.T, ns string) {
	t.Helper()
	e.apply(t, fmt.Sprintf(`
apiVersion: v1
kind: Namespace
metadata:
  name: %[1]s
---
apiVersion: v1
kind: Secret
metadata: {name: %[2]s, namespace: %[1]s}
stringData:
  access-key-id: e2e-access
  secret-access-key: e2e-secret
---
apiVersion: v1
kind: Secret
metadata: {name: %[3]s, namespace: %[1]s}
stringData:
  password: e2e-postgres-password
---
apiVersion: v1
kind: Secret
metadata: {name: %[4]s, namespace: %[1]s}
stringData:
  password: e2e-redis-password
`, ns, secretS3, secretPG, secretRedis))
}

// installWorkloads deploys Postgres, Redis and the notification receiver,
// labelled exactly as examples/config/minimal.yaml expects.
//
// The discovery selects pods cluster-wide by label, so only ONE namespace in
// the cluster may carry those labels: a second one would be discovered by a
// release whose RBAC does not reach it, and every run would fail on the
// unreadable Secret. The k3s release therefore installs the storage alone.
func (e *Env) installWorkloads(t *testing.T, ns string) {
	t.Helper()
	e.apply(t, e.workloadManifests(ns))
	for _, name := range []string{"postgres", "redis", "echo"} {
		e.waitDeployment(t, ns, name)
	}
	e.seedPostgres(t, ns)
}

// installStorage deploys the S3 endpoint and creates the bucket, then proves the
// endpoint really stores an object and that a pod resolves it by Service name.
func (e *Env) installStorage(t *testing.T, ns string) {
	t.Helper()
	e.apply(t, e.storageManifests(ns))
	e.waitDeployment(t, ns, "seaweedfs")
	e.createBucket(t, ns)
	e.waitS3Write(t, ns)
	e.waitClusterDNS(t, ns)
}

// waitS3Write proves the S3 endpoint stores an object end to end: the bucket,
// the filer and a writable volume of the bucket's collection. `volume.list` is
// not proof — the pinned image's entrypoint injects `-volume.max=0`, so the
// node advertises as many volume slots as its free disk allows, and on a full
// disk every slot can sit in the empty collection while the bucket's own
// collection has none ("No writable volumes and no free volumes left"). A real
// PUT is the check that fails on exactly that, with the server's own answer.
func (e *Env) waitS3Write(t *testing.T, ns string) {
	t.Helper()
	url := "http://localhost:8333/" + bucket + "/e2e-probe/ready"
	waitFor(t, "a real S3 write to bucket "+bucket, 2*time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/seaweedfs", "--",
			"curl", "-sS", "-w", "\n%{http_code}", "-X", "PUT", "--data-binary", "e2e-probe", url)
		if err != nil {
			return false, out
		}
		lines := strings.Split(strings.TrimSpace(out), "\n")
		// The body is kept for the failure message: SeaweedFS answers a write
		// with no writable volume with its own XML error, and that text names
		// the collection.
		return lines[len(lines)-1] == "200", out
	})
	// The probe key is outside the tool's storage prefix, so no survey counts
	// it; removing it keeps the bucket readback clean anyway.
	if out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/seaweedfs", "--",
		"curl", "-sS", "-o", "/dev/null", "-X", "DELETE", url); err != nil {
		t.Logf("removing the S3 probe object: %v\n%s", err, out)
	}
}

// waitClusterDNS proves CoreDNS answers a Service name from a pod. A k3s first
// start pulls its system images — measured at over four minutes on a loaded
// VM — so a run started before CoreDNS is ready fails every lookup with
// "connection refused"; this gate turns that into one clear message.
func (e *Env) waitClusterDNS(t *testing.T, ns string) {
	t.Helper()
	url := "http://seaweedfs." + ns + ".svc.cluster.local:9333/dir/status"
	waitFor(t, "CoreDNS to answer a Service name from a pod", 5*time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/seaweedfs", "--",
			"curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", url)
		if err != nil {
			return false, out
		}
		return strings.TrimSpace(out) == "200", out
	})
}

// workloadManifests is Postgres, Redis and the notification receiver.
func (e *Env) workloadManifests(ns string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata: {name: postgres, namespace: %[1]s}
spec:
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata:
      labels:
        app: postgres
        backup.example/engine: postgres
        backup.example/instance: postgres
    spec:
      containers:
        - name: postgres
          image: %[2]s
          imagePullPolicy: IfNotPresent
          ports: [{containerPort: 5432}]
          env:
            - {name: POSTGRES_DB, value: %[3]s}
            - {name: POSTGRES_USER, value: postgres}
            - name: POSTGRES_PASSWORD
              valueFrom: {secretKeyRef: {name: %[4]s, key: password}}
          readinessProbe:
            exec: {command: ["pg_isready", "-U", "postgres"]}
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: postgres, namespace: %[1]s}
spec:
  selector: {app: postgres}
  ports: [{port: 5432, targetPort: 5432}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: redis, namespace: %[1]s}
spec:
  replicas: 1
  selector: {matchLabels: {app: redis}}
  template:
    metadata:
      labels:
        app: redis
        backup.example/engine: redis
        backup.example/instance: redis
    spec:
      containers:
        - name: redis
          image: %[5]s
          imagePullPolicy: IfNotPresent
          command: ["redis-server", "--requirepass", "$(REDIS_PASSWORD)"]
          ports: [{containerPort: 6379}]
          env:
            - name: REDIS_PASSWORD
              valueFrom: {secretKeyRef: {name: %[6]s, key: password}}
          readinessProbe:
            exec: {command: ["redis-cli", "-a", "$(REDIS_PASSWORD)", "ping"]}
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: redis, namespace: %[1]s}
spec:
  selector: {app: redis}
  ports: [{port: 6379, targetPort: 6379}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: echo, namespace: %[1]s}
spec:
  replicas: 1
  selector: {matchLabels: {app: echo}}
  template:
    metadata: {labels: {app: echo}}
    spec:
      containers:
        - name: echo
          image: %[7]s
          imagePullPolicy: IfNotPresent
          ports: [{containerPort: 8080}]
          env:
            - {name: HTTP_PORT, value: "8080"}
          readinessProbe:
            tcpSocket: {port: 8080}
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: echo, namespace: %[1]s}
spec:
  selector: {app: echo}
  ports: [{port: 8080, targetPort: 8080}]
`, ns, imagePostgres, appDatabase, secretPG, imageRedis, secretRedis, imageEchoServer)
}

// storageManifests is the S3 endpoint: SeaweedFS, a real S3 server.
//
// `-volume.max` is stated explicitly: the pinned image's entrypoint injects
// `-volume.max=0` for `server`, which leaves the node's volume-slot count to its
// free disk. On a full disk that is as few as six slots, all taken by the empty
// collection before a bucket's own collection asks for one, and every S3 write
// then answers 500 "No writable volumes and no free volumes left". A fixed
// count keeps the fixture independent of the runner's disk.
func (e *Env) storageManifests(ns string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata: {name: seaweedfs, namespace: %[1]s}
spec:
  replicas: 1
  selector: {matchLabels: {app: seaweedfs}}
  template:
    metadata: {labels: {app: seaweedfs}}
    spec:
      containers:
        - name: seaweedfs
          image: %[2]s
          imagePullPolicy: IfNotPresent
          args: ["server", "-dir=/data", "-volume.max=64", "-s3", "-s3.port=8333", "-master.port=9333", "-volume.port=8080"]
          ports:
            - {name: s3, containerPort: 8333}
            - {name: master, containerPort: 9333}
          readinessProbe:
            tcpSocket: {port: 8333}
            initialDelaySeconds: 3
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: seaweedfs, namespace: %[1]s}
spec:
  selector: {app: seaweedfs}
  ports:
    - {name: s3, port: 8333, targetPort: 8333}
    - {name: master, port: 9333, targetPort: 9333}
`, ns, imageSeaweedFS)
}

// waitDeployment blocks until a Deployment has a ready replica.
func (e *Env) waitDeployment(t *testing.T, ns, name string) {
	t.Helper()
	waitFor(t, "deployment/"+name+" ready", 3*time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "get", "deployment", name, "-n", ns,
			"-o", "jsonpath={.status.readyReplicas}")
		if err != nil {
			return false, out
		}
		return strings.TrimSpace(out) == "1", "readyReplicas=" + strings.TrimSpace(out)
	})
}

// seedPostgres creates the table the restore scenario asserts on.
func (e *Env) seedPostgres(t *testing.T, ns string) {
	t.Helper()
	// Idempotent, so a second run against the same cluster seeds the same rows.
	sql := fmt.Sprintf("drop table if exists %[1]s; create table %[1]s(id int primary key, note text); insert into %[1]s select g, 'row ' || g from generate_series(1, 42) g;", appTable)
	if out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/postgres", "--",
		"psql", "-U", "postgres", "-d", appDatabase, "-v", "ON_ERROR_STOP=1", "-c", sql); err != nil {
		t.Fatalf("seed postgres: %v\n%s", err, out)
	}
}

// createBucket creates the S3 bucket SeaweedFS does not make on its own.
func (e *Env) createBucket(t *testing.T, ns string) {
	t.Helper()
	cmd := fmt.Sprintf("printf 's3.bucket.create -name %s\\n' | weed shell -master=localhost:9333", bucket)
	waitFor(t, "s3 bucket "+bucket, time.Minute, func() (bool, string) {
		// The command exits non-zero when the bucket already exists, so the
		// output decides, not the exit code.
		out, _ := e.exec("", "kubectl", "exec", "-n", ns, "deploy/seaweedfs", "--", "sh", "-c", cmd)
		return strings.Contains(out, "created bucket") || strings.Contains(out, "already exists"), out
	})
}

// --- image references -----------------------------------------------------

// TestPinnedTagDerivation pins the derived-tag rule. The pod specs, the load and
// the containerd check agree only while a pin and its tag stay in step, and the
// pin is a digest no reader can re-derive by eye.
func TestPinnedTagDerivation(t *testing.T) {
	want := map[string]string{
		pinPostgres:   "postgres:e2e-10a5c69a8303",
		pinRedis:      "redis:e2e-896f241f438a",
		pinSeaweedFS:  "chrislusf/seaweedfs:e2e-f7a49e3f3972",
		pinEchoServer: "mendhak/http-https-echo:e2e-c4a81744e620",
	}
	for pin, tag := range want {
		if got := pinnedTag(pin); got != tag {
			t.Errorf("pinnedTag(%q) = %q, want %q", pin, got, tag)
		}
	}
	if len(dependencyPins) != len(dependencyImages) {
		t.Fatalf("dependencyPins and dependencyImages differ in length: %d and %d", len(dependencyPins), len(dependencyImages))
	}
	for i, pin := range dependencyPins {
		if dependencyImages[i] != pinnedTag(pin) {
			t.Errorf("dependencyImages[%d] = %q, not the tag of %q", i, dependencyImages[i], pin)
		}
	}
}

// TestContainerdRef pins the normalisation the containerd check relies on.
func TestContainerdRef(t *testing.T) {
	cases := map[string]string{
		"postgres":                             "docker.io/library/postgres:latest",
		"postgres:16":                          "docker.io/library/postgres:16",
		"postgres:e2e-10a5c69a8303":            "docker.io/library/postgres:e2e-10a5c69a8303",
		"go-cluster-backup:e2e":                "docker.io/library/go-cluster-backup:e2e",
		"chrislusf/seaweedfs:e2e-f7a49e3f3972": "docker.io/chrislusf/seaweedfs:e2e-f7a49e3f3972",
		"docker.io/go-cluster-backup:e2e":      "docker.io/library/go-cluster-backup:e2e",
		"docker.io/chrislusf/seaweedfs:e2e-x":  "docker.io/chrislusf/seaweedfs:e2e-x",
		"ghcr.io/x/y:1":                        "ghcr.io/x/y:1",
		"registry.example.com/ns/img:tag":      "registry.example.com/ns/img:tag",
		"localhost/gcb:e2e":                    "localhost/gcb:e2e",
		"localhost:5000/img:tag":               "localhost:5000/img:tag",
	}
	for ref, want := range cases {
		if got := containerdRef(ref); got != want {
			t.Errorf("containerdRef(%q) = %q, want %q", ref, got, want)
		}
	}
}

// --- the tool's configuration --------------------------------------------

// toolConfig is the deployment's configuration document: examples/config/minimal.yaml
// with the S3 endpoint, the notification receiver and the namespace filled in,
// plus a Redis rule so the Redis dependency is backed up too.
func (e *Env) toolConfig(ns, catalog string) string {
	return strings.NewReplacer(
		"__NS__", ns,
		"__S3__", "seaweedfs."+ns+".svc.cluster.local",
		"__ECHO__", "echo."+ns+".svc.cluster.local",
		"__CATALOG__", catalog,
	).Replace(toolConfigTemplate)
}

// previousEcho is the notification fragment both events carry: the previous
// Job's failure and the failed earlier attempts of the run's own Job. A run
// that dies before its own report still reaches the receiver through the next
// run's run_started.
const previousEcho = ` previous={{ with .Run.Previous.Failure }}failed job={{ .Job }} reason={{ .Reason }}{{ range .Containers }} container={{ .Container }}{{ with .Terminated }} exit={{ .ExitCode }} reason={{ .Reason }}{{ end }}{{ end }}{{ else }}none{{ end }} attempts={{ with .Run.Previous.Attempts }}{{ .Failed }}{{ else }}none{{ end }}`

const toolConfigTemplate = `run:
  spool_dir: /spool
  spool_reserve_bytes: 1GiB
  max_artifact_bytes: 3GiB
  deadline: 5m
  lease_hard_bound: 1800s

restore:
  spool_dir: /spool
  spool_reserve_bytes: 1GiB
  storage_order: [primary]
  apply:
    postgres: {jobs: 1, no_owner: false, no_acl: false, connect_attempts: 6, connect_interval: 2s}

discoveries:
  workloads:
    type: kubernetes_labels
    config:
      secrets_path: /run/discovered
      exclude: {label: backup.example/ignore, value: "true"}
      undeclared_volumes: {importance: best_effort}
      pods:
        - name: postgres
          selector: "backup.example/engine=postgres"
          covers_volumes: true
          min_matches: 0
          artifact:
            group: databases
            name: '{{ .Namespace }}/{{ label "backup.example/instance" }}'
            importance: required
            sensitivity: plain
          pipeline: [compress]
          source:
            type: postgres
            config:
              address: '{{ label "backup.example/instance" }}.{{ .Namespace }}.svc.cluster.local:{{ port 0 0 }}'
              database: '{{ envValue 0 "POSTGRES_DB" }}'
              username: '{{ envValue 0 "POSTGRES_USER" }}'
              password: {file: '{{ envFile 0 "POSTGRES_PASSWORD" }}'}
              format: custom
              compression: none
              connect_attempts: 6
              connect_interval: 2s
              all_databases: true
              exclude_databases: ["postgres"]
        - name: redis
          selector: "backup.example/engine=redis"
          covers_volumes: false
          min_matches: 0
          artifact:
            group: caches
            name: '{{ .Namespace }}/{{ label "backup.example/instance" }}'
            importance: best_effort
            sensitivity: plain
          pipeline: [compress]
          source:
            type: redis
            config:
              address: '{{ label "backup.example/instance" }}.{{ .Namespace }}.svc.cluster.local:{{ port 0 0 }}'
              password: {file: '{{ envFile 0 "REDIS_PASSWORD" }}'}
              scratch_dir: /scratch
              connect_attempts: 6
              connect_interval: 2s
      volumes: []

processors:
  compress:
    type: zstd
    config: {level: better, window_bytes: 8MiB, decode_window_bytes: 16MiB}

storages:
  primary:
    type: s3
    retention:
      scheduled: {type: keep_last, config: {count: 7, count_incomplete: true}}
      manual: {type: keep_last, config: {count: 3, count_incomplete: true}}
    max_total_bytes: 5GiB
    time_budget: 5m
    config:
      endpoint: http://__S3__:8333
      region: us-east-1
      bucket: ` + bucket + `
      prefix: ` + prefix + `
      access_key_id: {file: /run/secrets/s3/access-key-id}
      secret_access_key: {file: /run/secrets/s3/secret-access-key}
      addressing: path

catalog:
  type: kubernetes_configmap
  config:
    namespace: __NS__
    name: __CATALOG__

notifiers:
  ops:
    type: webhook
    config:
      url: http://__ECHO__:8080/
      method: POST
      headers: {Content-Type: text/plain}
      body_format: text
      timeout: 10s
      events:
        run_started: |
          event=run_started run={{ .Run.RunID }} kind={{ .Run.Kind }}` + previousEcho + `
        run_finished: |
          event=run_finished run={{ .Run.RunID }} kind={{ .Run.Kind }} outcome={{ .Run.Outcome }}` + previousEcho + `
      payload: |
        {{ .Text }}
`

// k3sConfig is the single-node shape: the stage initContainer snapshots the
// node's k3s datastore and the run reads it back with the staged source.
func (e *Env) k3sConfig(ns, catalog string) string {
	return strings.NewReplacer(
		"__NS__", ns,
		"__S3__", "seaweedfs."+ns+".svc.cluster.local",
		"__CATALOG__", catalog,
	).Replace(k3sConfigTemplate)
}

const k3sConfigTemplate = `run:
  spool_dir: /spool
  spool_reserve_bytes: 1GiB
  max_artifact_bytes: 3GiB
  deadline: 5m
  lease_hard_bound: 1800s

restore:
  spool_dir: /spool
  spool_reserve_bytes: 1GiB
  storage_order: [primary]

stage:
  dir: /staged
  sources:
    host_state:
      type: k3s_state
      artifact: {group: control-plane, name: k3s-state, importance: required, sensitivity: secret}
      pipeline: [seal]
      config: {datastore: /k3s-db/state.db, token: {file: /k3s-token}, scratch_dir: /scratch, integrity_check: quick}

sources:
  control_plane:
    type: staged
    artifact: {group: control-plane, name: k3s-state, importance: required, sensitivity: secret}
    pipeline: []
    config: {dir: /staged, name: k3s-state}

processors:
  seal:
    type: age
    config:
      recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"]
      identities: []

storages:
  primary:
    type: s3
    retention:
      scheduled: {type: keep_last, config: {count: 7, count_incomplete: true}}
      manual: {type: keep_last, config: {count: 3, count_incomplete: true}}
    max_total_bytes: 5GiB
    time_budget: 5m
    config:
      endpoint: http://__S3__:8333
      region: us-east-1
      bucket: ` + bucket + `
      prefix: ` + prefix + `
      access_key_id: {file: /run/secrets/s3/access-key-id}
      secret_access_key: {file: /run/secrets/s3/secret-access-key}
      addressing: path

catalog:
  type: kubernetes_configmap
  config:
    namespace: __NS__
    name: __CATALOG__
`

// --- helm values ----------------------------------------------------------

const (
	releaseName = "gcb"
	// The two releases share one cluster, so their names must differ: the
	// ClusterRoles a release renders are cluster-scoped and Helm refuses to
	// hand one to a second release.
	suiteFullname = "gcb"
	k3sFullname   = "gcb-k3s"
	coverMount    = "/gocover"
	coverEnvName  = "gcb-coverage"
)

// catalogName is the catalog ConfigMap of one release; the chart refuses to
// render when the configuration names a different one.
func catalogName(fullname string) string { return fullname + "-catalog" }

// baseValues is the chart's deployment shape for every release: the pinned
// image, the S3 secret mount, the RBAC the discovery needs, a suspended
// scheduled CronJob so no uncontrolled writer can appear mid-run, and — when a
// coverage directory is configured — the hostPath volume and GOCOVERDIR the
// instrumented image writes into.
func (e *Env) baseValues(ns, fullname string, extraVolumes, extraMounts []string) string {
	v := fmt.Sprintf(`fullnameOverride: %[1]s
image:
  repository: %[2]q
  tag: %[3]q
  pullPolicy: IfNotPresent
activeDeadlineSeconds: 900
# The scheduled CronJob is suspended: the suite starts only the Jobs it controls.
# A live schedule could fire near 03:00 UTC and take the writer lease in the
# middle of the manual-run or lease scenario.
suspend: true
catalog:
  name: %[4]s
secretMounts:
  - {secretName: %[5]s, mountPath: /run/secrets/s3}
rbac:
  secretNames: [%[5]s]
  workloadNamespaces: [%[6]s]
`, fullname, e.Repo, e.Tag, catalogName(fullname), secretS3, ns)

	// The coverage volume and GOCOVERDIR go into the same lists the caller's
	// entries do: a second `extraVolumes:` key would silently replace the first.
	volumes := append([]string{}, extraVolumes...)
	mounts := append([]string{}, extraMounts...)
	env := []string{}
	if e.CoverDir != "" {
		volumes = append(volumes, fmt.Sprintf("  - name: gocov\n    hostPath: {path: %s, type: Directory}", coverNodeDir))
		mounts = append(mounts, fmt.Sprintf("  - {name: gocov, mountPath: %s}", coverMount))
		env = append(env, fmt.Sprintf("  - name: GOCOVERDIR\n    valueFrom: {configMapKeyRef: {name: %s, key: GOCOVERDIR}}", coverEnvName))
	}
	for _, block := range []struct {
		key     string
		entries []string
	}{{"extraVolumes", volumes}, {"extraVolumeMounts", mounts}, {"extraEnv", env}} {
		if len(block.entries) > 0 {
			v += block.key + ":\n" + strings.Join(block.entries, "\n") + "\n"
		}
	}
	return v
}

// coverageConfigMap is the ConfigMap the writer and restore containers read
// GOCOVERDIR from (the chart refuses an inline env value).
func (e *Env) coverageManifests(ns string) string {
	if e.CoverDir == "" {
		return ""
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata: {name: %s, namespace: %s}
data:
  GOCOVERDIR: %s
`, coverEnvName, ns, coverMount)
}

// writeFiles writes the config, the base values and any extra values into a
// fresh directory and returns their paths.
func writeFiles(t *testing.T, files map[string]string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	out := map[string]string{}
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		out[name] = p
	}
	return out
}
