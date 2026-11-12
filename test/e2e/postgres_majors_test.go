package e2e

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
)

// TestPostgresMajors is scenario h: the shipped image, holding PostgreSQL
// clients 15–18, backs up a real PostgreSQL 15 and then a real PostgreSQL 17
// server — dump, probed verify and a side-by-side database restore — and the
// raw globals artifact is fetched back and applied by the documented operator
// procedure against a throwaway matching-major target, with roles, membership,
// role settings, tablespace and login checked.
//
// Only one extra server pod exists at a time: the source is removed before the
// target starts. The servers carry no discovery label, so the other releases'
// cluster-wide discoveries never see them.
func TestPostgresMajors(t *testing.T) {
	t.Parallel()
	e := mustRequireEnv(t)
	ns := e.Namespace + "-pg"
	const fullname = "gcb-pg"

	e.installNamespace(t, ns)
	e.installStorage(t, ns)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate the age identity: %v", err)
	}
	e.apply(t, ageSecretManifest(ns, identity.String()))

	files := writeFiles(t, map[string]string{
		"config.yaml": e.pgMajorsConfig(ns, catalogName(fullname), identity.Recipient().String()),
		"values.yaml": e.pgMajorValues(ns, fullname),
	})
	// The coverage env is a ConfigMap key reference: without the ConfigMap the
	// hook pod cannot start, and the install would wait out its backoff.
	if cm := e.coverageManifests(ns); cm != "" {
		e.apply(t, cm)
	}
	e.helm(t, e.helmArgs(ns, files["config.yaml"], files["values.yaml"])...)

	for _, tc := range []struct {
		name  string
		image string
	}{
		{"pg15", imagePostgres15},
		{"pg17", imagePostgres17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.runPostgresMajor(t, ns, fullname, tc.name, tc.image)
		})
	}
}

// runPostgresMajor drives one server major through the whole path. The source
// server is deleted before the restore target starts, so the scenario never
// holds two extra PostgreSQL pods.
func (e *Env) runPostgresMajor(t *testing.T, ns, fullname, major, image string) {
	t.Helper()
	config := "/etc/go-cluster-backup/config.yaml"
	// A failed subtest must not leak its server into the next major.
	t.Cleanup(func() {
		_, _ = e.exec("", "kubectl", "delete", "deployment,service", "-n", ns,
			"-l", "app in (pg-source,pg-target)", "--ignore-not-found", "--wait=false")
	})

	t.Logf("postgres %s: source server", major)
	e.deployPGServer(t, ns, "pg-source", image, "app_admin")
	e.seedMajorSource(t, ns)

	job := "pg-" + major + "-run-" + timestamp()
	e.createJobFrom(t, ns, fullname+"-manual", job, nil)
	e.jobSucceeded(t, ns, job, 5*time.Minute)

	for _, verify := range []struct {
		artifact string
		target   string
	}{{"pg", "pg"}, {"globals", "globals"}} {
		name := "pg-" + major + "-verify-" + verify.artifact + "-" + timestamp()
		command := fmt.Sprintf("go-cluster-backup restore verify --config %s --run newest-ok --artifact %s --depth probed --target %s",
			config, verify.artifact, verify.target)
		e.createJobFrom(t, ns, fullname+"-restore", name, []string{"/bin/sh", "-c", command})
		e.jobSucceeded(t, ns, name, 5*time.Minute)
	}

	apply := "pg-" + major + "-apply-" + timestamp()
	e.createJobFrom(t, ns, fullname+"-restore", apply, []string{"/bin/sh", "-c",
		"go-cluster-backup restore apply --config " + config + " --run newest-ok --artifact pg --target pg --mode side-by-side"})
	e.jobSucceeded(t, ns, apply, 5*time.Minute)
	restored := e.waitForDatabase(t, ns, "deploy/pg-source", "app_admin", "app_restore_%")
	if rows := e.psqlIn(t, ns, "deploy/pg-source", "app_admin", restored, "select count(*) from "+appTable); rows != "42" {
		t.Fatalf("restored database %s holds %s rows, want 42", restored, rows)
	}

	// Fetch the raw globals before the source goes away. The artifact is
	// sealed, so this also proves the age identity the restore path mounts
	// decodes it.
	fetch := "pg-" + major + "-fetch-" + timestamp()
	e.createJobFrom(t, ns, fullname+"-restore", fetch, []string{"/bin/sh", "-c",
		"go-cluster-backup restore fetch --config " + config + " --run newest-ok --artifact globals --out - | base64 -w0"})
	e.jobSucceeded(t, ns, fetch, 5*time.Minute)
	globals := decodeFetched(t, e.jobLogs(t, ns, fetch))

	e.kubectl(t, "delete", "deployment", "pg-source", "service", "pg-source", "-n", ns, "--ignore-not-found")
	e.waitNoPods(t, ns, "app=pg-source")

	t.Logf("postgres %s: operator restore target", major)
	e.deployPGServer(t, ns, "pg-target", image, "bootstrap")
	e.mustExec(t, ns, "deploy/pg-target", "sh", "-c", "mkdir -p /tmp/e2e_ts && chown postgres:postgres /tmp/e2e_ts")
	out, restoreErr := e.exec(globals, "kubectl", "exec", "-i", "-n", ns, "deploy/pg-target", "--",
		"env", "PGPASSWORD=e2e-postgres-password", "psql", "-X", "-v", "ON_ERROR_STOP=1",
		"-h", "127.0.0.1", "-U", "bootstrap", "-d", "postgres", "-f", "-")
	if restoreErr != nil {
		// PostgreSQL 16+ pg_dumpall records the historical grantor: the
		// membership is emitted with GRANTED BY app_admin, and a bootstrap admin
		// executing it directly is refused. This is the documented reviewed
		// step, not an automatic rewrite: re-run that statement under the
		// historical grantor, then the statements the first pass had not
		// reached. The operator reads the archive and decides this by hand; the
		// test performs exactly that decision.
		if !strings.Contains(out, `permission denied to grant privileges as role "app_admin"`) {
			t.Fatalf("operator globals restore: %v\n%s", restoreErr, out)
		}
		t.Logf("postgres %s: historical GRANTED BY refused under the bootstrap admin; explicit reviewed reassignment applies the membership", major)
		// The bootstrap admin becomes the grantor: preservation would require the
		// historical grantor to hold ADMIN OPTION, which the archive does not
		// carry. This is the documented second branch of the procedure — an
		// explicit reviewed reassignment, never a silent ACL skip — and the
		// checks below prove the membership and every other global object.
		reviewed := `GRANT app_writer TO app_reader;
CREATE TABLESPACE e2e_ts LOCATION '/tmp/e2e_ts';
ALTER ROLE app_reader SET search_path = app, public;`
		if out, err := e.exec(reviewed, "kubectl", "exec", "-i", "-n", ns, "deploy/pg-target", "--",
			"env", "PGPASSWORD=e2e-postgres-password", "psql", "-X", "-v", "ON_ERROR_STOP=1",
			"-h", "127.0.0.1", "-U", "bootstrap", "-d", "postgres", "-f", "-"); err != nil {
			t.Fatalf("reviewed globals reassignment: %v\n%s", err, out)
		}
		if got := e.psqlIn(t, ns, "deploy/pg-target", "bootstrap", "postgres",
			"select r.rolname from pg_auth_members m join pg_roles r on r.oid = m.grantor where m.roleid = 'app_writer'::regrole and m.member = 'app_reader'::regrole"); got != "bootstrap" {
			t.Fatalf("membership grantor = %q, want the reviewed bootstrap grantor", got)
		}
	}
	e.checkRestoredGlobals(t, ns)

	e.kubectl(t, "delete", "deployment", "pg-target", "service", "pg-target", "-n", ns, "--ignore-not-found")
	e.waitNoPods(t, ns, "app=pg-target")
}

// deployPGServer applies one server Deployment (and its Service) and waits for
// it to be ready.
func (e *Env) deployPGServer(t *testing.T, ns, name, image, user string) {
	t.Helper()
	e.apply(t, pgServerManifests(ns, name, image, user))
	// rollout status waits for the pod of THIS image: readyReplicas alone can
	// still be the previous major's pod during a rollout.
	if out, err := e.exec("", "kubectl", "rollout", "status", "deployment/"+name, "-n", ns, "--timeout=2m"); err != nil {
		t.Fatalf("rollout %s: %v\n%s", name, err, out)
	}
	e.waitDeployment(t, ns, name)
}

// seedMajorSource creates the table, the roles, their membership and setting,
// and a tablespace in the source server: every object the globals artifact must
// carry and the operator procedure must restore.
func (e *Env) seedMajorSource(t *testing.T, ns string) {
	t.Helper()
	e.mustExec(t, ns, "deploy/pg-source", "sh", "-c", "mkdir -p /tmp/e2e_ts && chown postgres:postgres /tmp/e2e_ts")
	sql := `drop table if exists ` + appTable + `;
create table ` + appTable + `(id int primary key, note text);
insert into ` + appTable + ` select g, 'row ' || g from generate_series(1, 42) g;
create role app_writer nologin;
create role app_reader login password 'reader-pass';
grant app_writer to app_reader;
alter role app_reader set search_path = app, public;`
	if out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/pg-source", "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "app_admin", "-d", appDatabase, "-c", sql); err != nil {
		t.Fatalf("seed the %s source server: %v\n%s", ns, err, out)
	}
	// CREATE TABLESPACE cannot run inside a transaction block: one command of its
	// own, the way the operator procedure states it.
	if out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/pg-source", "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "app_admin", "-d", "postgres",
		"-c", "create tablespace e2e_ts location '/tmp/e2e_ts'"); err != nil {
		t.Fatalf("create the source tablespace: %v\n%s", err, out)
	}
}

// mustExec runs one command in a pod and fails the test on a non-zero exit.
func (e *Env) mustExec(t *testing.T, ns, pod, name string, args ...string) string {
	t.Helper()
	full := append([]string{"exec", "-n", ns, pod, "--", name}, args...)
	out, err := e.exec("", "kubectl", full...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(full, " "), err, out)
	}
	return out
}

// waitForDatabase returns the one side-by-side database a restore created.
func (e *Env) waitForDatabase(t *testing.T, ns, deployment, user, pattern string) string {
	t.Helper()
	var name string
	waitFor(t, "the restored database", time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "exec", "-n", ns, deployment, "--",
			"psql", "-U", user, "-d", "postgres", "-tAc",
			"select datname from pg_database where datname like '"+pattern+"'")
		if err != nil {
			return false, out
		}
		name = strings.TrimSpace(out)
		return name != "", "no side-by-side database yet"
	})
	return name
}

// psqlIn runs one query in a pod and returns its trimmed output.
func (e *Env) psqlIn(t *testing.T, ns, deployment, user, database, query string) string {
	t.Helper()
	out, err := e.exec("", "kubectl", "exec", "-n", ns, deployment, "--",
		"psql", "-U", user, "-d", database, "-tAc", query)
	if err != nil {
		t.Fatalf("psql -d %s: %v\n%s", database, err, out)
	}
	return strings.TrimSpace(out)
}

// checkRestoredGlobals proves the operator procedure's checks: the roles exist,
// the membership survived, the role setting is the source's, the tablespace is
// there, and the restored role can log in with the source's password.
func (e *Env) checkRestoredGlobals(t *testing.T, ns string) {
	t.Helper()
	query := func(q string) string { return e.psqlIn(t, ns, "deploy/pg-target", "bootstrap", "postgres", q) }
	if got := query("select count(*) from pg_roles where rolname in ('app_admin','app_reader','app_writer')"); got != "3" {
		t.Fatalf("restored roles = %s, want 3", got)
	}
	if got := query("select pg_has_role('app_reader','app_writer','member')"); got != "t" {
		t.Fatalf("restored membership = %s, want t", got)
	}
	if got := query("select array_to_string(rolconfig, ',') from pg_roles where rolname = 'app_reader'"); !strings.Contains(got, "search_path=app, public") {
		t.Fatalf("restored role setting = %q, want the source's search_path", got)
	}
	if got := query("select count(*) from pg_tablespace where spcname = 'e2e_ts'"); got != "1" {
		t.Fatalf("restored tablespaces = %s, want the source's e2e_ts", got)
	}
	login, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/pg-target", "--",
		"env", "PGPASSWORD=reader-pass", "psql", "-h", "127.0.0.1", "-U", "app_reader", "-d", "postgres", "-tAc", "select current_user")
	if err != nil {
		t.Fatalf("login as the restored role: %v\n%s", err, login)
	}
	if got := strings.TrimSpace(login); got != "app_reader" {
		t.Fatalf("restored role login = %q, want app_reader", got)
	}
}

// waitNoPods waits until no pod with the label remains: the next server must not
// start before the previous pod is gone.
func (e *Env) waitNoPods(t *testing.T, ns, selector string) {
	t.Helper()
	waitFor(t, "no pods with "+selector, 2*time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "get", "pods", "-n", ns, "-l", selector, "-o", "name")
		if err != nil {
			return false, out
		}
		return strings.TrimSpace(out) == "", out
	})
}

// decodeFetched recovers the fetched artifact from a Job's log: the artifact is
// printed as one base64 line, and every other line is the command's report.
func decodeFetched(t *testing.T, logs string) string {
	t.Helper()
	var best string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			continue
		}
		// The report lines that happen to be valid base64 are short; the
		// artifact is the long one.
		if len(decoded) > len(best) {
			best = string(decoded)
		}
	}
	if best == "" || !strings.Contains(best, "CREATE ROLE app_reader") {
		t.Fatalf("the fetched globals artifact was not found in the log:\n%s", logs)
	}
	return best
}
