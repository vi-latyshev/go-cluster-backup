package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// liveBuffer is a bytes.Buffer a test can read while the run writes it: the
// container log is exactly this, a stream read while the process is alive.
type liveBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *liveBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *liveBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitFor blocks until the stream holds want, or the deadline passes. It is how
// a test proves a line arrived before the run ended: the run itself waits here,
// so a log buffered until exit never lets it through.
func (b *liveBuffer) waitFor(want string, deadline time.Duration) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if strings.Contains(b.String(), want) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// TestRunLogStreamsLineByLine is the streaming contract: the log lines are
// written as the events happen, not collected and flushed at exit. The run is
// held inside the first storage upload until the line the source already wrote
// is visible on the stream; a buffered log would never show it and the test
// would time out.
func TestRunLogStreamsLineByLine(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

	stream := &liveBuffer{}
	var seen bool
	w.storage("offsite").Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut {
			seen = stream.waitFor("dumping test:thing", 5*time.Second)
		}
		return nil
	}

	var stdout bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"run", "--kind", "scheduled", "--config", cfg}, &stdout, stream)
	if code != 0 {
		t.Fatalf("exit code = %d\nlog: %s", code, stream.String())
	}
	if !seen {
		t.Fatalf("the upload started before the source's line was on the stream: the log is buffered\n%s", stream.String())
	}
}

// TestRunLogTellsTheWholeStory pins the shape of the log: the opening line, the
// discovery, the artifact's source/processor/spool/storage steps in order, the
// retention and catalog lines, the notification and the closing summary. It
// also pins what the default output must NOT carry — a sha256 or a full object
// key — which stay in the manifest and the catalog.
func TestRunLogTellsTheWholeStory(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 0 {
		t.Fatalf("exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	log := stderr

	want := []string{
		"run " + testRunID + " starting — config " + cfg + ", storages: exchange, offsite",
		"discovery: 1 artifacts (0 covered or ignored)",
		"── g/n (required)",
		"source synthetic/thing: dumping test:thing",
		"processor zstd/squeeze: 5 B → ",
		"spool ",
		"storage local/exchange: ✓ g/n",
		"✓ g/n  ",
		"retention: kept 1, deleted 0, freed 0 B",
		"catalog memory/catalog: saved run " + testRunID,
		"notifier memory/sink: ✓ sent run_started",
		"notifier memory/sink: ✓ sent run_finished",
	}
	for _, line := range want {
		if !strings.Contains(log, line) {
			t.Errorf("the log does not carry %q:\n%s", line, log)
		}
	}
	// Order: the source line comes before the processor line, which comes before
	// the storage line, which comes before the artifact's own done line.
	order := []string{"dumping test:thing", "processor zstd/squeeze", "spool ", "✓ g/n  "}
	at := -1
	for _, marker := range order {
		i := strings.Index(log, marker)
		if i < 0 {
			t.Fatalf("the log does not carry %q:\n%s", marker, log)
		}
		if i < at {
			t.Errorf("%q appears out of order:\n%s", marker, log)
		}
		at = i
	}
	// The default output carries no machine detail: a sha256 and a full object
	// key belong to the manifest and the catalog, never to the screen.
	if strings.Contains(log, "sha256") || strings.Contains(log, "runs/") {
		t.Errorf("the default log carries machine detail:\n%s", log)
	}
	if strings.Contains(stdout, "sha256") || strings.Contains(stdout, "runs/") {
		t.Errorf("the default summary carries machine detail:\n%s", stdout)
	}
	// The summary is a summary: it names the outcome and every storage.
	for _, line := range []string{"run " + testRunID + " delivered in ", "artifacts: 1 ok, 0 failed, 0 excluded", "exchange: verified"} {
		if !strings.Contains(stdout, line) {
			t.Errorf("the summary does not carry %q:\n%s", line, stdout)
		}
	}
}

// TestRunLogDebugCarriesTheMachineDetail: --log-level debug is where the sha256,
// the key and the byte-exact counts live, so the default output stays readable
// without losing them.
func TestRunLogDebugCarriesTheMachineDetail(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg, "--log-level", "debug")
	if code != 0 {
		t.Fatalf("exit code = %d\nstderr: %s", code, stderr)
	}
	if !strings.Contains(stdout, "machine g/n key=runs/"+testRunID+"/g/n.dat.zst sha256=") {
		t.Errorf("the debug summary does not carry the key and the sha256:\n%s", stdout)
	}
}

// TestArtifactDurationIsRecorded is the regression for the deferred duration
// that landed on a copy: every artifact's duration was 0s because the closure
// set it on a local the caller never received.
func TestArtifactDurationIsRecorded(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("exit code = %d\nstderr: %s", code, stderr)
	}
	manifestPath := filepath.Join(root, "runs", testRunID, "manifest.json")
	var manifest model.RunReport
	if err := json.Unmarshal([]byte(mustRead(t, manifestPath)), &manifest); err != nil {
		t.Fatalf("manifest does not decode: %v", err)
	}
	if len(manifest.Artifacts) != 1 {
		t.Fatalf("the manifest holds %d artifacts, want 1", len(manifest.Artifacts))
	}
	// The dump of one tiny payload can be fast, but it is never zero: a duration
	// that reads 0s is the copy bug, not a fast artifact.
	if d := manifest.Artifacts[0].Duration.Std(); d <= 0 {
		t.Errorf("artifact duration = %s, want a measured duration", d)
	}
	if d := manifest.Duration.Std(); d <= 0 {
		t.Errorf("run duration = %s, want a measured duration", d)
	}
}

// TestStageLogStreamsTheArtifact: stage tells the same story as a run.
func TestStageLogStreamsTheArtifact(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	staged := filepath.Join(dir, "staged")
	cfg := mustWrite(t, dir, "config.yaml", `version: 1
stage:
  dir: `+staged+`
  degraded_exit_code: 3
  failed_exit_code: 7
  sources:
    control:
      type: synthetic
      artifact: {group: control-plane, name: state, importance: required, sensitivity: plain}
      pipeline: []
      config: {payload: staged-bytes, dir: "", origin: "test:stage"}
`)
	code, stdout, stderr := w.main(context.Background(), "stage", "--config", cfg)
	if code != 0 {
		t.Fatalf("exit code = %d\nstderr: %s", code, stderr)
	}
	for _, line := range []string{
		"stage starting — config " + cfg,
		"── control-plane/state (required)",
		"source synthetic/control: dumping test:stage",
		"✓ control-plane/state  ",
	} {
		if !strings.Contains(stderr, line) {
			t.Errorf("the stage log does not carry %q:\n%s", line, stderr)
		}
	}
	if !strings.Contains(stdout, "stage delivered in ") {
		t.Errorf("the stage summary does not name the outcome:\n%s", stdout)
	}
}

// TestInfoLogCarriesNoKeyOrDigest pins the one thing the renderer decides and an
// emitter must never take back: an object key and a sha256 are machine detail,
// shown only at debug. It drives the paths that used to leak them — a retried
// part, a retention deletion, a failed upload and the summary that reports the
// failure — and then proves the check is falsifiable by finding both at debug.
func TestInfoLogCarriesNoKeyOrDigest(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	// Six older runs, so this run's retention drops three of them: the delete
	// path is one that used to print the object's key.
	for i := range 6 {
		seedStoredRun(t, root, fmt.Sprintf("2026010%dT000000Z", i+1))
	}
	parts := newMultipartStorage(4)

	// Two storages reach the two paths that used to print a key: `exchange` is
	// multipart, so its first part fails once and the retry line runs;
	// `archive` is a local storage holding six older runs, so this run's
	// retention deletes three of them. attempts 3 is what there is to retry.
	archive := filepath.Join(dir, "archive")
	if err := os.MkdirAll(archive, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		seedStoredRun(t, archive, fmt.Sprintf("2026010%dT000000Z", i+1))
	}
	// One artifact fails, so the summary's failure path — the other place a key
	// or a digest could reach the default output — is exercised too.
	w.checks["broken"] = errors.New("the datastore refused the connection")
	body := plainSourceYAML("hello") + `  broken:
    type: synthetic
    artifact: {group: control-plane, name: broken, importance: required, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: "x", dir: "", origin: "test:broken"}
` + zstdYAML + `storages:
  exchange:
    type: multipart
    retention: ` + keepRetention + `
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
  archive:
    type: local
    retention: ` + keepRetention + `
    max_total_bytes: 1048576
    time_budget: 10s
    config: {root: "` + archive + `", page_size: 100}
`
	raw := strings.Replace(configYAML(dir, body), "attempts: 1, initial_delay: 1ms", "attempts: 3, initial_delay: 1ms", 1)
	raw = strings.Replace(raw, "storage_order: [offsite, exchange]", "storage_order: [exchange, archive]", 1)
	cfg := mustWrite(t, dir, "config.yaml", raw)
	uniqueRunIDs(t, cfg) // this test runs the engine twice
	run := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := Main(context.Background(),
			Options{Program: "backup", Register: multipartRegister(w, parts), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
			args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}
	code, stdout, stderr := run("run", "--kind", "scheduled", "--config", cfg)
	if code == 0 {
		t.Fatalf("a required artifact failed, so the run must fail; exit %d", code)
	}
	if !strings.Contains(stdout, "✗ control-plane/broken: ") {
		t.Fatalf("the summary does not carry the failure this check is about:\n%s", stdout)
	}
	// The scenario must really reach each path, or a clean log proves nothing.
	for _, want := range []string{
		"retry part 1 of ",
		"storage multipart/exchange: multipart ",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("the run did not exercise %q:\n%s", want, stderr)
		}
	}
	// The delete path really ran — at least one object was freed — whatever the
	// count, which depends on what the failed artifact did to the overlay.
	if !regexp.MustCompile(`retention: kept \d+, deleted [1-9]\d*, freed `).MatchString(stderr) {
		t.Fatalf("the run did not delete anything, so the delete path proves nothing:\n%s", stderr)
	}
	// Both streams are the default output: the log and the summary a command
	// writes as its result.
	digest := regexp.MustCompile(`[0-9a-f]{64}`)
	for name, out := range map[string]string{"log": stderr, "summary": stdout} {
		if strings.Contains(out, "runs/") {
			t.Errorf("an object key reached the info %s:\n%s", name, out)
		}
		if m := digest.FindString(out); m != "" {
			t.Errorf("a sha256 reached the info %s: %s", name, m)
		}
	}

	// Falsifiable: at debug both ARE there, so a silent regression in the
	// renderer cannot pass by emitting nothing.
	code, debugOut, debugLog := run("run", "--kind", "scheduled", "--config", cfg, "--log-level", "debug")
	if code == 0 {
		t.Fatalf("the debug run should behave the same; exit %d", code)
	}
	if !strings.Contains(debugLog, "runs/") || digest.FindString(debugLog) == "" {
		t.Errorf("the debug log does not carry the key and the digest, so the info check proves nothing:\n%s", debugLog)
	}
	if !strings.Contains(debugOut, "runs/") || digest.FindString(debugOut) == "" {
		t.Errorf("the debug summary does not carry the key and the digest, so the info check proves nothing:\n%s", debugOut)
	}
}

// TestStorageResultStreamsBeforeASlowNeighbour is the M3 contract, checked on
// the RAW stream: a storage's own result is written inside its own goroutine,
// so a storage that finished must not wait for one that is still inside Put.
// The check cannot be recovered by sorting — the fast storage's line has to be
// visible while the slow one is provably still blocked.
func TestStorageResultStreamsBeforeASlowNeighbour(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	w.storage("offsite").Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut {
			<-release
		}
		return nil
	}
	// Two uploads at once: the fast one (exchange) finishes while the slow one
	// (offsite) is still inside its Put.
	raw := strings.Replace(configYAML(dir, plainSourceYAML("hello")+zstdYAML+
		"storages:\n"+exchangeYAML(root)+faultyYAML), "storage_parallelism: 1", "storage_parallelism: 2", 1)
	cfg := mustWrite(t, dir, "config.yaml", raw)

	stream := &liveBuffer{}
	done := make(chan int, 1)
	go func() {
		var stdout bytes.Buffer
		done <- Main(context.Background(),
			Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
			[]string{"run", "--kind", "scheduled", "--config", cfg}, &stdout, stream)
	}()

	if !stream.waitFor("storage faulty/offsite: uploading", 5*time.Second) {
		close(release)
		t.Fatalf("the slow storage never started:\n%s", stream.String())
	}
	if !stream.waitFor("storage local/exchange: ✓ ", 5*time.Second) {
		close(release)
		t.Fatalf("the fast storage's result was held back by a slow neighbour still inside Put:\n%s", stream.String())
	}
	close(release)
	if code := <-done; code != 0 {
		t.Fatalf("exit %d\n%s", code, stream.String())
	}
}

// TestInfoLogCarriesNoKeyOrDigestInStorageFailures is the other half of the
// pin: a storage failure's own text can embed an object key. Both shapes are
// driven here — a terminal Put the endpoint rejects, and an object that never
// becomes visible after a successful Put — and both must name the ARTIFACT, on
// both streams, with the key only at debug.
func TestInfoLogCarriesNoKeyOrDigestInStorageFailures(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	// The first storage accepts the object but never shows it; the second
	// rejects the upload outright.
	w.storage("vanishing").Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpStat {
			return plugin.NotFound(errors.New("gone"))
		}
		return nil
	}
	w.storage("refusing").Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut {
			return plugin.Permanent(errors.New("the endpoint rejected the upload"))
		}
		return nil
	}
	storages := `storages:
  exchange:
    type: faulty
    retention: ` + keepRetention + `
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
  offsite:
    type: faulty
    retention: ` + keepRetention + `
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
`
	w.storages["exchange"] = w.storage("vanishing")
	w.storages["offsite"] = w.storage("refusing")
	cfg := mustWrite(t, dir, "config.yaml",
		strings.Replace(configYAML(dir, plainSourceYAML("hello")+zstdYAML+storages), "storage_order: [offsite, exchange]", "storage_order: [exchange, offsite]", 1))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code == 0 {
		t.Fatalf("no storage verified the artifact, so the run must fail; exit %d\n%s", code, stderr)
	}
	// The failures must name the artifact — that is what makes the message
	// useful without the key.
	for stream, out := range map[string]string{"log": stderr, "summary": stdout} {
		for _, want := range []string{"not visible after the upload", "the endpoint rejected the upload"} {
			if !strings.Contains(out, want) {
				t.Errorf("the %s does not carry %q:\n%s", stream, want, out)
			}
		}
		if !strings.Contains(out, "g/n") {
			t.Errorf("the %s does not name the artifact:\n%s", stream, out)
		}
		if strings.Contains(out, "runs/") {
			t.Errorf("an object key reached the info %s:\n%s", stream, out)
		}
		if m := regexp.MustCompile(`[0-9a-f]{64}`).FindString(out); m != "" {
			t.Errorf("a sha256 reached the info %s: %s", stream, m)
		}
	}
}

// TestFailuresStayVisibleAtWarnAndError is the severity contract: an event
// carries its own level, so a failed catalog save and a failed notification are
// still on screen when the operator turned the log down to warnings or errors
// only. Routing every event at info hid exactly the lines a bad night is read
// for.
func TestFailuresStayVisibleAtWarnAndError(t *testing.T) {
	cases := []struct {
		level  string
		expect string
	}{
		{"warn", "catalog memory/catalog: ✗ saving run "},
		{"error", "catalog memory/catalog: ✗ saving run "},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			w := newWorld()
			dir := t.TempDir()
			root := filepath.Join(dir, "exchange")
			w.catalogErr = errors.New("the API said 409")
			w.notifierErr = errors.New("the receiver answered 500")
			cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

			code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg, "--log-level", tc.level)
			if code != 0 {
				t.Fatalf("exit %d\n%s", code, stderr)
			}
			if !strings.Contains(stderr, tc.expect) {
				t.Errorf("a failed catalog save is invisible at --log-level %s:\n%s", tc.level, stderr)
			}
			if !strings.Contains(stderr, "notifier memory/sink: ✗ run_started: ") &&
				!strings.Contains(stderr, "notifier memory/sink: ✗ ") {
				t.Errorf("a notifier failure is invisible at --log-level %s:\n%s", tc.level, stderr)
			}
			// And the routine lines are gone: the level still means something.
			if strings.Contains(stderr, "run ") && strings.Contains(stderr, " starting — ") {
				t.Errorf("an info line survived --log-level %s:\n%s", tc.level, stderr)
			}
			_ = stdout
		})
	}
}

// seedStoredRun plants one finished run in a local storage root: an object and
// a manifest, which is what the retention survey reads.
func seedStoredRun(t *testing.T, root, id string) {
	t.Helper()
	manifest, err := json.Marshal(model.RunReport{
		Schema: model.ManifestSchema, RunID: id,
		StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Outcome: model.OutcomeDelivered,
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string][]byte{
		filepath.Join(root, "runs", id, "manifest.json"):                 manifest,
		filepath.Join(root, "runs", id, "databases", "example.dump.zst"): []byte("bytes"),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSummaryNamesExclusionsOnlyAtDebug: the closing summary counts covered and
// ignored objects in its totals line, and names each one only with machine true
// (--log-level debug). At info a clean night must not read as a problem.
func TestSummaryNamesExclusionsOnlyAtDebug(t *testing.T) {
	rep := model.RunReport{
		RunID: "r", Outcome: model.OutcomeDelivered,
		Excluded: []model.Exclusion{{Name: "ns/media", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "carries the marker"}},
	}
	var info, debug bytes.Buffer
	printRunReport(&info, rep, false)
	printRunReport(&debug, rep, true)
	if strings.Contains(info.String(), "excluded ns/media") {
		t.Fatalf("the info summary names an excluded object:\n%s", info.String())
	}
	if !strings.Contains(debug.String(), "excluded ns/media") {
		t.Fatalf("the debug summary does not name the excluded object, so the info check proves nothing:\n%s", debug.String())
	}
}

// TestErrorLinesAreProminent: a failing artifact is one ✗ line naming it and the
// reason, in the default output.
func TestErrorLinesAreProminent(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	w.storage("offsite").Fault = down
	w.storage("exchange").Fault = down
	storages := `storages:
  exchange:
    type: faulty
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
` + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainSourceYAML("hello")+zstdYAML+storages+""))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 7 {
		t.Fatalf("exit code = %d, want 7\nstderr: %s", code, stderr)
	}
	if !strings.Contains(stderr, "✗ g/n: no storage verified the artifact") {
		t.Errorf("the log does not name the failed artifact and its reason:\n%s", stderr)
	}
	if !strings.Contains(stdout, "✗ g/n: no storage verified the artifact") {
		t.Errorf("the summary does not name the failed artifact and its reason:\n%s", stdout)
	}
}

// storyConfig is the run every log test drives: one source, one processor, a
// memory catalog, a memory notifier, and two storages. The exchange root is
// created first, so its retention survey reads an empty store instead of
// failing on a directory that does not exist yet.
func storyConfig(dir, root string) string {
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	return configYAML(dir, plainSourceYAML("hello")+zstdYAML+
		"catalog:\n  type: memory\n  config: {}\nnotifiers:\n  sink:\n    type: memory\n    config: {}\n"+
		"storages:\n"+exchangeYAML(root)+faultyYAML)
}

// clockPrefix strips the [HH:MM:SS] clock from a log so a golden comparison
// does not depend on when the test ran.
var clockPrefix = regexp.MustCompile(`^\[\d\d:\d\d:\d\d\] `)

// TestRunLogGolden pins the whole run log of a fixed scenario, line by line,
// with the clock removed. A change to any line's shape fails here, so the
// story the log tells is a decision, not an accident.
func TestRunLogGolden(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", storyConfig(dir, root))

	code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 0 {
		t.Fatalf("exit code = %d\nstderr: %s", code, stderr)
	}
	got := normalizeLog(stderr, dir, cfg)
	// The per-storage upload lines are interleaved by whichever storage's
	// goroutine gets there first, so normalizeLog sorts them. This golden is a
	// SHAPE check — the order property lives in
	// TestStorageResultStreamsBeforeASlowNeighbour, which reads the raw stream
	// (a sort here would normalize a wrong order into a right one).
	want := `scheduled run fixedrun starting — config CFG, storages: exchange, offsite
discovery: 1 artifacts (0 covered or ignored)
scheduled run holds the writer lease
catalog memory/catalog: saved run fixedrun
notifier memory/sink: ✓ sent run_started
── g/n (required)
source synthetic/thing: dumping test:thing
processor zstd/squeeze: 5 B → 18 B (0.3x) in DUR
spool SPOOL 18 B
storage faulty/offsite: uploading g/n
storage faulty/offsite: ✓ g/n (18 B)
storage local/exchange: uploading g/n
storage local/exchange: ✓ g/n (18 B)
✓ g/n  5 B → 18 B (0.3x) in DUR
storage local/exchange: retention: kept 1, deleted 0, freed 0 B
storage faulty/offsite: retention: kept 1, deleted 0, freed 0 B
notifier memory/sink: ✓ sent run_finished
catalog memory/catalog: saved run fixedrun
released the writer lease
`
	if got != want {
		t.Errorf("the run log changed:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// normalizeLog removes the clock, the spool path, the config path and every
// measured duration, so the golden text holds only the shape — and sorts the
// run of per-storage upload lines, which are interleaved by whichever storage's
// goroutine gets there first. Sorting here is what makes this a shape check and
// not an order check; the order is pinned on the raw stream elsewhere.
func normalizeLog(log, dir, cfg string) string {
	lines := strings.Split(log, "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if l == "" {
			continue
		}
		l = clockPrefix.ReplaceAllString(l, "")
		l = strings.ReplaceAll(l, cfg, "CFG")
		l = regexp.MustCompile(`^spool [^ ]+`).ReplaceAllString(l, "spool SPOOL")
		l = regexp.MustCompile(`in [0-9][0-9m.µ]*s`).ReplaceAllString(l, "in DUR")
		out = append(out, l)
	}
	return strings.Join(sortUploadRuns(out), "\n") + "\n"
}

// sortUploadRuns sorts each contiguous run of per-storage upload lines by the
// storage's own name and then by what the line says, so a run of them reads the
// same whatever order the goroutines finished in.
func sortUploadRuns(lines []string) []string {
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); {
		if !uploadLine(lines[i]) {
			out = append(out, lines[i])
			i++
			continue
		}
		j := i
		for j < len(lines) && uploadLine(lines[j]) {
			j++
		}
		run := append([]string(nil), lines[i:j]...)
		sort.Strings(run)
		out = append(out, run...)
		i = j
	}
	return out
}

func uploadLine(l string) bool {
	return strings.HasPrefix(l, "storage ") && (strings.Contains(l, ": uploading ") || strings.Contains(l, ": ✓ "))
}
