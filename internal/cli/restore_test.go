package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// plainBody is the standard run body: one plain artifact through zstd, the real
// local storage and the fault-injectable one.
func plainBody(root string) string {
	return plainSourceYAML("hello") + zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
}

// syntheticTargetYAML is one apply target instance: a synthetic source with a
// directory to write into and the origin it claims.
func syntheticTargetYAML(name, dir, origin string) string {
	return fmt.Sprintf(`  %s:
    type: synthetic
    artifact: {group: t, name: %s, importance: best_effort, sensitivity: plain}
    pipeline: []
    config: {payload: "", dir: %q, origin: %q}
`, name, name, dir, origin)
}

// TestRestoreFetchFallsBackToSecondStorage: the first storage of the walk order
// is down, the copy is on the second, and the bytes at the destination equal the
// source bytes.
func TestRestoreFetchFallsBackToSecondStorage(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	// offsite is first in restore.storage_order and is down now, while it does
	// hold a copy: the walk must move on to exchange.
	w.storage("offsite").Fault = down

	out := filepath.Join(dir, "fetched.dump")
	code, stdout, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "n", "--out", out)
	if code != 0 {
		t.Fatalf("fetch: exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if got := mustRead(t, out); got != "hello" {
		t.Errorf("fetched %q, want the source bytes", got)
	}
	if !strings.Contains(stdout, "from exchange") {
		t.Errorf("report does not name the storage that served:\n%s", stdout)
	}
	if !strings.Contains(stdout, "offsite: the storage survey failed") {
		t.Errorf("report does not name the skipped storage:\n%s", stdout)
	}
	// The plan comes before the transfer: the run, the object and every
	// storage of the walk with its verdict.
	if !strings.Contains(stdout, "plan: run "+testRunID) || !strings.Contains(stdout, "exchange: holds a copy") ||
		!strings.Contains(stdout, "offsite: the storage survey failed") {
		t.Errorf("the plan does not announce what will be fetched:\n%s", stdout)
	}
	if strings.Index(stdout, "plan: run ") > strings.Index(stdout, "✓ fetched ") {
		t.Errorf("the plan is not printed before the fetch:\n%s", stdout)
	}
	assertNoPartial(t, dir)

	// A destination of - streams the artifact on stdout and moves the report to
	// stderr, so a pipeline can consume the bytes untouched.
	code, stdout, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "n", "--out", "-")
	if code != 0 {
		t.Fatalf("fetch to stdout: exit code = %d\nstderr: %s", code, stderr)
	}
	if stdout != "hello" {
		t.Errorf("stdout = %q, want the source bytes alone", stdout)
	}
	if !strings.Contains(stderr, "✓ fetched ") {
		t.Errorf("the report is not on stderr: %s", stderr)
	}
}

// TestRestoreFetchRawKeepsStoredBytes: --raw writes the stored object, the zstd
// frame the run uploaded, without reversing the chain.
func TestRestoreFetchRawKeepsStoredBytes(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}
	out := filepath.Join(dir, "fetched.zst")
	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "n", "--out", out, "--raw")
	if code != 0 {
		t.Fatalf("fetch --raw: exit code = %d\nstderr: %s", code, stderr)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		t.Errorf("--raw did not keep the stored zstd frame: % x", raw)
	}
	if string(raw) == "hello" {
		t.Error("--raw decoded the artifact")
	}
}

// TestRestoreFetchUnknownArtifactName: --artifact takes the artifact's own name,
// and a wrong one is answered with the names the run does hold — never with the
// engine's per-storage reason, which reads as if a copy were missing.
func TestRestoreFetchUnknownArtifactName(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	out := filepath.Join(dir, "fetched.dump")
	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", "g/n", "--out", out)
	if code != exitFailure {
		t.Fatalf("fetch of the group-prefixed name: exit %d, want %d\nstderr: %s", code, exitFailure, stderr)
	}
	if !strings.Contains(stderr, `run `+testRunID+` has no artifact named "g/n"; it holds: n`) {
		t.Errorf("the failure does not name the run's artifacts: %s", stderr)
	}
	if strings.Contains(stderr, "absent from the run") {
		t.Errorf("the failure still reads as a missing copy: %s", stderr)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("a file was published at %s despite the failure", out)
	}
	assertNoPartial(t, dir)

	// A name the run does hold, whose object is gone from every storage, is a
	// different answer: the engine's own reason, never "no artifact named".
	key := "runs/" + testRunID + "/g/n.dat.zst"
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(key))); err != nil {
		t.Fatal(err)
	}
	if err := w.storage("offsite").Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	code, _, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", "n", "--out", out)
	if code != exitFailure {
		t.Fatalf("fetch of a missing object: exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, "is not fetchable") {
		t.Errorf("a missing object does not carry the engine's reason: %s", stderr)
	}
	if strings.Contains(stderr, "has no artifact named") {
		t.Errorf("a missing object is reported as a wrong name: %s", stderr)
	}
}

// TestRestoreFetchNewestOkNamesTheNewestRun: with the newest-ok selector and a
// name no run holds, the engine's own sentence stays and the answer appends what
// the newest run a storage can read does hold, naming that run.
func TestRestoreFetchNewestOkNamesTheNewestRun(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	out := filepath.Join(dir, "fetched.dump")
	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "g/absent", "--out", out)
	if code != exitFailure {
		t.Fatalf("fetch of an absent name with newest-ok: exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, `no run on any storage holds artifact "g/absent" as ok`) {
		t.Errorf("the engine's sentence is gone: %s", stderr)
	}
	if !strings.Contains(stderr, "the newest run with a manifest, "+testRunID+", holds: n") {
		t.Errorf("the answer does not name the newest run and its artifacts: %s", stderr)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("a file was published at %s despite the failure", out)
	}

	// With no manifest readable anywhere the engine's sentence stands alone.
	w2 := newWorld()
	dir2 := t.TempDir()
	w2.storage("exchange").Fault = down
	w2.storage("offsite").Fault = down
	storages := `storages:
  exchange:
    type: faulty
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
` + faultyYAML
	cfg2 := mustWrite(t, dir2, "config.yaml", configYAML(dir2, plainSourceYAML("hello")+zstdYAML+storages+""))
	code, _, stderr = w2.main(context.Background(),
		"restore", "fetch", "--config", cfg2, "--run", newestOK, "--artifact", "g/absent", "--out", filepath.Join(dir2, "fetched.dump"))
	if code != exitFailure {
		t.Fatalf("fetch with every storage down: exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, `no run on any storage holds artifact "g/absent" as ok`) {
		t.Errorf("the engine's sentence is gone: %s", stderr)
	}
	if strings.Contains(stderr, "the newest run with a manifest") {
		t.Errorf("a run was named although no manifest is readable: %s", stderr)
	}
}

// TestRestoreFetchNewestOkHintSurvivesAStorageDown: StoredRuns is all-or-nothing,
// so the hint reads one storage at a time — a storage whose survey fails must not
// swallow what the healthy one answered.
func TestRestoreFetchNewestOkHintSurvivesAStorageDown(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}
	// offsite is first in the walk order and cannot be listed; exchange, the
	// real local storage, answers.
	w.storage("offsite").Fault = down

	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "g/absent",
		"--out", filepath.Join(dir, "fetched.dump"))
	if code != exitFailure {
		t.Fatalf("fetch with one storage down: exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, "the newest run with a manifest, "+testRunID+", holds: n") {
		t.Errorf("the hint did not survive the down storage: %s", stderr)
	}
}

// TestRestoreApplyDoesNotSurveyTwice: the apply surveys the storages inside the
// engine alone — the resolve lists every storage of the walk order (2), and the
// download lists until the first storage serves the object (1 more) — so three
// lists in total. A CLI-side resolve before the call would add two of its own.
func TestRestoreApplyDoesNotSurveyTwice(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Both storages are the fault-injectable fake, so every survey is countable.
	storages := `storages:
  exchange:
    type: faulty
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
` + faultyYAML
	body := plainSourceYAML("hello") + syntheticTargetYAML("target", targetDir, "test:thing") +
		zstdYAML + storages
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	names := []string{"exchange", "offsite"}
	before := 0
	for _, name := range names {
		before += w.storage(name).CallCount(storagetest.OpList, "runs/")
	}
	code, _, stderr := w.main(context.Background(),
		"restore", "apply", "--config", cfg, "--run", testRunID, "--artifact", "n",
		"--target", "target", "--mode", "side-by-side")
	if code != 0 {
		t.Fatalf("apply: exit code = %d\nstderr: %s", code, stderr)
	}
	surveys := 0
	for _, name := range names {
		surveys += w.storage(name).CallCount(storagetest.OpList, "runs/")
	}
	if got := surveys - before; got != 3 {
		t.Errorf("the apply surveyed the storages %d times, want 3 (the engine's resolve and its download; the CLI adds none)", got)
	}
}

// TestRestoreListPrintsTheArtifactNamesItAccepts: the listing prints each name
// in the form --artifact takes, and a name copied from it fetches.
func TestRestoreListPrintsTheArtifactNamesItAccepts(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	code, stdout, stderr := w.main(context.Background(), "restore", "list", "--config", cfg)
	if code != 0 {
		t.Fatalf("restore list: exit %d\nstderr: %s", code, stderr)
	}
	// The name, not the key: the group is not part of it.
	if !strings.Contains(stdout, "  artifacts: n\n") {
		t.Fatalf("the listing does not print the artifact names:\n%s", stdout)
	}
	name, ok := listedArtifactName(stdout)
	if !ok {
		t.Fatalf("the listing prints no artifact names:\n%s", stdout)
	}
	out := filepath.Join(dir, "fetched.dump")
	code, _, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", name, "--out", out)
	if code != 0 {
		t.Fatalf("fetch of the listed name %q: exit %d\nstderr: %s", name, code, stderr)
	}
	if got := mustRead(t, out); got != "hello" {
		t.Errorf("fetched %q, want the source bytes", got)
	}
}

// listedArtifactName returns the first name of the first "  artifacts: …" line —
// the form an operator copies into --artifact.
func listedArtifactName(stdout string) (string, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		rest, ok := strings.CutPrefix(line, "  artifacts: ")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(rest, ", ")
		return strings.TrimSpace(name), true
	}
	return "", false
}

// TestRestoreFetchWritesPrivateFiles: every fetch output is 0600 — the
// .partial it is written to and the published file alike, whatever the
// artifact's label says. The published file inherits the .partial's mode through
// the rename, so this binds the partial's creation mode too: a fetch over a
// world-readable destination and a world-readable stale .partial must leave
// neither mode behind.
func TestRestoreFetchWritesPrivateFiles(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	out := filepath.Join(dir, "fetched.dump")
	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "n", "--out", out)
	if code != 0 {
		t.Fatalf("fetch: exit code = %d\nstderr: %s", code, stderr)
	}
	assertMode(t, out, 0o600)
	assertNoPartial(t, dir)

	if err := os.WriteFile(out, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(out, 0o644); err != nil {
		t.Fatal(err)
	}
	partial := out + ".partial"
	if err := os.WriteFile(partial, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(partial, 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "n", "--out", out)
	if code != 0 {
		t.Fatalf("fetch over an existing file: exit code = %d\nstderr: %s", code, stderr)
	}
	if got := mustRead(t, out); got != "hello" {
		t.Errorf("the refetched file holds %q, want the source bytes", got)
	}
	assertMode(t, out, 0o600)
	assertNoPartial(t, dir)
}

// assertMode pins one file's permission bits.
func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != want {
		t.Errorf("%s has mode %04o, want %04o", path, got, want)
	}
}

// TestRestoreFetchSecretPlaintextRule: a decoded secret artifact may go to a
// file only with --plaintext-to-file; streaming it to stdout is always allowed,
// and --raw (the sealed bytes) is never plaintext.
func TestRestoreFetchSecretPlaintextRule(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	w := newWorld()
	dir := t.TempDir()
	keyPath := mustWrite(t, dir, "age.key", identity.String())
	// Both storages are the fault-injectable fake, so the test can count every
	// Get each of them served.
	body := fmt.Sprintf(`sources:
  secret_thing:
    type: synthetic
    artifact: {group: s, name: vpn, importance: required, sensitivity: secret}
    pipeline: [sealer]
    config: {payload: top-secret, dir: "", origin: "test:secret"}
processors:
  sealer:
    type: age
    config: {recipients: ["%s"], identities: [{file: %s}]}
storages:
  exchange:
    type: faulty
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
%s`, identity.Recipient().String(), keyPath, faultyYAML)
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	key := artifactKey(t, w.storage("exchange"))

	// A refused request downloads nothing: the plan names the artifact as a
	// secret one and the refusal lands before a single Get of the object.
	out := filepath.Join(dir, "plain.txt")
	code, stdout, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "vpn", "--out", out)
	if code != exitFailure {
		t.Fatalf("decoded secret artifact was not refused: exit %d\nstdout: %s", code, stdout)
	}
	if !strings.Contains(stderr, "--plaintext-to-file") {
		t.Errorf("refusal does not name the flag: %s", stderr)
	}
	if !strings.Contains(stdout, "plan: run "+testRunID) || !strings.Contains(stdout, "(secret,") {
		t.Errorf("the plan does not announce the secret artifact before the refusal:\n%s", stdout)
	}
	for _, name := range []string{"exchange", "offsite"} {
		if n := w.storage(name).CallCount(storagetest.OpGet, key); n != 0 {
			t.Errorf("the refused fetch read %s from %s %d times", key, name, n)
		}
	}
	// The refusal is informed, not vacuous: the plan read a manifest to learn
	// the artifact is a secret one.
	if n := w.storage("exchange").CallCount(storagetest.OpGet, "runs/"+testRunID+"/manifest.json"); n == 0 {
		t.Error("the refusal did not read the manifest it decided from")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("a file was published at %s despite the refusal", out)
	}
	assertNoPartial(t, dir)

	code, _, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "vpn", "--out", out, "--plaintext-to-file")
	if code != 0 {
		t.Fatalf("fetch --plaintext-to-file: exit code = %d\nstderr: %s", code, stderr)
	}
	if got := mustRead(t, out); got != "top-secret" {
		t.Errorf("fetched %q, want the decoded plaintext", got)
	}
	reads := w.storage("exchange").CallCount(storagetest.OpGet, key) + w.storage("offsite").CallCount(storagetest.OpGet, key)
	if reads == 0 {
		t.Error("the acknowledged fetch never read the object")
	}

	// The artifact on stdout needs no flag; the plan moves to stderr with it.
	code, stdout, stderr = w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "vpn", "--out", "-")
	if code != 0 || stdout != "top-secret" {
		t.Fatalf("fetch to stdout: exit code = %d stdout = %q\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "plan: run "+testRunID) {
		t.Errorf("the plan is not on stderr next to the artifact:\n%s", stderr)
	}

	// --raw is the sealed object: a file destination is legal without the flag.
	rawOut := filepath.Join(dir, "sealed.age")
	if code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "vpn", "--out", rawOut, "--raw"); code != 0 {
		t.Fatalf("fetch --raw: exit code = %d\nstderr: %s", code, stderr)
	}
	if got := mustRead(t, rawOut); got == "top-secret" {
		t.Error("--raw wrote plaintext")
	}
}

// TestRestoreFetchByKeyWithoutAManifest: the by-key form reaches an object no
// manifest describes.
func TestRestoreFetchByKeyWithoutAManifest(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}
	key := "runs/" + testRunID + "/manifest.json"
	out := filepath.Join(dir, "manifest.json")
	code, stdout, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--storage", "exchange", "--key", key, "--out", out)
	if code != 0 {
		t.Fatalf("fetch by key: exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "verified=false") {
		t.Errorf("a by-key fetch claims verification it cannot have:\n%s", stdout)
	}
	if !strings.Contains(mustRead(t, out), `"schema"`) {
		t.Error("the fetched object is not the manifest")
	}
}

// TestRestoreApplyRefusesAndApplies: nothing destructive is reachable without
// its mode, an in-place apply needs the exact confirmation, and a deliberate
// foreign origin needs its own flag.
func TestRestoreApplyRefusesAndApplies(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	targetDir := filepath.Join(dir, "target")
	foreignDir := filepath.Join(dir, "foreign")
	for _, d := range []string{targetDir, foreignDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	body := plainSourceYAML("hello") +
		syntheticTargetYAML("target", targetDir, "test:thing") +
		syntheticTargetYAML("foreign", foreignDir, "test:other") +
		zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	base := []string{"restore", "apply", "--config", cfg, "--run", newestOK, "--artifact", "n", "--target", "target"}

	code, _, stderr := w.main(context.Background(), base...)
	if code != exitUsage || !strings.Contains(stderr, "--mode") {
		t.Fatalf("apply without --mode: exit %d, stderr %s", code, stderr)
	}
	code, _, stderr = w.main(context.Background(), append(base, "--mode", "in-place")...)
	if code != exitUsage || !strings.Contains(stderr, "--confirm") {
		t.Fatalf("in-place without --confirm: exit %d, stderr %s", code, stderr)
	}
	wrongName := []string{"restore", "apply", "--config", cfg, "--run", testRunID, "--artifact", "g/n", "--target", "target", "--mode", "side-by-side"}
	code, _, stderr = w.main(context.Background(), wrongName...)
	if code != exitFailure || !strings.Contains(stderr, `has no artifact named "g/n"`) {
		t.Fatalf("apply of the group-prefixed name: exit %d, stderr %s", code, stderr)
	}
	code, _, stderr = w.main(context.Background(), append(base, "--mode", "in-place", "--confirm", "wrong")...)
	if code != exitFailure || !strings.Contains(stderr, `target origin "test:thing"`) {
		t.Fatalf("in-place with a wrong confirmation: exit %d, stderr %s", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "in-place")); !os.IsNotExist(err) {
		t.Error("a refused apply wrote something")
	}

	code, stdout, stderr := w.main(context.Background(), append(base, "--mode", "side-by-side")...)
	if code != 0 {
		t.Fatalf("side-by-side apply: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "applied to ") {
		t.Errorf("apply report missing:\n%s", stdout)
	}
	if got := mustRead(t, filepath.Join(targetDir, "side-by-side-"+testRunID)); got != "hello" {
		t.Errorf("side-by-side wrote %q, want the decoded bytes", got)
	}

	code, _, stderr = w.main(context.Background(), append(base, "--mode", "in-place", "--confirm", "test:thing")...)
	if code != 0 {
		t.Fatalf("in-place apply: exit %d\nstderr: %s", code, stderr)
	}
	if got := mustRead(t, filepath.Join(targetDir, "in-place")); got != "hello" {
		t.Errorf("in-place wrote %q, want the decoded bytes", got)
	}

	foreign := []string{"restore", "apply", "--config", cfg, "--run", newestOK, "--artifact", "n", "--target", "foreign", "--mode", "side-by-side"}
	code, _, stderr = w.main(context.Background(), foreign...)
	if code != exitFailure || !strings.Contains(stderr, "AllowForeignOrigin") {
		t.Fatalf("foreign origin without the flag: exit %d, stderr %s", code, stderr)
	}
	code, _, stderr = w.main(context.Background(), append(foreign, "--allow-foreign-origin")...)
	if code != 0 {
		t.Fatalf("foreign origin accepted explicitly: exit %d\nstderr: %s", code, stderr)
	}
	if got := mustRead(t, filepath.Join(foreignDir, "side-by-side-"+testRunID)); got != "hello" {
		t.Errorf("foreign apply wrote %q, want the decoded bytes", got)
	}
}

// TestRestoreVerifyDepths: stored checks the object, decoded reverses the chain,
// probed asks the named source, and any failure is a non-zero exit.
func TestRestoreVerifyDepths(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	targetDir := filepath.Join(dir, "target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := plainSourceYAML("hello") + syntheticTargetYAML("target", targetDir, "test:thing") +
		zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	// One storage of the walk order is down; verification still finds the copy
	// on the other, and every depth reports which run it checked.
	w.storage("offsite").Fault = down
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"stored", []string{"--depth", "stored", "--artifact", "n"}},
		{"decoded", []string{"--depth", "decoded", "--artifact", "n"}},
		{"probed", []string{"--depth", "probed", "--artifact", "n", "--target", "target"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"restore", "verify", "--config", cfg, "--run", newestOK}, tc.args...)
			code, stdout, stderr := w.main(context.Background(), args...)
			if code != 0 || !strings.Contains(stdout, "✓") {
				t.Fatalf("verify %s: exit %d\nstdout: %s\nstderr: %s", tc.name, code, stdout, stderr)
			}
			if !strings.Contains(stdout, "run="+testRunID) {
				t.Fatalf("verify %s does not name the run it checked:\n%s", tc.name, stdout)
			}
		})
	}

	code, _, stderr := w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", testRunID, "--artifact", "g/n", "--depth", "stored")
	if code != exitFailure || !strings.Contains(stderr, `has no artifact named "g/n"`) {
		t.Fatalf("verify of the group-prefixed name: exit %d, stderr %s", code, stderr)
	}

	// A stored check with the newest-ok selector resolves the run itself and
	// reports it truthfully: the artifact is present, so nothing fails.
	code, stdout, stderr := w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", newestOK, "--artifact", "n", "--depth", "stored")
	if code != 0 || !strings.Contains(stdout, "run="+testRunID) || !strings.Contains(stdout, "ok on exchange") {
		t.Fatalf("stored with newest-ok: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	code, _, stderr = w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", newestOK, "--artifact", "n", "--depth", "probed")
	if code != exitUsage || !strings.Contains(stderr, "--target") {
		t.Fatalf("probed without --target: exit %d, stderr %s", code, stderr)
	}
	code, _, stderr = w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", newestOK, "--artifact", "n", "--depth", "deep")
	if code != exitUsage || !strings.Contains(stderr, "--depth") {
		t.Fatalf("unknown depth: exit %d, stderr %s", code, stderr)
	}
	// A name the run does not hold is answered before any row exists.
	code, _, stderr = w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", testRunID, "--artifact", "absent", "--depth", "stored")
	if code != exitFailure || !strings.Contains(stderr, `has no artifact named "absent"`) {
		t.Fatalf("verify of a name the run does not hold: exit %d, stderr %s", code, stderr)
	}

	// A verification that ran and failed keeps its own row: the object is
	// gone from the disk while the manifest still records it.
	if err := os.Remove(filepath.Join(root, "runs", testRunID, "g", "n.dat.zst")); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ = w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", testRunID, "--artifact", "n", "--depth", "stored")
	if code != exitFailure || !strings.Contains(stdout, "✗") {
		t.Fatalf("a failed verification: exit %d, stdout %s", code, stdout)
	}
}

// TestRestoreListReportsEveryStorage: the listing names both storages with and
// without a manifest, filters by run and storage, and renders JSON on request.
func TestRestoreListReportsEveryStorage(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}

	code, stdout, stderr := w.main(context.Background(), "restore", "list", "--config", cfg)
	if code != 0 {
		t.Fatalf("list: exit code = %d\nstderr: %s", code, stderr)
	}
	if got := strings.Count(stdout, "manifest=yes"); got != 2 {
		t.Fatalf("list reports %d manifests, want 2:\n%s", got, stdout)
	}

	code, stdout, stderr = w.main(context.Background(), "restore", "list", "--config", cfg, "--output", "json")
	if code != 0 {
		t.Fatalf("list --output json: exit code = %d\nstderr: %s", code, stderr)
	}
	var rows []storedRunJSON
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatalf("json output does not decode: %v\n%s", err, stdout)
	}
	if len(rows) != 2 || rows[0].RunID != testRunID || rows[0].Report == nil || !rows[0].HasManifest {
		t.Fatalf("json output = %+v", rows)
	}

	code, stdout, _ = w.main(context.Background(), "restore", "list", "--config", cfg, "--storage", "exchange")
	if code != 0 || strings.Count(stdout, "manifest=") != 1 || !strings.Contains(stdout, "storage=exchange") {
		t.Fatalf("list --storage: exit %d\n%s", code, stdout)
	}
	code, stdout, _ = w.main(context.Background(), "restore", "list", "--config", cfg, "--run", "other")
	if code != 0 || strings.TrimSpace(stdout) != "" {
		t.Fatalf("list --run other: exit %d, output %q", code, stdout)
	}
}

func assertNoPartial(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".partial") {
			t.Errorf("%s was left behind", e.Name())
		}
	}
}

// TestRestoreFetchNewestOkHintWalksOnlyTheOrder: the hint names a run only from
// the storages the default fetch walk would read. A storage declared but left
// out of restore.storage_order holds a newer manifest, and the hint must stay
// with the run the walk can actually fetch.
func TestRestoreFetchNewestOkHintWalksOnlyTheOrder(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml",
		strings.Replace(configYAML(dir, plainBody(root)), "storage_order: [offsite, exchange]", "storage_order: [exchange]", 1))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit code = %d\nstderr: %s", code, stderr)
	}
	// A newer run's manifest on the excluded storage: were the hint reading it,
	// it would name hiddenrun.
	hidden := model.RunReport{
		Schema:    model.ManifestSchema,
		RunID:     "hiddenrun",
		StartedAt: time.Now().Add(time.Hour),
		Outcome:   model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Group: "g", Name: "h", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Outcome:  model.ArtifactOK,
		}},
	}
	raw, err := json.Marshal(hidden)
	if err != nil {
		t.Fatal(err)
	}
	w.storage("offsite").PutObject("runs/hiddenrun/manifest.json", raw, hidden.StartedAt)

	code, _, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", newestOK, "--artifact", "g/absent",
		"--out", filepath.Join(dir, "fetched.dump"))
	if code != exitFailure {
		t.Fatalf("fetch of an absent name: exit %d, want %d\nstderr: %s", code, exitFailure, stderr)
	}
	if !strings.Contains(stderr, "the newest run with a manifest, "+testRunID+", holds: n") {
		t.Errorf("the hint does not stay with the run the walk can fetch: %s", stderr)
	}
	if strings.Contains(stderr, "hiddenrun") {
		t.Errorf("the hint read a storage outside restore.storage_order: %s", stderr)
	}
}

// TestRestoreFetchChecksTheDecodeSideFirst: a fetch decodes with the processors
// of the manifest's chain, and each of them is asked whether it can decode
// before the object is downloaded — an identity file that is not mounted fails
// with the processor's name, not halfway through a stream.
func TestRestoreFetchChecksTheDecodeSideFirst(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`sources:
  secret_thing:
    type: synthetic
    artifact: {group: s, name: vpn, importance: required, sensitivity: secret}
    pipeline: [sealer]
    config: {payload: top-secret, dir: "", origin: "test:secret"}
processors:
  sealer:
    type: age
    config: {recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], identities: [{file: /nonexistent/identity.txt}]}
storages:
  exchange:
    type: local
    retention: %s
    max_total_bytes: 1048576
    time_budget: 10s
    config: {root: %q, page_size: 100}
`+faultyYAML, keepRetention, root)
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("the run must seal without the identity file: exit %d\n%s", code, stderr)
	}

	// Fetching the same artifact from another config: the restore side's own
	// identity file is the one that is missing.
	restoreCfg := mustWrite(t, dir, "restore-config.yaml", `version: 1
layout: {run_prefix: "runs/{{ .Run.ID }}", artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}", manifest_key: manifest.json}
restore:
  spool_dir: `+filepath.Join(dir, "restore")+`
  spool_reserve_bytes: 0
  storage_order: [exchange]
  unclassified_error: transient
  retry: {attempts: 1, initial_delay: 1ms, max_delay: 1ms, jitter: 0}
processors:
  sealer:
    type: age
    config: {recipients: [], identities: [{file: /nonexistent/identity.txt}]}
storages:
  exchange:
    type: local
    time_budget: 10s
    config: {root: `+strconv.Quote(root)+`, page_size: 100}
`)
	out := filepath.Join(dir, "artifact.bin")
	code, stdout, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", restoreCfg, "--run", newestOK, "--artifact", "vpn",
		"--plaintext-to-file", "--out", out)
	if code == exitOK {
		t.Fatalf("a fetch whose processor cannot decode succeeded:\n%s", stdout)
	}
	if !strings.Contains(stderr, `processor "sealer" cannot decode`) {
		t.Errorf("the failure does not name the processor:\n%s", stderr)
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused fetch wrote %s", out)
	}
}

// cliEngine builds the engine the restore helpers are exercised against, from
// the harness's own registry and defaults.
func cliEngine(t *testing.T, w *world, cfg string) *engine.Engine {
	t.Helper()
	reg := plugin.NewRegistry()
	if err := w.register(reg); err != nil {
		t.Fatal(err)
	}
	docs, err := defaultsDocuments([][]byte{defaults.Defaults, []byte(defaultsYAML)})
	if err != nil {
		t.Fatal(err)
	}
	defs, err := engine.LoadDefaults(reg, docs)
	if err != nil {
		t.Fatal(err)
	}
	log, err := loggerFor(io.Discard, "info")
	if err != nil {
		t.Fatal(err)
	}
	e, err := openEngine(context.Background(), reg, defs, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// TestRestoreListNamesAManifestlessRun: a run a storage holds without a manifest
// is the interesting one on this listing, and the run filter keeps it.
func TestRestoreListNamesAManifestlessRun(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	// An object under a run prefix with no manifest beside it: an attempt that
	// started and never reported.
	unfinished := filepath.Join(root, "runs", "unfinished")
	if err := os.MkdirAll(unfinished, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unfinished, "data"), []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := w.main(context.Background(), "restore", "list", "--config", cfg, "--run", "unfinished")
	if code != 0 {
		t.Fatalf("list --run unfinished: exit %d\nstderr: %s", code, stderr)
	}
	if !strings.Contains(stdout, "run unfinished storage=exchange") || !strings.Contains(stdout, "manifest=no") {
		t.Errorf("the listing does not name the manifestless run:\n%s", stdout)
	}
	if strings.Contains(stdout, "manifest=yes") {
		t.Errorf("the run filter kept another run:\n%s", stdout)
	}
}

// TestRestoreListRefusesWhatItCannotRead: a storage the engine cannot order and
// an output stream that cannot carry the listing both end the command.
func TestRestoreListRefusesWhatItCannotRead(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))

	code, _, stderr := w.main(context.Background(), "restore", "list", "--config", cfg, "--storage", "absent")
	if code != exitFailure || !strings.Contains(stderr, "absent") {
		t.Fatalf("list --storage absent: exit %d, stderr %s", code, stderr)
	}

	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	for _, output := range []string{"text", "json"} {
		t.Run(output, func(t *testing.T) {
			var stderr strings.Builder
			code := Main(context.Background(),
				Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
				[]string{"restore", "list", "--config", cfg, "--output", output}, failWriter{}, &stderr)
			if code != exitFailure || !strings.Contains(stderr.String(), "stream is broken") {
				t.Fatalf("exit %d, stderr %s", code, stderr.String())
			}
		})
	}
}

// TestRestoreFetchRefusesADestinationItCannotWrite: a destination that cannot be
// opened, and one a finished transfer cannot be renamed onto, both fail without
// leaving a half-written file behind.
func TestRestoreFetchRefusesADestinationItCannotWrite(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	fetch := func(out string) (int, string) {
		var stderr strings.Builder
		code := Main(context.Background(),
			Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
			[]string{"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", "n", "--out", out},
			&strings.Builder{}, &stderr)
		return code, stderr.String()
	}
	code, stderr := fetch(filepath.Join(dir, "no-such-dir", "out"))
	if code != exitFailure {
		t.Fatalf("fetch into a missing directory: exit %d\nstderr: %s", code, stderr)
	}
	// A destination that is an existing directory: the finished transfer cannot
	// be renamed onto it.
	asDir := filepath.Join(dir, "as-dir")
	if err := os.MkdirAll(asDir, 0o755); err != nil {
		t.Fatal(err)
	}
	code, stderr = fetch(asDir)
	if code != exitFailure {
		t.Fatalf("fetch onto a directory: exit %d\nstderr: %s", code, stderr)
	}
	assertNoPartial(t, dir)
}

// TestRestoreFetchToStdoutFailsWhenTheObjectCannotBeRead: the storage answers
// the survey the plan is built from and then refuses the transfer, and the bytes
// were promised on stdout.
func TestRestoreFetchToStdoutFailsWhenTheObjectCannotBeRead(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := mustWrite(t, dir, "config.yaml",
		strings.Replace(configYAML(dir, plainSourceYAML("hello")+zstdYAML+"storages:\n"+faultyYAML),
			"storage_order: [offsite, exchange]", "storage_order: [offsite]", 1))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	// The manifest reads back, so the plan is built; the artifact itself is what
	// cannot be read.
	w.storage("offsite").Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpGet && !strings.HasSuffix(key, "/manifest.json") {
			return errors.New("the object cannot be read")
		}
		return nil
	}
	code, stdout, stderr := w.main(context.Background(),
		"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", "n", "--out", "-")
	if code != exitFailure {
		t.Fatalf("fetch of an unreadable object: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if stdout != "" {
		t.Errorf("stdout carries %q, want nothing", stdout)
	}
}

// TestRestoreApplyAndVerifyRefuseABrokenOutput: both reports are checked
// outputs, so a stream that cannot carry them ends the command.
func TestRestoreApplyAndVerifyRefuseABrokenOutput(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	targetDir := filepath.Join(dir, "target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := plainSourceYAML("hello") + syntheticTargetYAML("target", targetDir, "test:thing") +
		zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	commands := [][]string{
		{"restore", "apply", "--config", cfg, "--run", testRunID, "--artifact", "n", "--target", "target", "--mode", "side-by-side"},
		{"restore", "verify", "--config", cfg, "--run", testRunID, "--depth", "stored"},
	}
	for _, command := range commands {
		t.Run(strings.Join(command[:2], " "), func(t *testing.T) {
			var stderr strings.Builder
			code := Main(context.Background(),
				Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
				command, failWriter{}, &stderr)
			if code != exitFailure || !strings.Contains(stderr.String(), "stream is broken") {
				t.Fatalf("exit %d, stderr %s", code, stderr.String())
			}
		})
	}
}

// TestRestoreVerifyFailsOnARunItCannotResolve: an unknown run is the command's
// failure, not a row of the report.
func TestRestoreVerifyFailsOnARunItCannotResolve(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	code, _, stderr := w.main(context.Background(), "restore", "verify", "--config", cfg, "--run", "absent", "--depth", "stored")
	if code != exitFailure {
		t.Fatalf("verify of an unknown run: exit %d\nstderr: %s", code, stderr)
	}
}

// TestRestoreHelpersStayHonest: the hints name only what a storage actually
// holds, and a storage that cannot be read withholds nothing.
func TestRestoreHelpersStayHonest(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
	}
	// An attempt that started and never reported: a run a storage holds without
	// a manifest, which no hint may name.
	unfinished := filepath.Join(root, "runs", "unfinished")
	if err := os.MkdirAll(unfinished, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unfinished, "data"), []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := cliEngine(t, w, cfg)
	ctx := context.Background()

	// A run no storage holds: the hint has nothing to name, so the engine's own
	// sentence stands.
	names, ok := runManifestNames(ctx, e, []engine.StorageHold{{Storage: "exchange"}}, "absent")
	if ok || names != nil {
		t.Errorf("runManifestNames(absent) = %v, %t, want nothing", names, ok)
	}
	// A storage that cannot be read is skipped, not fatal: the other one answers.
	w.storage("offsite").Fault = down
	if id, got, ok := newestManifestNames(ctx, e, []string{"offsite", "exchange"}); !ok || id != testRunID || len(got) != 1 {
		t.Errorf("newestManifestNames with a storage down = %q, %v, %t, want the readable run", id, got, ok)
	}
	if id, got, ok := newestManifestNames(ctx, e, []string{"offsite"}); ok || id != "" || got != nil {
		t.Errorf("newestManifestNames(all down) = %q, %v, %t, want nothing", id, got, ok)
	}
	w.storage("offsite").Fault = nil
	if got := storageNames(e, []string{"absent"}); got != nil {
		t.Errorf("storageNames(absent) = %v, want nothing", got)
	}
	// A by-key fetch has no manifest to explain a miss with.
	if err := unknownArtifactError(ctx, e, engine.FetchRequest{RawStorage: "exchange", RawKey: "k"}, engine.FetchPlan{}, errors.New("absent")); err != nil {
		t.Errorf("unknownArtifactError(by key) = %v, want nil", err)
	}
	// The newest run a storage can read is named with what it holds.
	id, held, ok := newestManifestNames(ctx, e, []string{"exchange"})
	if !ok || id != testRunID || len(held) != 1 || held[0] != "n" {
		t.Errorf("newestManifestNames = %q, %v, %t, want the run and its artifact", id, held, ok)
	}
}

// TestRestoreFetchCleansUpWhenTheFileCannotBePrepared: the two file operations
// whose failure belongs to the machine — setting the mode on the partial file,
// closing it after the transfer — both end the command with the partial removed
// and nothing published at the destination.
func TestRestoreFetchCleansUpWhenTheFileCannotBePrepared(t *testing.T) {
	cases := []struct {
		name string
		// inject replaces one of the two seams for the duration of one fetch.
		inject func(t *testing.T)
	}{
		{"the mode cannot be set", func(t *testing.T) {
			t.Helper()
			real := chmod
			chmod = func(*os.File, os.FileMode) error { return errors.New("the mode cannot be set") }
			t.Cleanup(func() { chmod = real })
		}},
		{"the file cannot be closed", func(t *testing.T) {
			t.Helper()
			real := closeFile
			closeFile = func(f *os.File) error {
				_ = f.Close()
				return errors.New("the file cannot be closed")
			}
			t.Cleanup(func() { closeFile = real })
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld()
			dir := t.TempDir()
			root := filepath.Join(dir, "exchange")
			cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
			if code, _, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
				t.Fatalf("run: exit %d\nstderr: %s", code, stderr)
			}
			c.inject(t)
			out := filepath.Join(dir, "fetched.dump")
			code, _, stderr := w.main(context.Background(),
				"restore", "fetch", "--config", cfg, "--run", testRunID, "--artifact", "n", "--out", out)
			if code != exitFailure {
				t.Fatalf("fetch: exit %d\nstderr: %s", code, stderr)
			}
			if !strings.Contains(stderr, c.name) {
				t.Errorf("the failure does not name what went wrong:\n%s", stderr)
			}
			if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the failed fetch published %s", out)
			}
			assertNoPartial(t, dir)
		})
	}
}

// TestArtifactNameListIsBounded: an error naming what a run holds must not bury
// the answer, and a run holding nothing says so.
func TestArtifactNameListIsBounded(t *testing.T) {
	if got := artifactNameList(nil); got != "none" {
		t.Errorf("artifactNameList(nil) = %q, want %q", got, "none")
	}
	if got := artifactNameList([]string{"a", "b"}); got != "a, b" {
		t.Errorf("artifactNameList = %q, want %q", got, "a, b")
	}
	many := make([]string, 25)
	for i := range many {
		many[i] = "a"
	}
	got := artifactNameList(many)
	if !strings.Contains(got, "… (5 more)") {
		t.Errorf("a long list is not bounded: %q", got)
	}
}
