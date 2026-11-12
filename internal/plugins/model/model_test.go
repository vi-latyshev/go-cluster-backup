package model_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestRunReportMarshalsUnderJSONV2 is the regression guard for the manifest: the
// whole report — durations included — must marshal and round-trip through
// encoding/json/v2, which has no default duration representation of its own.
func TestRunReportMarshalsUnderJSONV2(t *testing.T) {
	started := time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC)
	rep := model.RunReport{
		Schema:    model.ManifestSchema,
		RunID:     "20261012T042000Z",
		StartedAt: started,
		Duration:  model.Duration(4*time.Minute + 39*time.Second),
		Outcome:   model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "ns/postgres", Group: "databases", Ext: ".dump.zst", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired},
			Outcome:  model.ArtifactOK,
			Key:      "runs/20261012T042000Z/databases/ns/postgres.dump.zst",
			Chain:    []model.ProcessorStep{{Instance: "zstd3", Type: "zstd", Ext: ".zst"}},
			Source:   model.SourceRef{Instance: "discovered/pg", Type: "postgres", Origin: "postgres://db.prod:5432/x"},
			Pipeline: "compress",
			RawBytes: 900, Bytes: 100, Stats: model.DumpStats{SkippedEntries: 2, PaddedFiles: 1},
			SHA256:   strings.Repeat("a", 64),
			Duration: model.Duration(time.Second),
		}},
		Storages: []model.StorageResult{{Storage: "yandex", Outcome: model.StorageVerified, Bytes: 100, Kept: 9, Deleted: []string{"OLD"}}},
		Excluded: []model.Exclusion{{Name: "volumes/ns/data", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker"}},
		Previous: model.PreviousRun{RunID: "20261011T042000Z", LastGoodAt: started.AddDate(0, 0, -1)},
		ExitCode: 0,
	}
	body, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"schema":1`, `"duration":"4m39s"`, `"chain":[{`, `"origin":"postgres://db.prod:5432/x"`, `"raw_bytes":900`,
		// The keys added for the opt-out coverage: a skipped/padded count per
		// artifact and the record of every deliberately excluded object. They
		// are stored with the manifest, so a rename here breaks restores.
		`"stats":{"skipped_entries":2,"padded_files":1}`,
		`"excluded":[{"name":"volumes/ns/data","group":"volumes","kind":"ignored","reason":"carries the exclusion marker"}]`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("manifest JSON lacks %s:\n%s", want, body)
		}
	}
	// The fields added for writer identity, warnings and the ledger's own cap
	// facts are omitted while unset: a v2.1.0 reader refuses an unknown member,
	// so an emitted zero would make every new run unreadable on a rollback.
	for _, absent := range []string{`"changed_files"`, `"writer_identity"`, `"warnings"`,
		`"retention_error"`, `"cap_overflow_bytes"`, `"cap_evicted"`, `"occupancy_unknown"`} {
		if strings.Contains(string(body), absent) {
			t.Errorf("manifest JSON carries %s while unset:\n%s", absent, body)
		}
	}
	var back model.RunReport
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Schema != model.ManifestSchema || back.Duration != rep.Duration ||
		len(back.Artifacts) != 1 || back.Artifacts[0].Key != rep.Artifacts[0].Key ||
		back.Artifacts[0].Chain[0] != rep.Artifacts[0].Chain[0] ||
		back.Artifacts[0].Source != rep.Artifacts[0].Source ||
		back.Artifacts[0].Stats != rep.Artifacts[0].Stats ||
		len(back.Excluded) != 1 || back.Excluded[0] != rep.Excluded[0] {
		t.Fatalf("round trip lost data:\n%+v", back)
	}
}

func TestDurationJSON(t *testing.T) {
	b, err := json.Marshal(model.Duration(90 * time.Second))
	if err != nil || string(b) != `"1m30s"` {
		t.Fatalf("marshal: %s, %v", b, err)
	}
	var d model.Duration
	if err := json.Unmarshal([]byte(`"30s"`), &d); err != nil || d.Std() != 30*time.Second {
		t.Fatalf("unmarshal: %v, %v", d, err)
	}
	if err := json.Unmarshal([]byte(`30`), &d); err == nil {
		t.Fatal("a bare number decoded into a duration")
	}
	if d.String() != "30s" {
		t.Fatalf("String: %q", d.String())
	}
}

// TestSHA256HexVectors: known digests of the empty input, one block and a
// multi-block input, pinned as lowercase hex. A mutation to another algorithm,
// another encoding or a byte-order change changes every one of them.
func TestSHA256HexVectors(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{strings.Repeat("a", 1000), "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"},
	}
	for _, tc := range cases {
		if got := model.SHA256Hex([]byte(tc.in)); got != tc.want {
			t.Errorf("SHA256Hex(%d bytes) = %s, want %s", len(tc.in), got, tc.want)
		}
	}
}

// TestDurationUnmarshalJSONRejectsWhatIsNotADuration: the string form parses,
// a number is not a duration at all, and a bad duration string is an error —
// swallowing either failure would turn a config typo into a zero deadline.
func TestDurationUnmarshalJSONRejectsWhatIsNotADuration(t *testing.T) {
	var d model.Duration
	if err := json.Unmarshal([]byte(`"30s"`), &d); err != nil || d.Std() != 30*time.Second {
		t.Fatalf(`"30s" = %v, %v`, d.Std(), err)
	}
	if err := json.Unmarshal([]byte(`"forever"`), &d); err == nil {
		t.Fatal(`"forever" was accepted`)
	}
	if err := json.Unmarshal([]byte(`30`), &d); err == nil {
		t.Fatal("a bare number was accepted as a duration")
	}
}

// TestRunKindValid pins the two kinds this build knows: every other value,
// including none at all, is invalid and the caller refuses it.
func TestRunKindValid(t *testing.T) {
	for _, tc := range []struct {
		kind  model.RunKind
		valid bool
	}{
		{model.RunKindScheduled, true},
		{model.RunKindManual, true},
		{model.RunKind(""), false},
		{model.RunKind("weekly"), false},
	} {
		if got := tc.kind.Valid(); got != tc.valid {
			t.Errorf("RunKind(%q).Valid() = %v, want %v", tc.kind, got, tc.valid)
		}
	}
}

// TestC4_EmptyStoredKindIsNeverReinterpreted proves the model layer invents no
// kind: an empty, missing or unknown stored kind stays exactly as decoded and
// reports invalid, and no function in the package maps an empty kind to a known
// run kind.
func TestC4_EmptyStoredKindIsNeverReinterpreted(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty", `{"schema":1,"run_id":"r","kind":""}`},
		{"missing", `{"schema":1,"run_id":"r"}`},
		{"unknown", `{"schema":1,"run_id":"r","kind":"weekly"}`},
	} {
		var rep model.RunReport
		if err := json.Unmarshal([]byte(tc.body), &rep); err != nil {
			t.Fatalf("%s kind: unmarshal: %v", tc.name, err)
		}
		want := model.RunKind("")
		if tc.name == "unknown" {
			want = "weekly"
		}
		if rep.Kind != want {
			t.Fatalf("%s kind decoded as %q, want %q unchanged", tc.name, rep.Kind, want)
		}
		if rep.Kind.Valid() {
			t.Fatalf("%s kind %q is valid; it must stay a problem run", tc.name, rep.Kind)
		}
	}
	assertNoKindNormalizer(t)
}

// assertNoKindNormalizer fails when a non-test file of this package compares a
// value with the empty string inside a function that also yields a known run
// kind: that is the removed shortcut that turned an empty stored kind into a
// scheduled run.
func assertNoKindNormalizer(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var comparesEmpty, yieldsKind bool
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.BinaryExpr:
					comparesEmpty = comparesEmpty || n.Op == token.EQL && (isEmptyString(n.X) || isEmptyString(n.Y))
				case *ast.ReturnStmt:
					for _, result := range n.Results {
						yieldsKind = yieldsKind || isRunKindConstant(result)
					}
				}
				return true
			})
			if comparesEmpty && yieldsKind {
				t.Fatalf("%s: %s compares with an empty string and yields a run kind; an empty stored kind must never be reinterpreted",
					name, fn.Name.Name)
			}
		}
	}
}

func isEmptyString(expr ast.Expr) bool {
	literal, ok := expr.(*ast.BasicLit)
	return ok && literal.Kind == token.STRING && literal.Value == `""`
}

func isRunKindConstant(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && (ident.Name == "RunKindScheduled" || ident.Name == "RunKindManual")
}

// TestExclusionKindKnown pins the one enum root every reader and kit asks: the
// three verdicts this build understands, and nothing else.
func TestExclusionKindKnown(t *testing.T) {
	for _, tc := range []struct {
		kind  model.ExclusionKind
		known bool
	}{
		{model.ExclusionIgnored, true},
		{model.ExclusionCovered, true},
		{model.ExclusionElsewhere, true},
		{model.ExclusionKind(""), false},
		{model.ExclusionKind("maybe"), false},
	} {
		if got := tc.kind.Known(); got != tc.known {
			t.Errorf("ExclusionKind(%q).Known() = %v, want %v", tc.kind, got, tc.known)
		}
	}
}

// TestStoredRunPolicyInputIsNeverStored: the artifact identities retention reads
// are policy input the protection ledger fills after a survey — they are not
// part of the stored run and must never reach a manifest or a listing.
func TestStoredRunPolicyInputIsNeverStored(t *testing.T) {
	run := model.StoredRun{
		RunID: "20261001T040000Z", Kind: model.RunKindScheduled, Manifest: true,
		Artifacts:         []string{"databases/db/postgres"},
		ObservedArtifacts: []string{"databases/db/postgres"},
	}
	body, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "databases/db/postgres") || strings.Contains(string(body), "Observed") {
		t.Fatalf("policy input reached the stored form: %s", body)
	}
}

// TestEnumListsAnswerMembership: every value list is the one membership source
// the readers share — Valid, Known, the manifest validator — and a value
// outside it is refused.
func TestEnumListsAnswerMembership(t *testing.T) {
	if !model.RunKind(model.RunKindScheduled).Valid() || !model.RunKind(model.RunKindManual).Valid() {
		t.Fatal("a declared run kind is not valid")
	}
	if model.RunKind("hourly").Valid() {
		t.Fatal("an unknown run kind is valid")
	}
	if !model.ExclusionIgnored.Known() || !model.ExclusionCovered.Known() || !model.ExclusionElsewhere.Known() {
		t.Fatal("a declared exclusion kind is not known")
	}
	if model.ExclusionKind("guess").Known() {
		t.Fatal("an unknown exclusion kind is known")
	}
	if !model.Listed(model.ArtifactOutcomeValues, model.ArtifactOK) || !model.Listed(model.ArtifactOutcomeValues, model.ArtifactFailed) {
		t.Fatal("a declared artifact outcome is not listed")
	}
	if model.Listed(model.ArtifactOutcomeValues, "partial") {
		t.Fatal("an unknown artifact outcome is listed")
	}
	for _, outcome := range []string{model.StorageVerified, model.StorageFailed, model.StorageSkipped} {
		if !model.Listed(model.StorageOutcomeValues, outcome) {
			t.Fatalf("a declared storage outcome %q is not listed", outcome)
		}
	}
	if model.Listed(model.StorageOutcomeValues, "lost") {
		t.Fatal("an unknown storage outcome is listed")
	}
}
