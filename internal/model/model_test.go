package model_test

import (
	"strings"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
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

// TestRunKindValidAndNormalizeKind pins the two kinds this build knows and the
// one compatibility rule a stored manifest carries: a manifest written before
// the key existed is a scheduled run, and any other value is handed back
// unchanged for the caller to refuse — NormalizeKind never invents a kind.
func TestRunKindValidAndNormalizeKind(t *testing.T) {
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
	if got := model.NormalizeKind(""); got != model.RunKindScheduled {
		t.Errorf("NormalizeKind(\"\") = %q, want scheduled", got)
	}
	for _, k := range []model.RunKind{model.RunKindScheduled, model.RunKindManual, "weekly"} {
		if got := model.NormalizeKind(k); got != k {
			t.Errorf("NormalizeKind(%q) = %q, want it unchanged", k, got)
		}
	}
}
