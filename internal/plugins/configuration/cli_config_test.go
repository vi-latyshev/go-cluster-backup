package configuration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	json "encoding/json/v2"
)

// TestReadConfigConvertsYAMLAndNamesTheFile.
func TestReadConfigConvertsYAMLAndNamesTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("run:\n  spool_dir: /spool\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := ReadConfig(path)
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if got := string(raw); got != `{"run":{"spool_dir":"/spool"}}` {
		t.Fatalf("ReadConfig = %s", got)
	}

	if _, err := ReadConfig(filepath.Join(dir, "absent.yaml")); err == nil {
		t.Error("ReadConfig accepted a missing file")
	}
	broken := filepath.Join(dir, "broken.yaml")
	if err := os.WriteFile(broken, []byte("\tnot: [yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConfig(broken); err == nil || !strings.Contains(err.Error(), broken) {
		t.Errorf("ReadConfig(broken) = %v, want the file named", err)
	}
}

// TestDefaultsDocumentsNamesTheBrokenOne.
func TestDefaultsDocumentsNamesTheBrokenOne(t *testing.T) {
	docs, err := DefaultsDocuments([][]byte{
		[]byte("plugins:\n  bundles: []\n"),
		[]byte("run:\n  deadline: 1m\n"),
	})
	if err != nil {
		t.Fatalf("DefaultsDocuments: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("DefaultsDocuments returned %d documents", len(docs))
	}
	if _, err := DefaultsDocuments([][]byte{[]byte("run: {}\n"), []byte("\tnot: [yaml")}); err == nil || !strings.Contains(err.Error(), "defaults document 2") {
		t.Errorf("DefaultsDocuments(broken) = %v, want the document named", err)
	}
}

// TestYAMLJSONIsTheWholeOfTheLayer: the conversion keeps what this build does
// not know, and refuses what YAML cannot parse or JSON cannot carry.
func TestYAMLJSONIsTheWholeOfTheLayer(t *testing.T) {
	raw, err := YAMLJSON([]byte("a: 1\nb: [x, y]\n"))
	if err != nil {
		t.Fatalf("YAMLJSON: %v", err)
	}
	for _, want := range []string{`"a":1`, `"b":["x","y"]`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("YAMLJSON = %s, want it to carry %s", raw, want)
		}
	}
	if _, err := YAMLJSON([]byte("\tnot: [yaml")); err == nil {
		t.Error("YAMLJSON accepted malformed YAML")
	}
	if _, err := YAMLJSON([]byte("a: .nan\n")); err == nil || !strings.Contains(err.Error(), "converting YAML to JSON") {
		t.Errorf("YAMLJSON(.nan) = %v, want the conversion refused", err)
	}
}

// TestYAMLJSONKeepsTheScalarTable: the v4 upgrade must not change what a YAML
// document becomes — the YAML 1.2 scalar table stays (yes/no/on/off are
// strings, true is a boolean, 0o17 and 0777 are octal, 1_000 is 1000, null and
// ~ are null), and string keys stay string keys.
func TestYAMLJSONKeepsTheScalarTable(t *testing.T) {
	raw, err := YAMLJSON([]byte("s1: yes\ns2: no\ns3: on\ns4: off\nb: true\nn1: null\nn2: ~\no1: 0o17\no2: 0777\nu: 1_000\n"))
	if err != nil {
		t.Fatalf("YAMLJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the converted document is not JSON: %v", err)
	}
	for key, want := range map[string]any{
		"s1": "yes", "s2": "no", "s3": "on", "s4": "off",
		"b": true, "o1": float64(15), "o2": float64(511), "u": float64(1000),
	} {
		if doc[key] != want {
			t.Errorf("%s = %#v, want %#v", key, doc[key], want)
		}
	}
	for _, key := range []string{"n1", "n2"} {
		if v, present := doc[key]; !present || v != nil {
			t.Errorf("%s = %#v (present=%v), want null", key, v, present)
		}
	}
}
