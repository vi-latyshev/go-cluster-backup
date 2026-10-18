package cli

import (
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// TestWriteFieldsMarksEveryClassification: a struct member's children carry the
// classification, a member the document fills carries its default, and one the
// deployment states is required.
func TestWriteFieldsMarksEveryClassification(t *testing.T) {
	fields := []plugin.Field{
		{Name: "nested", Type: "struct", Doc: "a struct", Fields: []plugin.Field{
			{Name: "inner", Type: "string", Required: true, Doc: "required"},
		}},
		{Name: "defaulted", Type: "string", Doc: "defaulted"},
		{Name: "stated", Type: "string", Required: true, Doc: "required"},
		{Name: "opaque", Type: "string", Required: true, Secret: true, Doc: "a secret"},
	}
	var out strings.Builder
	writeFields(&errWriter{w: &out}, fields, jsontext.Value(`{"nested":{},"defaulted":"v"}`), 1)
	text := out.String()
	for _, want := range []string{
		"nested struct", "  inner string [required]", `defaulted string [default: "v"]`,
		"stated string [required]", "opaque string [secret] [required]",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("writeFields does not carry %q:\n%s", want, text)
		}
	}
}

// TestWriteDescriptorRefusesAnExampleItCannotIndent: a descriptor handed an
// example that is not JSON is reported, never printed raw.
func TestWriteDescriptorRefusesAnExampleItCannotIndent(t *testing.T) {
	var stdout, stderr strings.Builder
	d := plugin.Descriptor{Kind: "source", Type: "probe", Stability: plugin.Experimental,
		Summary: "a probe", Example: jsontext.Value("{not json")}
	if code := writeDescriptor(&stdout, &stderr, "backup", nil, d); code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "example") {
		t.Errorf("the failure does not name the example: %s", stderr.String())
	}
}
