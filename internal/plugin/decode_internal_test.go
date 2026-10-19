package plugin

import (
	"reflect"
	"testing"
	"text/template"
)

// TestParseTemplatesLeavesANonAddressableTemplateAlone: a Template that is not
// addressable cannot be finalized in place, and the walk reports nothing rather
// than panicking on Addr. reflect.ValueOf yields such a value; a struct reached
// through an interface field is the other way to get one.
func TestParseTemplatesLeavesANonAddressableTemplateAlone(t *testing.T) {
	errs := parseTemplates(reflect.ValueOf(Template{text: "{{ .X }}"}), template.FuncMap{}, "/t")
	if len(errs) != 0 {
		t.Fatalf("parseTemplates = %v, want none", errs)
	}
}
