package engine

import (
	"reflect"
	"slices"
	"sort"
	"testing"
)

// The layout templates a deployment writes read the Go field names of the key
// data: `runs/{{ .Run.ID }}` and
// `{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}`. They are part
// of the template-visible surface and are pinned here, so a rename fails CI
// rather than orphaning every run already on every storage.
func TestLayoutTemplateFieldsArePinned(t *testing.T) {
	cases := map[string]struct {
		typ  reflect.Type
		want []string
	}{
		"keyData": {reflect.TypeOf(keyData{}), []string{"Artifact", "Run"}},
		"keyRun":  {reflect.TypeOf(keyRun{}), []string{"ID", "StartedAt"}},
	}
	for name, tc := range cases {
		got := make([]string, 0, tc.typ.NumField())
		for i := range tc.typ.NumField() {
			if f := tc.typ.Field(i); f.IsExported() {
				got = append(got, f.Name)
			}
		}
		sort.Strings(got)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s fields = %v, want %v: layout templates spell these names", name, got, tc.want)
		}
	}
}
