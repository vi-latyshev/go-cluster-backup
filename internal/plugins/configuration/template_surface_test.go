package configuration_test

import (
	"maps"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier/webhook"
)

// A deployment's configuration file spells the names a message template reads:
// the Go field names of the model types, and the template functions. That is a
// SemVer API — renaming a field or a function is a breaking change a config file
// feels at 04:20 — so the surface is pinned here, exactly like a stored-format
// default. A gopls rename that reaches the model or a func map fails these tests.

// modelPkg is the package whose struct types a template reaches.
var modelPkg = reflect.TypeOf(model.Event{}).PkgPath()

// pinnedFields is the exported field set of every model type reachable from the
// template data. TestTemplateModelTypesAreAllPinned proves the list is complete:
// a new reachable type fails until it is added here.
var pinnedFields = map[string][]string{
	"Event":                {"At", "Name", "Run"},
	"RunReport":            {"Artifacts", "Duration", "Excluded", "ExitCode", "Kind", "Lease", "Outcome", "Previous", "RunID", "Schema", "StartedAt", "Storages", "Warnings", "WriterIdentity"},
	"ArtifactResult":       {"Artifact", "Bytes", "Chain", "Duration", "Error", "Hint", "Key", "Outcome", "Pipeline", "RawBytes", "SHA256", "Source", "Stats", "Warnings"},
	"Artifact":             {"Ext", "Group", "Importance", "Name", "Sealed", "Sensitivity"},
	"SourceRef":            {"Instance", "Origin", "Type"},
	"ProcessorStep":        {"Ext", "Instance", "Seals", "Type"},
	"DumpStats":            {"ChangedFiles", "PaddedFiles", "SkippedEntries"},
	"StorageResult":        {"Attempts", "Bytes", "CapEvicted", "CapExceeded", "CapLimitBytes", "CapOverflowBytes", "CapUsedBytes", "Deleted", "Duration", "Error", "Freed", "Kept", "Objects", "OccupancyUnknown", "Outcome", "RetentionError", "Storage", "Type"},
	"Exclusion":            {"Group", "Kind", "Name", "Node", "Reason"},
	"Warning":              {"Code", "Message"},
	"PreviousRun":          {"Attempts", "Failure", "LastGoodAt", "RunID", "Unavailable", "Unfinished"},
	"PreviousFailure":      {"Consecutive", "Containers", "Events", "Job", "Message", "Reason"},
	"AttemptFailure":       {"Deleted", "Failed", "Job", "Pods"},
	"FailedPod":            {"Containers", "Message", "Name", "Reason"},
	"ContainerFailure":     {"Container", "Init", "Pod", "Terminated", "Waiting"},
	"ContainerTermination": {"ExitCode", "Message", "Reason"},
	"EventFailure":         {"Message", "Reason"},
	"LeaseState":           {"Error", "Holder", "HolderKind", "Refused"},
}

func pinnedType(name string) (reflect.Type, bool) {
	for _, t := range []reflect.Type{
		reflect.TypeOf(model.Event{}), reflect.TypeOf(model.RunReport{}),
		reflect.TypeOf(model.ArtifactResult{}), reflect.TypeOf(model.Artifact{}),
		reflect.TypeOf(model.SourceRef{}), reflect.TypeOf(model.ProcessorStep{}),
		reflect.TypeOf(model.DumpStats{}), reflect.TypeOf(model.StorageResult{}),
		reflect.TypeOf(model.Exclusion{}), reflect.TypeOf(model.PreviousRun{}),
		reflect.TypeOf(model.PreviousFailure{}), reflect.TypeOf(model.AttemptFailure{}),
		reflect.TypeOf(model.FailedPod{}),
		reflect.TypeOf(model.ContainerFailure{}),
		reflect.TypeOf(model.ContainerTermination{}), reflect.TypeOf(model.EventFailure{}),
		reflect.TypeOf(model.LeaseState{}), reflect.TypeOf(model.Warning{}),
	} {
		if t.Name() == name {
			return t, true
		}
	}
	return nil, false
}

// TestTemplateModelFieldsArePinned fails on any change of the exported field set
// of a model type a template reads.
func TestTemplateModelFieldsArePinned(t *testing.T) {
	for name, want := range pinnedFields {
		typ, ok := pinnedType(name)
		if !ok {
			t.Errorf("%s is pinned but not declared in pinnedType", name)
			continue
		}
		if got := exportedFields(typ); !slices.Equal(got, want) {
			t.Errorf("%s's exported fields = %v, want %v.\n"+
				"This is the template-visible surface: a deployment's config file spells these names, "+
				"so a rename breaks a message at run time with no compile error.", name, got, want)
		}
	}
}

// TestTemplateModelTypesAreAllPinned walks the type graph a template can reach
// from a run event and the run report, and fails on any model type that is
// reachable but not pinned — so adding a field of a new struct type cannot slip
// past the pin by being forgotten.
func TestTemplateModelTypesAreAllPinned(t *testing.T) {
	reachable := map[string]reflect.Type{}
	collectModelTypes(reflect.TypeOf(model.Event{}), reachable)
	collectModelTypes(reflect.TypeOf(model.RunReport{}), reachable)

	names := make([]string, 0, len(reachable))
	for name := range reachable {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, ok := pinnedFields[name]; !ok {
			t.Errorf("model.%s is reachable from template data but not pinned; add it to pinnedFields and pinnedType", name)
		}
	}
	if len(reachable) != len(pinnedFields) {
		var stale []string
		for name := range pinnedFields {
			if _, ok := reachable[name]; !ok {
				stale = append(stale, name)
			}
		}
		sort.Strings(stale)
		t.Errorf("pinned types no template can reach: %v; drop them from pinnedFields and pinnedType", stale)
	}
}

// collectModelTypes records every struct type of the model package reachable
// through exported fields, following pointers, slices, arrays and maps. The
// webhook's fill gate (internal/plugins/notifier/webhook/sample_test.go) walks
// the same graph from the run values: a shape one walker cannot measure has to
// be taught to both.
func collectModelTypes(t reflect.Type, out map[string]reflect.Type) {
	for {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			t = t.Elem()
			continue
		case reflect.Map:
			collectModelTypes(t.Key(), out)
			t = t.Elem()
			continue
		}
		break
	}
	if t.Kind() != reflect.Struct || t.PkgPath() != modelPkg {
		return
	}
	if _, seen := out[t.Name()]; seen {
		return
	}
	out[t.Name()] = t
	for i := range t.NumField() {
		if f := t.Field(i); f.IsExported() {
			collectModelTypes(f.Type, out)
		}
	}
}

// TestTemplateFuncsArePinned fails on any change of a template function name in
// the core set or in a plugin that adds its own.
func TestTemplateFuncsArePinned(t *testing.T) {
	cases := map[string]struct {
		funcs []string
		want  []string
	}{
		"core": {slices.Sorted(maps.Keys(configuration.CoreFuncs())),
			[]string{"bytes", "date", "duration", "json", "pick", "sibytes"}},
		"webhook": {slices.Sorted(maps.Keys(webhook.Config{}.TemplateFuncs())),
			[]string{"blank", "byGroup", "clip", "failed", "pickdef", "sumBytes"}},
		// The endpoint-only function set: what endpoint_templates: true renders
		// url and header values with, and the one function payload and message
		// templates never see.
		"webhook-endpoint": {slices.Sorted(maps.Keys(webhook.EndpointFuncs())),
			[]string{"secret"}},
		"kubernetes_labels": {slices.Sorted(maps.Keys((&kuberneteslabels.Config{}).TemplateFuncs())),
			[]string{"envFile", "envValue", "label", "port", "volumePath"}},
	}
	for name, tc := range cases {
		if !slices.Equal(tc.funcs, tc.want) {
			t.Errorf("%s template funcs = %v, want %v.\n"+
				"A template function name is what a config file writes; renaming one breaks every "+
				"message that calls it.", name, tc.funcs, tc.want)
		}
	}
}

func exportedFields(t reflect.Type) []string {
	out := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		if f := t.Field(i); f.IsExported() {
			out = append(out, f.Name)
		}
	}
	sort.Strings(out)
	return out
}
