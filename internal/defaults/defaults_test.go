package defaults

import (
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// storedFormatPins is every value of the embedded document a later run reads
// back — a key, a layout, an encoding an artifact written weeks ago is found
// and decoded by. They are defaults on purpose (no deployment has a reason to
// state them, and the owner asked for them out of the deployment's file), and that is
// exactly why each one is pinned here: a changed layout orphans every run
// already on every storage — the survey sees nothing, retention stops deleting
// and a restore finds nothing — and nothing at run time would ever say so.
//
// why is the reason a failure message carries, so the next reader meets the
// argument before the temptation to "just update the test".
var storedFormatPins = []struct {
	path []string // the document's own keys, a plugin block being one of them
	want string
	why  string
}{
	{[]string{"layout", "run_prefix"}, "runs/{{ .Run.ID }}",
		"every run of every storage lives under this prefix"},
	{[]string{"layout", "artifact_key"}, "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}",
		"this is the key of every object already uploaded"},
	{[]string{"layout", "manifest_key"}, "manifest.json",
		"a survey finds a run's manifest, and retention judges it, by this key alone"},
	{[]string{"run", "id_layout"}, "20060102T150405Z",
		"run ids of existing runs were written with this layout, and a manifest's key is built from its id"},
	{[]string{"layout", "manual_id_suffix"}, "-manual",
		"a survey derives a run's kind from its id by this suffix, so changing it makes every manual run already written read as a scheduled one"},
	{[]string{"plugins", "catalog/kubernetes_configmap", "key"}, "catalog.json",
		"the catalog ConfigMap holds the run history under this key"},
	{[]string{"plugins", "catalog/kubernetes_configmap", "lease_key_suffix"}, ".lease",
		"another process reads the writer lease under this key; changing it makes every live claim invisible"},
	{[]string{"plugins", "source/postgres", "format"}, "custom",
		"a dump already written is read back with the tool this format names (pg_restore against psql), and the artifact's extension comes from it"},
}

// TestStoredFormatDefaultsArePinned fails on any change of a value above, and
// on the value going missing from the document.
func TestStoredFormatDefaultsArePinned(t *testing.T) {
	var doc map[string]any
	if err := yaml.Unmarshal(Defaults, &doc); err != nil {
		t.Fatalf("the embedded document does not parse: %v", err)
	}
	for _, pin := range storedFormatPins {
		at := strings.Join(pin.path, "/")
		got, ok := lookup(doc, pin.path)
		switch {
		case !ok:
			t.Errorf("defaults.yaml no longer carries %s; it is a stored-format constant (%s)", at, pin.why)
		case got != pin.want:
			t.Errorf("defaults.yaml has %s = %q, want %q.\n"+
				"This value is a stored-format constant: %s.\n"+
				"Changing it orphans every run already written on every storage — the survey sees nothing, retention stops deleting, a restore finds nothing. "+
				"If that is really meant, every existing run is abandoned, and this pin is the only place that says so.",
				at, got, pin.want, pin.why)
		}
	}
}

// lookup reads one path of a YAML document, one document key per segment.
func lookup(doc map[string]any, path []string) (string, bool) {
	var cur any = doc
	for _, seg := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = obj[seg]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

// TestLeaseWaitDefaultIsMeaningful: the wait is what a scheduled run spends on a
// live holder before it gives up. It must be long enough for a normal run to
// finish and to poll at least once; a dead holder is taken over at once.
func TestLeaseWaitDefaultIsMeaningful(t *testing.T) {
	var doc map[string]any
	if err := yaml.Unmarshal(Defaults, &doc); err != nil {
		t.Fatalf("the embedded document does not parse: %v", err)
	}
	waitText, ok := lookup(doc, []string{"run", "lease_wait"})
	if !ok {
		t.Fatal("defaults.yaml carries no run.lease_wait")
	}
	wait, err := time.ParseDuration(waitText)
	if err != nil {
		t.Fatalf("run.lease_wait %q: %v", waitText, err)
	}
	pollText, ok := lookup(doc, []string{"run", "lease_poll"})
	if !ok {
		t.Fatal("defaults.yaml carries no run.lease_poll")
	}
	poll, err := time.ParseDuration(pollText)
	if err != nil {
		t.Fatalf("run.lease_poll %q: %v", pollText, err)
	}
	if wait < time.Minute {
		t.Errorf("run.lease_wait = %s, want at least a minute: a normal holder must be allowed to finish", wait)
	}
	if poll <= 0 || poll > wait {
		t.Errorf("run.lease_poll = %s against a %s wait", poll, wait)
	}
}
