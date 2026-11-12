package webhook

import (
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestSampleMatrixCoversKindsAndRefusals: every branch a template may take has a
// sample that reaches it. A manual run_started, a lease refusal and a cap
// refusal are the three the matrix gained last, and each is one a template that
// only ever saw a scheduled night would render wrong.
func TestSampleMatrixCoversKindsAndRefusals(t *testing.T) {
	var startedManual, finishedManual, leaseRefused, capRefused bool
	for _, s := range samples() {
		if s.ev.Run.Kind == model.RunKindManual {
			if s.ev.Name == model.EventStarted {
				startedManual = true
			}
			if s.ev.Name == model.EventFinished {
				finishedManual = true
			}
		}
		if s.ev.Run.Lease != nil && s.ev.Run.Lease.Refused {
			leaseRefused = true
		}
		for _, st := range s.ev.Run.Storages {
			if st.CapExceeded {
				capRefused = true
			}
		}
	}
	for _, c := range []struct {
		ok   bool
		name string
	}{
		{startedManual, "a manual run_started sample (the manual start branch never renders without one)"},
		{finishedManual, "a manual run_finished sample"},
		{leaseRefused, "a lease-refusal sample (Run.Lease.Refused)"},
		{capRefused, "a cap-refusal sample (StorageResult.CapExceeded)"},
	} {
		if !c.ok {
			t.Errorf("the sample matrix has no %s", c.name)
		}
	}
}

// previousShapes records which shapes of the previous-run record a sample
// carries.
type previousShapes struct {
	failure       bool // a previous Job failure with every container shape and its events
	containerless bool // failed earlier attempts with a pod that has no container status
	gone          bool // failed earlier attempts whose pods are all deleted
	unavailable   bool // a status the cluster could not answer
}

// TestSamplesCarryEveryPreviousShape: the previous-run record has two halves and
// a template may read both in either event. The storage half exists only for
// run_finished — a started sample carrying it would pass validation on data no
// started event ever has — while the cluster half reaches both: a previous Job's
// failure with each container shape, its events and its Consecutive count;
// failed earlier attempts with a containerless pod and with every pod gone; and
// an unreadable status.
func TestSamplesCarryEveryPreviousShape(t *testing.T) {
	var started, finished previousShapes
	for _, s := range samples() {
		prev := s.ev.Run.Previous
		var shapes *previousShapes
		switch s.ev.Name {
		case model.EventStarted:
			shapes = &started
			if prev.RunID != "" || prev.Unfinished || !prev.LastGoodAt.IsZero() {
				t.Errorf("%s: a started event carries no storage half, and this sample has %+v", s.name, prev)
			}
		case model.EventFinished:
			shapes = &finished
		}
		if prev.Failure != nil {
			var initTerminated, waiting, terminated bool
			for _, c := range prev.Failure.Containers {
				switch {
				case c.Init && c.Terminated != nil:
					initTerminated = true
				case c.Waiting != "":
					waiting = true
				case c.Terminated != nil:
					terminated = true
				}
			}
			if initTerminated && waiting && terminated && len(prev.Failure.Events) > 0 && prev.Failure.Consecutive > 1 {
				shapes.failure = true
			}
		}
		if prev.Attempts != nil {
			if prev.Attempts.Deleted > 0 && len(prev.Attempts.Pods) == 0 {
				shapes.gone = true
			}
			for _, pod := range prev.Attempts.Pods {
				if len(pod.Containers) == 0 && pod.Reason != "" {
					shapes.containerless = true
				}
			}
		}
		if prev.Unavailable != "" {
			shapes.unavailable = true
		}
	}
	for _, c := range []struct {
		ok   bool
		name string
	}{
		{started.failure, "a run_started sample with a previous Job failure (init terminated, waiting and terminated containers, events, Consecutive)"},
		{started.containerless, "a run_started sample with an attempt whose pod left no container status"},
		{started.gone, "a run_started sample with attempts whose pods are all deleted"},
		{started.unavailable, "a run_started sample with an unreadable cluster status"},
		{finished.failure, "a run_finished sample with a previous Job failure"},
		{finished.containerless, "a run_finished sample with an attempt whose pod left no container status"},
		{finished.gone, "a run_finished sample with attempts whose pods are all deleted"},
		{finished.unavailable, "a run_finished sample with an unreadable cluster status"},
	} {
		if !c.ok {
			t.Errorf("the sample matrix has no %s", c.name)
		}
	}
}

// maximalStarted and maximalFinished are the test-only maxima: every exported
// path a template can reach is non-zero with a valid enum value, lists carry
// two entries, and pointers are non-nil. Validate never executes them; they
// drive the fill gate and the checker-execution agreement. The started maximum
// leaves every notOnStarted path zero, because production sends those only with
// the finish.
func maximalFinished(at time.Time) model.Event {
	lastGood := at.Add(-24 * time.Hour)
	failure := &model.PreviousFailure{
		Job: "backup-max-previous", Reason: "BackoffLimitExceeded", Message: "the backoff limit",
		Consecutive: 2,
		Containers: []model.ContainerFailure{
			{Pod: "previous-a", Container: "init", Init: true,
				Terminated: &model.ContainerTermination{Reason: "Error", ExitCode: 1, Message: "invalid config"}},
			{Pod: "previous-a", Container: "backup", Waiting: "ImagePullBackOff"},
			{Pod: "previous-b", Container: "backup",
				Terminated: &model.ContainerTermination{Reason: "OOMKilled", ExitCode: 137, Message: "last lines"}},
		},
		Events: []model.EventFailure{{Reason: "FailedCreate", Message: "pods is forbidden"}},
	}
	attempts := &model.AttemptFailure{
		Job: "backup-max", Failed: 2, Deleted: 1,
		Pods: []model.FailedPod{
			{Name: "attempt-a", Reason: "Evicted", Message: "the node was low on memory"},
			{Name: "attempt-b", Containers: []model.ContainerFailure{
				{Pod: "attempt-b", Container: "stage", Init: true,
					Terminated: &model.ContainerTermination{Reason: "Error", ExitCode: 1, Message: "invalid stage config"}},
				{Pod: "attempt-b", Container: "backup", Waiting: "PodInitializing"},
				{Pod: "attempt-b", Container: "backup",
					Terminated: &model.ContainerTermination{Reason: "OOMKilled", ExitCode: 137, Message: "last lines"}},
			}},
		},
	}
	okArtifact := model.ArtifactResult{
		Artifact: model.Artifact{
			Name: "example/postgres", Group: "databases", Ext: ".dump.zst",
			Sensitivity: model.SensitivitySecret, Importance: model.ImportanceBestEffort, Sealed: true,
		},
		Outcome: model.ArtifactOK, Key: "runs/max/databases/example/postgres.dump.zst",
		Chain: []model.ProcessorStep{
			{Instance: "zstd", Type: "zstd", Ext: ".zst"},
			{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
		},
		Source:   model.SourceRef{Instance: "postgres", Type: "postgres", Origin: "postgres.example.com:5432/app"},
		Pipeline: "zstd+seal", RawBytes: 2, Bytes: 1,
		Stats:  model.DumpStats{SkippedEntries: 1, PaddedFiles: 1, ChangedFiles: 1},
		SHA256: "sha256-max", Duration: model.Duration(time.Second),
		Error: "the tree changed under the read", Hint: "re-run when the workload is quiet",
		Warnings: []model.Warning{{Code: "artifact.warning", Message: "a non-fatal artifact fact"}},
	}
	failedArtifact := okArtifact
	failedArtifact.Outcome = model.ArtifactFailed
	storage := model.StorageResult{
		Storage: "primary", Type: "s3", Outcome: model.StorageVerified,
		Objects: 1, Bytes: 1, Duration: model.Duration(time.Second), Attempts: 1,
		CapExceeded: true, CapLimitBytes: 2, CapUsedBytes: 1,
		Kept: 1, Deleted: []string{"old-a", "old-b"}, Freed: 1,
		Error: "the upload failed", RetentionError: "the retention pass failed",
		CapOverflowBytes: 1, CapEvicted: []string{"evicted-a"}, OccupancyUnknown: true,
	}
	failedStorage := storage
	failedStorage.Outcome = model.StorageFailed
	excluded := []model.Exclusion{
		{Name: "volumes/example/a", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "the marker", Node: "node-a"},
		{Name: "volumes/example/b", Group: "volumes", Kind: model.ExclusionElsewhere, Reason: "a declared peer", Node: "node-b"},
	}
	report := model.RunReport{
		Schema: model.ManifestSchema, RunID: "20260102T030405Z", Kind: model.RunKindScheduled,
		StartedAt: at, Duration: model.Duration(time.Minute), Outcome: model.OutcomeDegraded,
		Artifacts: []model.ArtifactResult{okArtifact, failedArtifact},
		Storages:  []model.StorageResult{storage, failedStorage},
		Excluded:  excluded,
		Previous: model.PreviousRun{
			RunID: "20260101T030405Z", Unfinished: true, LastGoodAt: lastGood,
			Failure: failure, Attempts: attempts, Unavailable: "previous status unavailable",
		},
		ExitCode: 1, WriterIdentity: "writer-max",
		Lease:    &model.LeaseState{Refused: true, Holder: "20260101T030000Z", HolderKind: model.RunKindManual, Error: "the wait expired"},
		Warnings: []model.Warning{{Code: "run.warning", Message: "a non-fatal run fact"}},
	}
	return model.Event{Name: model.EventFinished, At: at, Run: report}
}

// maximal is the test-only maximum of one event.
func maximal(event string) model.Event {
	at := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finished := maximalFinished(at)
	if event == model.EventStarted {
		return maximalStarted(at, finished)
	}
	return finished
}

// maximalStarted is the finished maximum reduced to what a started event
// carries: the storage half, the outcome, the crosses, the artifacts, the
// storages, the lease and the warnings are zero.
func maximalStarted(at time.Time, finished model.Event) model.Event {
	return model.Event{Name: model.EventStarted, At: at, Run: model.RunReport{
		Schema: model.ManifestSchema, RunID: finished.Run.RunID, Kind: finished.Run.Kind,
		StartedAt: at, Outcome: model.OutcomeRunning, Excluded: finished.Run.Excluded,
		WriterIdentity: finished.Run.WriterIdentity,
		Previous: model.PreviousRun{
			Failure: finished.Run.Previous.Failure, Attempts: finished.Run.Previous.Attempts,
			Unavailable: finished.Run.Previous.Unavailable,
		},
	}}
}

// notOnStarted is every run-report field path a run_started event never
// carries; the fill gate is two-sided and each prefix must name a reachable
// path.
var notOnStarted = []string{
	"Run.Duration", "Run.ExitCode", "Run.Artifacts", "Run.Storages", "Run.Lease", "Run.Warnings",
	"Run.Previous.RunID", "Run.Previous.Unfinished", "Run.Previous.LastGoodAt",
}

func isNotOnStarted(path string) bool {
	for _, prefix := range notOnStarted {
		if path == prefix || strings.HasPrefix(path, prefix+".") {
			return true
		}
	}
	return false
}

// walkedPaths is one walk's answer: every exported leaf path a template can
// reach from the root, and which of them this value filled.
type walkedPaths struct {
	paths  map[string]bool
	filled map[string]bool
}

// walkModel is the one D1 walker the gates share, over values and over the zero
// value (a type-only reachability walk). Root package only: it descends into
// structs of that package through pointers and slices; a struct of another
// package and every scalar is a leaf; a nil pointer walks its zero element and
// an empty slice its zero element. A map, interface or anonymous field is a
// shape the walker has to learn, so it fails the test by path. A leaf is filled
// when the value is non-zero with the walker's own rule: a slice by Len>0, a
// pointer by being non-nil, everything else by !IsZero.
func walkModel(t *testing.T, root any) walkedPaths {
	t.Helper()
	v := reflect.ValueOf(root)
	w := modelWalker{
		t:      t,
		pkg:    v.Type().PkgPath(),
		paths:  map[string]bool{},
		filled: map[string]bool{},
	}
	w.value(v, "")
	return walkedPaths{paths: w.paths, filled: w.filled}
}

type modelWalker struct {
	t      *testing.T
	pkg    string
	paths  map[string]bool
	filled map[string]bool
}

func (w *modelWalker) value(v reflect.Value, path string) {
	for v.Kind() == reflect.Pointer {
		w.paths[path] = true
		if v.IsNil() {
			v = reflect.Zero(v.Type().Elem())
			continue
		}
		w.filled[path] = true
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map, reflect.Interface:
		w.t.Fatalf("%s: the walker cannot measure a %s; teach it the shape", path, v.Kind())
	case reflect.Slice:
		w.paths[path] = true
		if v.Len() > 0 {
			w.filled[path] = true
		}
		if v.Len() == 0 {
			w.value(reflect.Zero(v.Type().Elem()), path)
			return
		}
		for i := 0; i < v.Len(); i++ {
			w.value(v.Index(i), path)
		}
	case reflect.Struct:
		if v.Type().PkgPath() != w.pkg {
			w.paths[path] = true
			if !v.IsZero() {
				w.filled[path] = true
			}
			return
		}
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if !f.IsExported() {
				continue
			}
			if f.Anonymous {
				w.t.Fatalf("%s: anonymous field %s; teach the walker", path, f.Name)
			}
			child := f.Name
			if path != "" {
				child = path + "." + f.Name
			}
			w.value(v.Field(i), child)
		}
	default:
		w.paths[path] = true
		if !v.IsZero() {
			w.filled[path] = true
		}
	}
}

// TestMaximalFillsEveryTemplateField is the structural gate: every exported
// field a template can reach is non-zero in the maximum of its event, so a
// field added to the model fails until sampled. A started maximum fills nothing
// under notOnStarted, and each prefix names a reachable path.
func TestMaximalFillsEveryTemplateField(t *testing.T) {
	paths := walkModel(t, model.Event{}).paths
	for _, event := range []string{model.EventStarted, model.EventFinished} {
		filled := walkModel(t, maximal(event)).filled
		for p := range paths {
			if event == model.EventStarted && isNotOnStarted(p) {
				if filled[p] {
					t.Errorf("run_started maximal fills %s, which a started event never carries", p)
				}
				continue
			}
			if !filled[p] {
				t.Errorf("%s maximum does not fill %s", event, p)
			}
		}
	}
	for _, prefix := range notOnStarted {
		matched := false
		for p := range paths {
			if p == prefix || strings.HasPrefix(p, prefix+".") {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("notOnStarted names %s, but no reachable path is under it", prefix)
		}
	}
}

// referenceTemplate generates the template that reads one reachable path, using
// range for every slice it crosses.
func referenceTemplate(t *testing.T, path, lastName string) string {
	t.Helper()
	cur := reflect.TypeFor[model.Event]()
	parts := strings.Split(path, ".")
	parts[len(parts)-1] = lastName
	var build func(parts []string, cur reflect.Type, expr string) string
	build = func(parts []string, cur reflect.Type, expr string) string {
		if len(parts) == 0 {
			return "{{ " + expr + " }}"
		}
		for cur.Kind() == reflect.Pointer {
			cur = cur.Elem()
		}
		if cur.Kind() == reflect.Slice || cur.Kind() == reflect.Array {
			return "{{ range " + expr + " }}" + build(parts, cur.Elem(), ".") + "{{ end }}"
		}
		if len(parts) == 1 {
			// The leaf is emitted as written: a mutated name must reach the
			// checker, not fail this generator.
			return "{{ " + joinExpr(expr, parts[0]) + " }}"
		}
		f, ok := cur.FieldByName(parts[0])
		if !ok {
			t.Fatalf("%s: %s has no field %q", path, cur, parts[0])
		}
		next := joinExpr(expr, parts[0])
		if f.Type.Kind() == reflect.Pointer {
			// A pointer may be nil on one element: guard the read, still
			// checking the type on the other side.
			return "{{ with " + next + " }}" + build(parts[1:], f.Type, ".") + "{{ end }}"
		}
		return build(parts[1:], f.Type, next)
	}
	return build(parts, cur, ".")
}

// joinExpr extends one dot expression by one field name.
func joinExpr(expr, name string) string {
	if expr == "." {
		return "." + name
	}
	return expr + "." + name
}

// TestCheckerAgreesOnEveryPath ties the static checker to execution: the
// reference template for every reachable path passes CheckTemplate and executes
// on its maximum, and the same template with its last name mutated by one
// letter is refused.
func TestCheckerAgreesOnEveryPath(t *testing.T) {
	funcs := messageFuncs()
	for path := range walkModel(t, model.Event{}).paths {
		parts := strings.Split(path, ".")
		name := parts[len(parts)-1]
		src := referenceTemplate(t, path, name)
		tmpl, err := template.New("agreement").Option("missingkey=error").Funcs(funcs).Parse(src)
		if err != nil {
			t.Fatalf("%s: %q does not parse: %v", path, src, err)
		}
		if err := configuration.CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
			t.Errorf("%s: the checker refused its own reference template: %v", path, err)
		}
		event := model.EventStarted
		if isNotOnStarted(path) {
			event = model.EventFinished
		}
		var b strings.Builder
		if err := tmpl.Execute(&b, maximal(event)); err != nil {
			t.Errorf("%s: the reference template does not execute on the %s maximum: %v", path, event, err)
		}

		mutated := referenceTemplate(t, path, name+"x")
		bad, err := template.New("agreement").Option("missingkey=error").Funcs(funcs).Parse(mutated)
		if err != nil {
			t.Fatalf("%s: the mutated template does not parse: %v", path, err)
		}
		if err := configuration.CheckTemplate(bad, reflect.TypeFor[model.Event](), funcs); err == nil {
			t.Errorf("%s: the mutated name %q was accepted", path, name+"x")
		}
	}
}
