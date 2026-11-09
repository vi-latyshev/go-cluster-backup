package inventory

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// manifestMembers are the top-level members the current writer always emits: a
// document missing one is incomplete, never a run without a manifest.
var manifestMembers = [...]string{
	"schema", "run_id", "kind", "started_at", "duration", "outcome",
	"artifacts", "storages", "excluded", "previous", "exit_code",
}

// decodeManifest is the one reader of the current manifest format. The whole
// document is walked recursively: a member the writer always emits — a nested
// artifact, chain step, source, storage, exclusion or previous-run field
// included — must be present, unknown members are refused, and a schema, run
// id, kind, outcome or start this build cannot accept is refused too. A refused
// manifest is a problem run — never "this run has no manifest", and never
// protection credit.
func decodeManifest(body []byte, runID string, kind model.RunKind) (*model.RunReport, error) {
	raw := jsontext.Value(body)
	if _, ok := configuration.ObjectOf(raw); !ok {
		return nil, errors.New("parsing the manifest: not a JSON object")
	}
	if err := errors.Join(manifestShape(raw, reflect.TypeFor[model.RunReport](), "")...); err != nil {
		return nil, fmt.Errorf("parsing the manifest: %w", err)
	}
	var rep model.RunReport
	if err := json.Unmarshal(body, &rep, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("parsing the manifest: %w", err)
	}
	switch {
	case rep.Schema != model.ManifestSchema:
		return nil, fmt.Errorf("manifest schema %d is not %d, which this build reads", rep.Schema, model.ManifestSchema)
	case rep.RunID != runID:
		return nil, fmt.Errorf("the manifest says run %s but the run id says %s", rep.RunID, runID)
	case rep.Kind != kind:
		return nil, fmt.Errorf("the manifest says kind %s but the run id says %s", rep.Kind, kind)
	case rep.Outcome != model.OutcomeDelivered && rep.Outcome != model.OutcomeDegraded && rep.Outcome != model.OutcomeFailed:
		return nil, fmt.Errorf("the manifest says outcome %q, which this build does not know", rep.Outcome)
	case rep.StartedAt.IsZero():
		return nil, errors.New("the manifest has no started_at")
	}
	if err := errors.Join(manifestSemantics(&rep)...); err != nil {
		return nil, fmt.Errorf("the manifest is incomplete: %w", err)
	}
	return &rep, nil
}

// manifestShape walks one raw manifest value against the model type: every
// member the current writer always emits — the struct field without omitempty,
// recursively — must be present, and a null where a value belongs is refused. A
// list the writer emits as null is empty, not missing.
func manifestShape(raw jsontext.Value, t reflect.Type, path string) []error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if configuration.IsNull(raw) {
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
			return nil
		}
		return []error{fmt.Errorf("%s is null", path)}
	}
	switch t.Kind() {
	case reflect.Struct:
		obj, ok := configuration.ObjectOf(raw)
		if !ok {
			return nil // the strict decode reports the wrong shape
		}
		var errs []error
		for i := range t.NumField() {
			f := t.Field(i)
			name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "" || name == "-" {
				continue
			}
			val, present := obj[name]
			field := path + "/" + name
			if !present {
				if strings.Contains(opts, "omitempty") {
					continue
				}
				errs = append(errs, fmt.Errorf("missing field %s", field))
				continue
			}
			errs = append(errs, manifestShape(val, f.Type, field)...)
		}
		return errs
	case reflect.Slice:
		var items []jsontext.Value
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil
		}
		var errs []error
		for i, item := range items {
			errs = append(errs, manifestShape(item, t.Elem(), fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	default:
		return nil
	}
}

// manifestSemantics refuses the contradictions a well-formed document can still
// carry: an artifact outcome this build does not know, an ok artifact without
// the object key and canonical sha256 a restore needs, a chain step that cannot
// be replayed, a storage outcome or exclusion kind outside its closed set.
func manifestSemantics(rep *model.RunReport) []error {
	var errs []error
	for i, ar := range rep.Artifacts {
		at := fmt.Sprintf("/artifacts/%d", i)
		switch ar.Outcome {
		case model.ArtifactOK:
			if ar.Key == "" || !canonicalSHA256(ar.SHA256) {
				errs = append(errs, fmt.Errorf("%s: an ok artifact must record its object key and a canonical sha256", at))
			}
		case model.ArtifactFailed:
		default:
			errs = append(errs, fmt.Errorf("%s: outcome %q is not %q or %q", at, ar.Outcome, model.ArtifactOK, model.ArtifactFailed))
		}
		for j, step := range ar.Chain {
			if step.Instance == "" || step.Type == "" {
				errs = append(errs, fmt.Errorf("%s/chain/%d: a recorded step must name its instance and type", at, j))
			}
		}
	}
	for i, s := range rep.Storages {
		switch s.Outcome {
		case model.StorageVerified, model.StorageFailed, model.StorageSkipped:
		default:
			errs = append(errs, fmt.Errorf("/storages/%d: outcome %q is not one this build knows", i, s.Outcome))
		}
	}
	for i, ex := range rep.Excluded {
		switch ex.Kind {
		case model.ExclusionIgnored, model.ExclusionCovered:
		default:
			errs = append(errs, fmt.Errorf("/excluded/%d: kind %q is not one this build knows", i, ex.Kind))
		}
	}
	return errs
}

// canonicalSHA256 reports whether s is the digest form this build writes and a
// restore recomputes: 64 lowercase hexadecimal characters. Any other string can
// never match a computed digest, so an ok artifact carrying one is not a copy a
// restore can use.
func canonicalSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
