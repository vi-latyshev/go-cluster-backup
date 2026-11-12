package planning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Unit is one artifact of one instance. A source whose domain holds a single
// artifact is its own unit; a source that enumerates gets one unit per name,
// each with a source of its own built from the config the plugin handed back —
// of the same plugin type, never another. It is also what a caller hands Expand
// as the parent: the instance already built and checked.
type Unit struct {
	// Instance is the instance name the unit's source was built as.
	Instance string
	// Type is the plugin type the parent was declared with; a unit is built of
	// that same type, never another.
	Type string
	// Source is the unit's own source; nil when it could not be built or checked.
	Source source.Source
	// Artifact is the unit's envelope, with the unit's own name under it.
	Artifact model.Artifact
	// Pipeline is the processor instance names the unit's bytes flow through,
	// in encode order: the parent's pipeline, because a unit is built from the
	// same plugin type and the same routing.
	Pipeline []string
	// Warnings are the coded, non-fatal facts the credential decision found
	// about this unit's bytes.
	Warnings []model.Warning
	// Problem is this unit's own failure: a unit whose source could not be built
	// or checked fails alone, and the units beside it still run.
	Problem error
}

// SourceFactory constructs one source instance the way the run does.
type SourceFactory func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error)

// SourceCheck runs one source's own Check exactly as the run checks it.
type SourceCheck func(ctx context.Context, src source.Source, instance string) error

// CredentialCheck applies one built source's credential/sealing decision
// exactly as the run's Resolve does: a refusal fails the source, a warning is
// carried beside its row. It is the configuration's own decision, applied to
// discovered and enumerated sources the same way it is applied to declared
// ones.
type CredentialCheck func(ctx context.Context, src source.Source, artifact model.Artifact, pipeline []string) (model.Warning, error)

// Own records a source with the caller's scope, which closes it in reverse
// build order.
type Own func(io.Closer)

// Expansion are the typed collaborators unit expansion calls, projected by
// assembly.
type Expansion struct {
	// Build constructs one unit's source from the config the plugin handed back.
	Build SourceFactory
	// Check runs the built source's Check; nil skips it, as a plugin without one.
	Check SourceCheck
	// Credential applies the run's credential/sealing decision to each built
	// unit; nil skips it.
	Credential CredentialCheck
	// Own registers every source this expansion builds with the caller's scope;
	// nil means the caller keeps ownership some other way.
	Own Own
}

// Expand turns one built instance — the parent — into the artifacts it holds. A
// parent that implements no Enumerator, or answers errors.ErrUnsupported, is
// exactly one unit: itself, which is every source that is not a multi-artifact
// instance.
//
// Both names are built from the unit's name, so a plugin never spells an
// artifact name: the artifact is "<envelope>/<unit>" and the instance
// "<instance>/<unit>". A failure of Units itself is one unit — the parent's
// instance, the parent's envelope — carrying the error: a domain whose contents
// cannot be read is one failed artifact, never a discovery that fails and takes
// every other rule's results with it. An empty result is the same: a plugin that
// enumerates nothing has broken its own contract, and a run that silently
// produces no artifact for it would be worse than one failed artifact. A unit
// whose name cannot name an artifact — a server's own database name, with a
// separator or a control character in it — is that unit's own failed artifact,
// and the units beside it are still dumped.
//
// Every source this builds belongs to the caller's scope from the moment it
// exists — the caller closes them, whatever the outcome.
func (p Planner) Expand(ctx context.Context, parent Unit, e Expansion) []Unit {
	en, ok := parent.Source.(source.Enumerator)
	if !ok {
		return []Unit{parent}
	}
	units, single, err := p.ListUnits(ctx, parent.Instance, en)
	switch {
	case single:
		return []Unit{parent}
	case err != nil:
		parent.Problem = fmt.Errorf("enumerating the artifacts of %s: %w", parent.Instance, err)
		return []Unit{parent}
	case len(units) == 0:
		parent.Problem = fmt.Errorf("%s answers no artifact: an empty unit list is a plugin fault, not an empty backup", parent.Instance)
		return []Unit{parent}
	}
	out := make([]Unit, 0, len(units))
	for _, u := range units {
		unit := Unit{
			Instance: parent.Instance + "/" + u.Name,
			Type:     parent.Type,
			Artifact: parent.Artifact,
			Pipeline: parent.Pipeline,
		}
		unit.Artifact.Name = parent.Artifact.Name + "/" + u.Name
		if why := unitNameRefusal(u.Name); why != "" {
			unit.Problem = pluginerrors.Permanent(fmt.Errorf("the unit %q cannot name an artifact: %s", u.Name, why))
			out = append(out, unit)
			continue
		}
		s, err := e.Build(ctx, parent.Type, unit.Instance, u.Config)
		if err != nil {
			unit.Problem = err
			out = append(out, unit)
			continue
		}
		if c, ok := s.(io.Closer); ok && e.Own != nil {
			e.Own(c)
		}
		// The credential decision is the run's own, applied before anything
		// operational touches the unit: a refusal is this unit's failure, a
		// warning rides beside it.
		if e.Credential != nil {
			w, err := e.Credential(ctx, s, unit.Artifact, unit.Pipeline)
			switch {
			case err != nil:
				unit.Problem = err
			case w.Code != "":
				unit.Warnings = []model.Warning{w}
			}
		}
		if e.Check != nil && unit.Problem == nil {
			if err := e.Check(ctx, s, unit.Instance); err != nil {
				unit.Problem = err
			} else {
				unit.Source = s
			}
		} else if unit.Problem == nil {
			unit.Source = s
		}
		out = append(out, unit)
	}
	return out
}

// ListUnits asks a source for its units under the planner's policy. The bool
// reports the answer "this instance is one artifact" — errors.ErrUnsupported,
// which is a decision and not a fault: it costs neither the retry budget nor the
// run's deadline, whatever the policy says an unclassified error means.
func (p Planner) ListUnits(ctx context.Context, name string, en source.Enumerator) ([]source.Unit, bool, error) {
	units, err := execution.Attempt(ctx, p.policy, "units "+name, func(ctx context.Context, _ int) ([]source.Unit, error) {
		units, err := execution.SafeValue(func() ([]source.Unit, error) { return en.Units(ctx) })
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, pluginerrors.Permanent(err)
		}
		return units, err
	})
	if errors.Is(err, errors.ErrUnsupported) {
		return nil, true, nil
	}
	return units, false, err
}

// unitNameRefusal reports why a unit's name cannot name an artifact — both the
// artifact and the instance name are built from it, so a separator would make
// one unit read as two and a control character would corrupt the report — and
// "" when it can. The rule lives here, not in the plugin: a plugin reports what
// its source holds, and a name a server hands back is not the plugin's to
// invent, so a name the planner cannot use costs that unit alone.
func unitNameRefusal(name string) string {
	switch {
	case name == "":
		return "it is empty"
	case strings.ContainsRune(name, '/'):
		return "it carries a path separator"
	case strings.ContainsFunc(name, unicode.IsControl):
		return "it carries a control character"
	}
	return ""
}
