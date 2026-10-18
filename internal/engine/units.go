package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// expandedUnit is one artifact of one instance. A source whose domain holds a
// single artifact is its own unit; a source that enumerates gets one unit per
// name, each with a source of its own built from the config the plugin handed
// back — of the same plugin type, never another. It is also what a caller hands
// expandUnits as the parent: the instance already built and checked.
type expandedUnit struct {
	instance string
	srcType  string
	src      source.Source
	artifact model.Artifact
	// problem is this unit's own failure: a unit whose source could not be
	// built or checked fails alone, and the units beside it still run.
	problem error
}

// expandUnits turns one built instance — the parent — into the artifacts it
// holds. A parent that implements no Enumerator, or answers
// errors.ErrUnsupported, is exactly one unit: itself, which is every source that
// is not a multi-artifact instance.
//
// The engine builds both names from the unit's name, so a plugin never spells an
// artifact name: the artifact is "<envelope>/<unit>" and the instance
// "<instance>/<unit>". A failure of Units itself is one unit — the parent's
// instance, the parent's envelope — carrying the error: a domain whose contents
// cannot be read is one failed artifact, never a discovery that fails and takes
// every other rule's results with it. An empty result is the same: a plugin that
// enumerates nothing has broken its own contract, and a run that silently
// produces no artifact for it would be worse than one failed artifact. A unit
// whose name the engine cannot build a name from — a server's own database name,
// with a separator or a control character in it — is that unit's own failed
// artifact, and the units beside it are still dumped.
//
// Every source this builds belongs to the caller's scope from the moment it
// exists — the caller closes them, whatever the outcome.
func (e *Engine) expandUnits(ctx context.Context, p retryPolicy, parent expandedUnit, scope *closerScope) []expandedUnit {
	en, ok := parent.src.(source.Enumerator)
	if !ok {
		return []expandedUnit{parent}
	}
	units, single, err := listUnits(ctx, p, parent.instance, en)
	switch {
	case single:
		return []expandedUnit{parent}
	case err != nil:
		parent.problem = fmt.Errorf("enumerating the artifacts of %s: %w", parent.instance, err)
		return []expandedUnit{parent}
	case len(units) == 0:
		parent.problem = fmt.Errorf("%s answers no artifact: an empty unit list is a plugin fault, not an empty backup", parent.instance)
		return []expandedUnit{parent}
	}
	out := make([]expandedUnit, 0, len(units))
	for _, u := range units {
		unit := expandedUnit{
			instance: parent.instance + "/" + u.Name,
			srcType:  parent.srcType,
			artifact: parent.artifact,
		}
		unit.artifact.Name = parent.artifact.Name + "/" + u.Name
		if why := unitNameRefusal(u.Name); why != "" {
			unit.problem = plugin.Permanent(fmt.Errorf("the unit %q cannot name an artifact: %s", u.Name, why))
			out = append(out, unit)
			continue
		}
		s, err := plugin.Build(ctx, e.reg, source.Kind, parent.srcType, u.Config, e.deps("source", parent.srcType, unit.instance))
		if err != nil {
			unit.problem = err
			out = append(out, unit)
			continue
		}
		scope.track(s)
		if err := e.checkSource(ctx, s, unit.instance); err != nil {
			unit.problem = err
		} else {
			unit.src = s
		}
		out = append(out, unit)
	}
	return out
}

// listUnits asks a source for its units under a retry policy. The bool reports
// the answer "this instance is one artifact" — errors.ErrUnsupported, which is a
// decision and not a fault: it costs neither the retry budget nor the run's
// deadline, whatever the policy says an unclassified error means.
func listUnits(ctx context.Context, p retryPolicy, name string, en source.Enumerator) ([]source.Unit, bool, error) {
	units, err := attempt(ctx, p, "units "+name, func(ctx context.Context, _ int) ([]source.Unit, error) {
		units, err := safeValue(func() ([]source.Unit, error) { return en.Units(ctx) })
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, plugin.Permanent(err)
		}
		return units, err
	})
	if errors.Is(err, errors.ErrUnsupported) {
		return nil, true, nil
	}
	return units, false, err
}

// unitNameRefusal reports why a unit's name cannot name an artifact — the engine
// builds both the artifact and the instance name from it, so a separator would
// make one unit read as two and a control character would corrupt the report —
// and "" when it can. The rule lives here, not in the plugin: a plugin reports
// what its source holds, and a name a server hands back is not the plugin's to
// invent, so a name the engine cannot use costs that unit alone.
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
