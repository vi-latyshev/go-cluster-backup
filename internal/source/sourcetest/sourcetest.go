// Package sourcetest is the conformance suite every source implementation runs:
//
//	sourcetest.Run(t, func(t *testing.T) source.Source { return newTestSource(t) })
package sourcetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

type Check struct {
	Name string
	Run  func(t *testing.T, s source.Source) error
}

var Checks = []Check{
	{"describe", checkDescribe},
	{"dump", checkDump},
	{"returns on cancellation", checkCancel},
	{"propagates a write error", checkWriterError},
	{"units of an enumerating source", checkUnits},
	{"dump stats", checkDumpStats},
}

func Run(t *testing.T, newFn func(t *testing.T) source.Source) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(t, newFn(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkDescribe(_ *testing.T, s source.Source) error {
	tr, err := s.Describe(context.Background())
	if err != nil {
		return fmt.Errorf("Describe: %w", err)
	}
	if tr.Ext != "" && !strings.HasPrefix(tr.Ext, ".") {
		return fmt.Errorf("Describe: extension %q must be empty or start with a dot", tr.Ext)
	}
	if tr.Origin == "" {
		return fmt.Errorf("Describe: Origin is empty; a restore compares it against its target")
	}
	if secret, why := originCarriesSecret(tr.Origin); secret {
		return fmt.Errorf("Describe: Origin %q carries a credential (%s)", tr.Origin, why)
	}
	return nil
}

// originCarriesSecret is deliberately blunt: an origin is an address or a path,
// and the two shapes a credential takes inside one are a URL userinfo component
// and a key=value token.
func originCarriesSecret(origin string) (bool, string) {
	lower := strings.ToLower(origin)
	if strings.Contains(lower, "password=") ||
		strings.Contains(lower, "passwd=") ||
		strings.Contains(lower, "token=") ||
		strings.Contains(lower, "secret=") {
		return true, "key=value credential"
	}
	if scheme, rest, ok := strings.Cut(lower, "://"); ok {
		if _, _, hasUserinfo := strings.Cut(rest, "@"); hasUserinfo {
			return true, scheme + " URL with userinfo"
		}
	}
	return false, ""
}

func checkDump(_ *testing.T, s source.Source) error {
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		return fmt.Errorf("Dump: %w", err)
	}
	return nil
}

func checkCancel(t *testing.T, s source.Source) error {
	return plugintest.MustReturnWithError(t, 15*time.Second, "Dump", func(ctx context.Context) error {
		return s.Dump(ctx, io.Discard)
	})
}

func checkWriterError(_ *testing.T, s source.Source) error {
	fw := &plugintest.ErrWriter{}
	err := s.Dump(context.Background(), fw)
	if fw.Wrote > 0 && err == nil {
		return fmt.Errorf("Dump wrote %d bytes into a failing writer and reported success", fw.Wrote)
	}
	return nil
}

// checkUnits is the conformance of the optional Enumerator capability: a source
// without it is one artifact and passes here, and so does one that answers
// errors.ErrUnsupported — the same "I am one artifact" in call form. What comes
// back otherwise must be units the engine can name and build from.
func checkUnits(t *testing.T, s source.Source) error {
	e, ok := s.(source.Enumerator)
	if !ok {
		return nil
	}
	units, err := e.Units(context.Background())
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("Units: %w", err)
	}
	if len(units) == 0 {
		return errors.New("Units returned no units and no error; an instance that holds nothing to dump is a fault of its own, never an empty result")
	}
	seen := map[string]bool{}
	for i, u := range units {
		switch {
		case u.Name == "":
			return fmt.Errorf("units[%d]: Name is empty", i)
		// What a name may look like is the engine's rule, not the plugin's: an
		// unusable name (a separator, a control character) is that unit's own
		// failed artifact, so a server handing one back never costs the
		// instance's other units. What the kit pins here is uniqueness — two
		// units of one result sharing a name is a plugin fault and nothing else.
		case len(u.Config) == 0 || !jsontext.Value(u.Config).IsValid():
			return fmt.Errorf("units[%d] (%s): Config is not valid JSON", i, u.Name)
		}
		if seen[u.Name] {
			return fmt.Errorf("units[%d]: name %q repeats within one result", i, u.Name)
		}
		seen[u.Name] = true
	}
	return plugintest.MustReturnWithError(t, 15*time.Second, "Units", func(ctx context.Context) error {
		_, err := e.Units(ctx)
		return err
	})
}

// checkDumpStats is the conformance of the optional DumpStatsReporter
// capability: a source without it leaves nothing out that anyone must know
// about, and one with it answers with counts a reader can rely on.
func checkDumpStats(t *testing.T, s source.Source) error {
	r, ok := s.(source.DumpStatsReporter)
	if !ok {
		return nil
	}
	st, err := r.DumpStats(context.Background())
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("DumpStats: %w", err)
	}
	if st.SkippedEntries < 0 || st.PaddedFiles < 0 {
		return fmt.Errorf("DumpStats reports a negative count: skipped %d, padded %d", st.SkippedEntries, st.PaddedFiles)
	}
	return plugintest.MustReturnWithError(t, 15*time.Second, "DumpStats", func(ctx context.Context) error {
		_, err := r.DumpStats(ctx)
		return err
	})
}
