package sourcetest

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

type goodSource struct{}

func (goodSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "test://good/source"}, nil
}

func (goodSource) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := dst.Write([]byte("some artifact bytes"))
	return err
}

type badExtSource struct{ goodSource }

func (badExtSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: "bin", Origin: "test://bad/ext"}, nil
}

type noOriginSource struct{ goodSource }

func (noOriginSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin"}, nil
}

type secretOriginSource struct{ goodSource }

func (secretOriginSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "postgres://user:secret@host:5432/db"}, nil
}

type tokenOriginSource struct{ goodSource }

func (tokenOriginSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "postgres://host/db?token=abc"}, nil
}

type swallowWriteErrorSource struct{ goodSource }

func (swallowWriteErrorSource) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, _ = dst.Write([]byte("swallowing the failure"))
	return nil
}

type nilOnCancelSource struct{ goodSource }

func (nilOnCancelSource) Dump(ctx context.Context, dst io.Writer) error {
	if ctx.Err() != nil {
		return nil
	}
	_, err := dst.Write([]byte("x"))
	return err
}

// unitsSource carries the optional Enumerator capability; units and err make it
// break one rule of that capability at a time.
type unitsSource struct {
	goodSource
	units []source.Unit
	err   error
}

func (u unitsSource) Units(ctx context.Context) ([]source.Unit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return u.units, u.err
}

// statsSource carries the optional DumpStatsReporter capability.
type statsSource struct {
	goodSource
	stats model.DumpStats
	err   error
}

func (s statsSource) DumpStats(ctx context.Context) (model.DumpStats, error) {
	if err := ctx.Err(); err != nil {
		return model.DumpStats{}, err
	}
	return s.stats, s.err
}

// brokenSource carries a plain error through every call a check drives, so each
// check's own error branch is exercised rather than the fault being reported by
// the source's implementation.
type brokenSource struct{ err error }

func (s brokenSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, s.err
}

func (s brokenSource) Dump(context.Context, io.Writer) error { return s.err }

func TestSuitePassesGoodSource(t *testing.T) {
	Run(t, func(t *testing.T) source.Source { return goodSource{} })
}

func TestChecksCatchBrokenSources(t *testing.T) {
	unit := source.Unit{Name: "db1", Config: jsontext.Value(`{"database":"db1"}`)}
	cases := []struct {
		check string
		src   source.Source
	}{
		{"describe", badExtSource{}},
		{"describe", noOriginSource{}},
		{"describe", secretOriginSource{}},
		{"describe", tokenOriginSource{}},
		{"describe", brokenSource{err: errors.New("the source is gone")}},
		{"dump", brokenSource{err: errors.New("the dump failed")}},
		{"propagates a write error", swallowWriteErrorSource{}},
		{"returns on cancellation", nilOnCancelSource{}},
		// An enumerating source that holds nothing to dump is a fault of its
		// own, never an empty result. A name the engine cannot build an artifact
		// from is the engine's rule, not this kit's: it is checked there, where
		// it costs one unit and not the instance.
		{"units of an enumerating source", unitsSource{}},
		{"units of an enumerating source", unitsSource{err: errors.New("the enumeration failed")}},
		{"units of an enumerating source", unitsSource{units: []source.Unit{unit, unit}}},
		{"units of an enumerating source", unitsSource{units: []source.Unit{{Config: jsontext.Value(`{"database":"db1"}`)}}}},
		{"units of an enumerating source", unitsSource{units: []source.Unit{{Name: "db1", Config: jsontext.Value(`{no`)}}}},
		// A count a reader cannot rely on is worse than no count at all.
		{"dump stats", statsSource{stats: model.DumpStats{SkippedEntries: -1}}},
		{"dump stats", statsSource{stats: model.DumpStats{PaddedFiles: -1}}},
		{"dump stats", statsSource{err: errors.New("the report failed")}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.src); err == nil {
					t.Fatalf("check %q passed a broken source", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

func TestGoodChecksFindNothingOnAGoodSource(t *testing.T) {
	unit := source.Unit{Name: "db1", Config: jsontext.Value(`{"database":"db1"}`)}
	good := []source.Source{
		goodSource{},
		// The two sanctioned answers of an optional capability: units the
		// engine can name, and errors.ErrUnsupported next to a well-formed one.
		// A name the engine refuses still leaves this suite green — the engine
		// is what decides what an artifact can be called.
		unitsSource{units: []source.Unit{unit}},
		unitsSource{units: []source.Unit{{Name: "a/b", Config: jsontext.Value(`{}`)}}},
		unitsSource{err: errors.ErrUnsupported},
		statsSource{stats: model.DumpStats{SkippedEntries: 2, PaddedFiles: 1}},
		statsSource{},
		// The other sanctioned answer of the optional capability: a reporter
		// that leaves nothing out that anyone must know about.
		statsSource{err: errors.ErrUnsupported},
	}
	for _, c := range Checks {
		for _, src := range good {
			if err := c.Run(t, src); err != nil {
				t.Errorf("check %q failed a source that keeps its contract: %v", c.Name, err)
			}
		}
	}
}

// TestRunFailsABrokenSource is Run's own contract: it must fail the subtest for
// a source that breaks a check. Run reports through t.Fatal, which cannot be
// intercepted in-process, so the failure is observed from a child run of this
// test binary.
func TestRunFailsABrokenSource(t *testing.T) {
	if os.Getenv("SOURCETEST_BROKEN_CHILD") == "1" {
		Run(t, func(*testing.T) source.Source { return noOriginSource{} })
		return
	}
	args := []string{"-test.run=^TestRunFailsABrokenSource$"}
	// A binary built without coverage rejects -test.coverprofile, so the flag
	// and the fold-back exist only when this run was built with coverage. The
	// directory is not a t.TempDir: the framework removes those at test end,
	// before TestMain (testmain_test.go) can read the profile.
	if testing.CoverMode() != "" {
		coverDir, err := os.MkdirTemp("", "sourcetest-cover-")
		if err != nil {
			t.Fatal(err)
		}
		childCover := filepath.Join(coverDir, "child.cover")
		childCoverPath = childCover
		args = append(args, "-test.coverprofile="+childCover)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SOURCETEST_BROKEN_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Run accepted a source that breaks a check:\n%s", out)
	}
	if !strings.Contains(string(out), "Origin is empty") {
		t.Fatalf("Run failed without the check's own reason:\n%s", out)
	}
}
