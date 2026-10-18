package catalogtest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// memory is a conforming catalog: the suite's own proof that its checks pass.
type memory struct {
	mu   sync.Mutex
	runs map[string]model.RunReport
	mode string
}

func newMemory() *memory { return &memory{runs: map[string]model.RunReport{}} }

func (m *memory) Save(ctx context.Context, run model.RunReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.RunID] = run
	return nil
}

func (m *memory) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	runs := make([]model.RunReport, 0, len(m.runs))
	for _, r := range m.runs {
		runs = append(runs, r)
	}
	sortNewestFirst(runs)
	if req.Limit > 0 && len(runs) > req.Limit {
		runs = runs[:req.Limit]
	}
	if m.mode == "oldest-first" {
		for i, j := 0, len(runs)-1; i < j; i, j = i+1, j-1 {
			runs[i], runs[j] = runs[j], runs[i]
		}
	}
	return runs, nil
}

func (m *memory) Forget(ctx context.Context, runIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range runIDs {
		delete(m.runs, id)
	}
	return nil
}

func sortNewestFirst(runs []model.RunReport) {
	for i := 1; i < len(runs); i++ {
		for j := i; j > 0 && runs[j].StartedAt.After(runs[j-1].StartedAt); j-- {
			runs[j], runs[j-1] = runs[j-1], runs[j]
		}
	}
}

type ctxIgnoring struct{ *memory }

func (m ctxIgnoring) Save(ctx context.Context, run model.RunReport) error {
	if err := ctx.Err(); err != nil {
		return nil // lies about success
	}
	return m.memory.Save(ctx, run)
}

type duplicate struct{ *memory }

func (m duplicate) Save(ctx context.Context, run model.RunReport) error {
	m.mu.Lock()
	key := run.RunID
	if _, exists := m.runs[key]; exists {
		key = key + "/" + string(run.Outcome)
	}
	m.mu.Unlock()
	run.RunID = key
	return m.memory.Save(ctx, run)
}

func TestMemoryPassesTheSuite(t *testing.T) {
	Run(t, func(*testing.T) catalog.Catalog { return newMemory() })
}

func TestChecksCatchBrokenCatalogs(t *testing.T) {
	cases := []struct {
		check string
		cat   catalog.Catalog
	}{
		{"save upserts by run id", duplicate{newMemory()}},
		{"load is newest first and limited", func() catalog.Catalog {
			m := newMemory()
			m.mode = "oldest-first"
			return m
		}()},
		{"returns on cancellation", ctxIgnoring{newMemory()}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.cat); err == nil {
					t.Fatalf("check %q passed a broken catalog", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

type failingLoad struct{ *memory }

func (f *failingLoad) Load(context.Context, catalog.LoadRequest) ([]model.RunReport, error) {
	return nil, errors.New("load failed")
}

type failingSave struct {
	*memory
	calls int
}

func (f *failingSave) Save(ctx context.Context, run model.RunReport) error {
	f.calls++
	if f.calls > 1 {
		return errors.New("save failed")
	}
	return f.memory.Save(ctx, run)
}

// A catalog whose second Save fails trips the save-and-load error branch, not
// only the happy path.
func TestChecksCatchFailingOperations(t *testing.T) {
	for _, c := range Checks {
		switch c.Name {
		case "save and load":
			if err := c.Run(t, &failingLoad{memory: newMemory()}); err == nil {
				t.Fatal("a failing load passed")
			}
		case "save upserts by run id":
			if err := c.Run(t, &failingSave{memory: newMemory()}); err == nil {
				t.Fatal("a failing second save passed")
			}
		}
	}
}

// brokenSave fails every Save, whichever check drives it.
type brokenSave struct{ *memory }

func (b brokenSave) Save(context.Context, model.RunReport) error { return errors.New("save failed") }

// brokenLoad fails the Nth Load, so a check that loads twice is shown reporting
// its second failure as well as its first.
type brokenLoad struct {
	*memory
	failOn int
	loads  int
}

func (b *brokenLoad) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	b.loads++
	if b.loads == b.failOn {
		return nil, errors.New("load failed")
	}
	return b.memory.Load(ctx, req)
}

// brokenForget fails every Forget.
type brokenForget struct{ *memory }

func (b brokenForget) Forget(context.Context, []string) error { return errors.New("forget failed") }

// stuckForget removes nothing, so the check sees the run it asked to forget.
type stuckForget struct{ *memory }

func (s stuckForget) Forget(context.Context, []string) error { return nil }

// blindLoad answers every Load with nothing, so a check sees an empty history.
type blindLoad struct{ *memory }

func (b blindLoad) Load(context.Context, catalog.LoadRequest) ([]model.RunReport, error) {
	return nil, nil
}

// noUpsert ignores a second Save of the same run id, so the history keeps the
// stale outcome of the first.
type noUpsert struct{ *memory }

func (n noUpsert) Save(ctx context.Context, run model.RunReport) error {
	n.mu.Lock()
	_, exists := n.runs[run.RunID]
	n.mu.Unlock()
	if exists {
		return nil
	}
	return n.memory.Save(ctx, run)
}

// unorderedFullLoad answers a limited Load correctly and an unlimited one oldest
// first, so only the check's second comparison is exercised.
type unorderedFullLoad struct{ *memory }

func (u unorderedFullLoad) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	runs, err := u.memory.Load(ctx, req)
	if err != nil || req.Limit > 0 {
		return runs, err
	}
	slices.Reverse(runs)
	return runs, nil
}

// TestChecksReportEveryBrokenOperation: every error branch a check carries is
// reached by a catalog that breaks in exactly that way, so a broken catalog is
// reported rather than passing silently.
func TestChecksReportEveryBrokenOperation(t *testing.T) {
	cases := []struct {
		check string
		cat   catalog.Catalog
	}{
		{"save and load", brokenSave{newMemory()}},
		{"save and load", &failingLoad{memory: newMemory()}},
		{"save and load", blindLoad{newMemory()}},
		{"save upserts by run id", brokenSave{newMemory()}},
		{"save upserts by run id", &failingLoad{memory: newMemory()}},
		{"save upserts by run id", noUpsert{newMemory()}},
		{"load is newest first and limited", brokenSave{newMemory()}},
		{"load is newest first and limited", &failingLoad{memory: newMemory()}},
		{"load is newest first and limited", &brokenLoad{memory: newMemory(), failOn: 2}},
		{"load is newest first and limited", unorderedFullLoad{newMemory()}},
		{"forget removes and tolerates unknown ids", brokenSave{newMemory()}},
		{"forget removes and tolerates unknown ids", brokenForget{newMemory()}},
		{"forget removes and tolerates unknown ids", &failingLoad{memory: newMemory()}},
		{"forget removes and tolerates unknown ids", stuckForget{newMemory()}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.cat); err == nil {
					t.Fatalf("check %q passed a broken catalog", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// brokenEnv makes the child of TestRunFailsTheSuiteOnABrokenCatalog run the
// suite instead of spawning another child.
const brokenEnv = "CATALOGTEST_BROKEN"

// TestRunFailsTheSuiteOnABrokenCatalog: Run is what every catalog's test calls,
// and a check that reports a problem must fail the test — a conformance suite
// that swallowed one would pass a catalog that does not conform. The failing
// test is the child process's, the way a conformance suite's failure is never
// this package's own.
func TestRunFailsTheSuiteOnABrokenCatalog(t *testing.T) {
	if os.Getenv(brokenEnv) == "1" {
		Run(t, func(*testing.T) catalog.Catalog { return duplicate{newMemory()} })
		return
	}
	args := []string{"-test.run=TestRunFailsTheSuiteOnABrokenCatalog"}
	// The child is this same coverage-instrumented binary, and go test gave this
	// one a directory to report what it ran into: without it the child's own run
	// would be measured nowhere.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), brokenEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the conformance suite passed a catalog that does not upsert:\n%s", out)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("the child did not run to its own end: %v", err)
	}
	if !strings.Contains(string(out), "saving the same run id twice produced") {
		t.Fatalf("the child's failure does not carry the check's message:\n%s", out)
	}
}
