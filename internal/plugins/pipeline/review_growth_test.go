package pipeline

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
)

// TestR2_M15_CreateFailureClosesGrowth is the review's counterexample as written:
// a producer that dies before its spool file exists must still end the first
// attempt's Growth, or multipart followers wait for a change that cannot come.
func TestR2_M15_CreateFailureClosesGrowth(t *testing.T) {
	p := New(testOptions())
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	g := NewGrowth()
	_, err = produced(t, p, sp, testPlan(&fakeSource{data: []byte("z")}), g)
	if err == nil {
		t.Fatal("expected spool create failure")
	}
	if _, _, done, _, cause := g.Snapshot(); !done || cause == nil {
		t.Fatalf("spool failure left followers waiting: done=%v cause=%v", done, cause)
	}
}

// sequencedSource fails each Dump with its own error, so a test can tell the
// first attempt's cause from the last one's.
type sequencedSource struct {
	errs  []error
	dumps int
}

func (s *sequencedSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat", Origin: "test://sequenced"}, nil
}

func (s *sequencedSource) Dump(context.Context, io.Writer) error {
	s.dumps++
	if s.dumps <= len(s.errs) {
		return s.errs[s.dumps-1]
	}
	return nil
}

// TestR2_M15_TerminalExitMatrix covers the finding's sweep: an exit after the
// file exists, and a retry loop that ends on a later attempt, both publish a
// terminal state — and the cause the first attempt already published stays.
func TestR2_M15_TerminalExitMatrix(t *testing.T) {
	t.Run("after the file exists", func(t *testing.T) {
		p := New(testOptions())
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		g := NewGrowth()
		src := &fakeSource{data: []byte("bytes"), dumpErr: pluginerrors.Permanent(errors.New("dump refused"))}
		_, err = produced(t, p, sp, testPlan(src), g)
		if err == nil || !strings.Contains(err.Error(), "dump refused") {
			t.Fatalf("err = %v, want the dump failure", err)
		}
		if _, _, done, _, cause := g.Snapshot(); !done || cause == nil || !strings.Contains(cause.Error(), "dump refused") {
			t.Fatalf("growth = done=%v cause=%v, want the dump failure", done, cause)
		}
	})

	t.Run("keeps the first attempt cause", func(t *testing.T) {
		p := New(testOptions())
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		g := NewGrowth()
		src := &sequencedSource{errs: []error{
			errors.New("first cause"),
			pluginerrors.Permanent(errors.New("final cause")),
		}}
		_, err = produced(t, p, sp, testPlan(src), g)
		if err == nil || !strings.Contains(err.Error(), "final cause") {
			t.Fatalf("err = %v, want the retry loop's last failure", err)
		}
		if src.dumps != 2 {
			t.Fatalf("dumps = %d, want the first attempt and its retry", src.dumps)
		}
		_, _, done, _, cause := g.Snapshot()
		if !done || cause == nil || !strings.Contains(cause.Error(), "first cause") {
			t.Fatalf("growth = done=%v cause=%v, want the first attempt's cause preserved", done, cause)
		}
		if strings.Contains(cause.Error(), "final cause") {
			t.Fatalf("growth cause %q was replaced by a later attempt's", cause)
		}
	})
}
