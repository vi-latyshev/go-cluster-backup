package storagetest

// The kit's failure paths, driven with a recorder: what a broken storage makes
// the kit report is a function of its own, so it is asserted here rather than
// through a test that would have to fail to prove it.

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// recorder is the test handle the kit's failure paths are driven with: it
// records what would have failed the test instead of failing it.
type recorder struct{ fatals []string }

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}

func TestRunReportsAFailedCheck(t *testing.T) {
	r := &recorder{}
	reportCheck(r, Check{Name: "put stat get"}, errors.New("injected put failure"))
	if len(r.fatals) != 1 {
		t.Fatalf("fatals = %v, want exactly one", r.fatals)
	}
	if !strings.Contains(r.fatals[0], "put stat get") || !strings.Contains(r.fatals[0], "injected put failure") {
		t.Errorf("the report does not name the check and its error: %q", r.fatals[0])
	}
	r = &recorder{}
	reportCheck(r, Check{Name: "describe"}, nil)
	if len(r.fatals) != 0 {
		t.Errorf("a passing check reported %v", r.fatals)
	}
}

func TestRunMultipartReportsABadPartSize(t *testing.T) {
	for _, partSize := range []int64{0, -1, partSizeLimit + 1} {
		r := &recorder{}
		reportPartSize(r, partSize)
		if len(r.fatals) != 1 || !strings.Contains(r.fatals[0], "outside the kit's range") {
			t.Errorf("part size %d: fatals = %v", partSize, r.fatals)
		}
	}
	r := &recorder{}
	reportPartSize(r, 5<<20)
	if len(r.fatals) != 0 {
		t.Errorf("a part size the kit can drive reported %v", r.fatals)
	}
}

func TestRunMultipartReportsAFailedCheck(t *testing.T) {
	r := &recorder{}
	reportMultipartCheck(r, MultipartCheck{Name: "multipart: parts in order complete the object"},
		errors.New("injected part failure"))
	if len(r.fatals) != 1 || !strings.Contains(r.fatals[0], "injected part failure") {
		t.Errorf("fatals = %v", r.fatals)
	}
	r = &recorder{}
	reportMultipartCheck(r, MultipartCheck{}, nil)
	if len(r.fatals) != 0 {
		t.Errorf("a passing check reported %v", r.fatals)
	}
}

func TestGenerateReportsAnUngeneratableLocation(t *testing.T) {
	bad := &ConformanceLocation{base: "conformance-", hexDigits: 12, shape: regexp.MustCompile("^never$")}
	r := &recorder{}
	bad.generate(r)
	if len(r.fatals) != 1 || !strings.Contains(r.fatals[0], "not a conformance location") {
		t.Fatalf("fatals = %v, want the guard's report", r.fatals)
	}
	r = &recorder{}
	loc := NewConformanceLocation("conformance-", 12).generate(r)
	if loc == "" || len(r.fatals) != 0 {
		t.Errorf("generate = %q, fatals = %v", loc, r.fatals)
	}
}
