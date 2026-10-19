// Package plugintest is the general conformance kit: it checks every registered
// plugin type against the contract every plugin shares, whatever its kind. The
// per-kind suites live next to their kinds (storagetest, sourcetest, ...).
package plugintest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Issue is one failed general check of one plugin.
type Issue struct {
	Plugin string
	Check  string
	Err    error
}

// CheckSpec fails the test with every issue the general contract checks found:
// a complete example that decodes, unknown members rejected, every field documented,
// secret-named fields typed plugin.Secret, a constructor that succeeds, and an
// idempotent Close.
//
// The secret rule: a field whose JSON name mentions a token, secret, password or
// credential — unless it ends in _file or _path, a location rather than a value —
// must be typed plugin.Secret. One direction only: any other field may be Secret too.
func CheckSpec(t *testing.T, r *plugin.Registry) {
	t.Helper()
	checkSpec(t, r)
}

// errorReporter is what the reporting check needs from a test handle: *testing.T
// satisfies it, and the check's own test uses a recorder so the failure path can
// be asserted instead of ending the test — a live T cannot survive asserting its
// own failure, the same reason fatalReporter exists below.
type errorReporter interface {
	Helper()
	Errorf(format string, args ...any)
}

func checkSpec(t errorReporter, r *plugin.Registry) {
	t.Helper()
	for _, iss := range Inspect(r) {
		t.Errorf("%s: %s: %v", iss.Plugin, iss.Check, iss.Err)
	}
}

// Inspect runs the general checks and returns every failure; an empty slice is a
// pass. It exists so the checks themselves can be tested against broken plugins.
func Inspect(r *plugin.Registry) []Issue {
	var issues []Issue
	for _, d := range r.Descriptors() {
		for _, c := range checks {
			if err := c.fn(r, d); err != nil {
				issues = append(issues, Issue{Plugin: d.Kind + "/" + d.Type, Check: c.name, Err: err})
			}
		}
	}
	return issues
}

type specCheck struct {
	name string
	fn   func(r *plugin.Registry, d plugin.Descriptor) error
}

var checks = []specCheck{
	{"example decodes", func(r *plugin.Registry, d plugin.Descriptor) error {
		if err := r.Check(d.Kind, d.Type, d.Example); err != nil {
			return fmt.Errorf("the example does not decode: %w", err)
		}
		return nil
	}},
	{"unknown member rejected", func(r *plugin.Registry, d plugin.Descriptor) error {
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(d.Example, &obj); err != nil {
			return fmt.Errorf("the example is not a JSON object: %w", err)
		}
		obj["zzz_unknown_member"] = jsontext.Value("1")
		// The example decoded from valid JSON a line above, so re-encoding it
		// with one more member cannot fail.
		mutated := must.Do(json.Marshal(obj))
		err := r.Check(d.Kind, d.Type, mutated)
		got := "no error"
		if err != nil {
			got = err.Error()
		}
		if !strings.Contains(got, "zzz_unknown_member") {
			return fmt.Errorf("an unknown member must be rejected by name; the check returned %s", got)
		}
		return nil
	}},
	{"fields documented", func(_ *plugin.Registry, d plugin.Descriptor) error {
		return walkFields(d.Fields, func(f plugin.Field) error {
			if f.Doc == "" {
				return fmt.Errorf("field %q has no doc", f.Name)
			}
			return nil
		})
	}},
	{"secrets typed", func(_ *plugin.Registry, d plugin.Descriptor) error {
		return walkFields(d.Fields, func(f plugin.Field) error {
			if secretNamed(f.Name) && !f.Secret {
				return fmt.Errorf("field %q names a secret but is %s, not plugin.Secret", f.Name, f.Type)
			}
			return nil
		})
	}},
	{"builds and closes", func(r *plugin.Registry, d plugin.Descriptor) error {
		v, err := r.BuildAny(context.Background(), d.Kind, d.Type, d.Example,
			plugin.Deps{Instance: d.Type, Log: slog.New(slog.DiscardHandler)})
		if err != nil {
			return fmt.Errorf("constructing from the example: %w", err)
		}
		closer, ok := v.(io.Closer)
		if !ok {
			return nil
		}
		if err := closer.Close(); err != nil {
			return fmt.Errorf("first Close: %w", err)
		}
		if err := closer.Close(); err != nil {
			return fmt.Errorf("second Close (must be idempotent): %w", err)
		}
		return nil
	}},
}

func walkFields(fs []plugin.Field, fn func(plugin.Field) error) error {
	for _, f := range fs {
		if err := fn(f); err != nil {
			return err
		}
		if err := walkFields(f.Fields, fn); err != nil {
			return err
		}
	}
	return nil
}

var secretNameRe = regexp.MustCompile(`(?i)(tokens?|secrets?|passwords?|passwds?|credentials?)`)

func secretNamed(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, "_file") || strings.HasSuffix(lower, "_path") {
		return false
	}
	return secretNameRe.MatchString(lower)
}

// fatalReporter is what the cancellation helpers need from a test handle:
// *testing.T satisfies it, and the helpers' own tests use a recorder — a fake
// testing.TB is impossible (the interface carries an unexported method), and a
// live T cannot survive asserting its own failure path.
type fatalReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// MustReturnWithin runs fn with an already-cancelled context and fails the test
// when fn does not return within the deadline. It returns fn's error, which the
// caller must require to be non-nil for a blocking operation.
func MustReturnWithin(t *testing.T, deadline time.Duration, what string, fn func(ctx context.Context) error) error {
	return mustReturnWithin(t, deadline, what, fn)
}

func mustReturnWithin(t fatalReporter, deadline time.Duration, what string, fn func(ctx context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(deadline):
		t.Fatalf("%s did not return on a cancelled context within %s", what, deadline)
		return nil
	}
}

// MustReturnWithError is MustReturnWithin plus the contract every blocking
// operation owes a cancelled context: it must return an error, not success.
// what names the operation in both the timeout and the failure.
func MustReturnWithError(t *testing.T, deadline time.Duration, what string, fn func(ctx context.Context) error) error {
	return mustReturnWithError(t, deadline, what, fn)
}

func mustReturnWithError(t fatalReporter, deadline time.Duration, what string, fn func(ctx context.Context) error) error {
	t.Helper()
	err := mustReturnWithin(t, deadline, what, fn)
	if err == nil {
		return fmt.Errorf("%s with a cancelled context returned success", what)
	}
	return nil
}

// NewErrReader returns an io.Reader that fails on every read, for exercising a
// plugin's error path through a broken upstream.
func NewErrReader() io.Reader { return errReader{} }

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("plugintest: injected reader failure")
}

// ErrWriter is an io.Writer that accepts a few bytes and then fails, for
// exercising a plugin's error path through a broken downstream. It fails within
// the first two Write calls whatever the caller's chunk size is.
type ErrWriter struct{ Wrote int }

const errWriterLimit = 4

func (w *ErrWriter) Write(p []byte) (int, error) {
	if w.Wrote >= errWriterLimit {
		return 0, errors.New("plugintest: injected writer failure")
	}
	n := min(errWriterLimit-w.Wrote, len(p))
	w.Wrote += n
	if w.Wrote >= errWriterLimit {
		return n, errors.New("plugintest: injected writer failure")
	}
	return n, nil
}
