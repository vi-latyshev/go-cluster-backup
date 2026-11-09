package plugintest_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
)

type widget interface{ Widget() string }

var widgetKind = instances.NewKind[widget]("widget")

type widgetImpl struct{}

func (w *widgetImpl) Widget() string { return "w" }
func (w *widgetImpl) Close() error   { return nil }

type brokenCloser struct{ closed int }

func (w *brokenCloser) Widget() string { return "b" }
func (w *brokenCloser) Close() error {
	w.closed++
	if w.closed > 1 {
		return errors.New("closed twice")
	}
	return nil
}

type goodConfig struct {
	Label string               `json:"label" doc:"a label"`
	Token configuration.Secret `json:"token" doc:"a secret"`
}

func registerGood(r *instances.Registry) error {
	return instances.Register(r, widgetKind, instances.FactorySpec[goodConfig, widget]{
		Type: "good", Summary: "a good widget", Stability: instances.Stable,
		Example: []byte(`{"label":"x","token":{"file":"/f"}}`),
		New: func(context.Context, goodConfig, instances.Deps) (widget, error) {
			return &widgetImpl{}, nil
		},
	})
}

func TestCheckSpecPassesGoodPlugin(t *testing.T) {
	r := instances.NewRegistry()
	if err := registerGood(r); err != nil {
		t.Fatal(err)
	}
	if issues := plugintest.Inspect(r); len(issues) != 0 {
		t.Fatalf("good plugin reported issues: %+v", issues)
	}
	plugintest.CheckSpec(t, r) // fails the test on any issue
}

// brokenOne registers one deliberately broken plugin; the returned check name
// must appear among the issues.
func TestInspectCatchesBrokenPlugins(t *testing.T) {
	cases := []struct {
		name  string
		check string
		reg   func(*instances.Registry) error
	}{
		{
			name:  "undocumented field",
			check: "fields documented",
			reg: func(r *instances.Registry) error {
				type cfg struct {
					Label string `json:"label"`
				}
				return instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
					Type: "b1", Example: []byte(`{"label":"x"}`),
					New: func(context.Context, cfg, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "secret named without the type",
			check: "secrets typed",
			reg: func(r *instances.Registry) error {
				type cfg struct {
					APIToken string `json:"api_token" doc:"a token"`
				}
				return instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
					Type: "b2", Example: []byte(`{"api_token":"literal"}`),
					New: func(context.Context, cfg, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "token_file is a location, not a value",
			check: "",
			reg: func(r *instances.Registry) error {
				type cfg struct {
					TokenFile string `json:"token_file" doc:"a path"`
				}
				return instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
					Type: "ok_token_file", Example: []byte(`{"token_file":"/var/run/token"}`),
					New: func(context.Context, cfg, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "example rejected",
			check: "example decodes",
			reg: func(r *instances.Registry) error {
				type cfg struct {
					Label string `json:"label" doc:"a label"`
				}
				// Example without the required field: register itself must reject it.
				return instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
					Type: "b3", Example: []byte(`{}`),
					New: func(context.Context, cfg, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "unknown member accepted",
			check: "unknown member rejected",
			reg: func(r *instances.Registry) error {
				// A map config accepts any member by construction.
				return instances.Register(r, widgetKind, instances.FactorySpec[map[string]jsontext.Value, widget]{
					Type: "b4", Example: []byte(`{}`),
					New: func(context.Context, map[string]jsontext.Value, instances.Deps) (widget, error) {
						return &widgetImpl{}, nil
					},
				})
			},
		},
		{
			name:  "close is not idempotent",
			check: "builds and closes",
			reg: func(r *instances.Registry) error {
				type cfg struct {
					Label string `json:"label" doc:"a label"`
				}
				return instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
					Type: "b5", Example: []byte(`{"label":"x"}`),
					New: func(context.Context, cfg, instances.Deps) (widget, error) { return &brokenCloser{}, nil },
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := instances.NewRegistry()
			err := tc.reg(r)
			if tc.check == "example decodes" {
				if err == nil {
					t.Fatal("Register accepted a broken Example")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			issues := plugintest.Inspect(r)
			if tc.check == "" {
				if len(issues) != 0 {
					t.Fatalf("false positive: %+v", issues)
				}
				return
			}
			if len(issues) == 0 {
				t.Fatal("Inspect found no issue")
			}
			// The b5 case must trip close idempotency: wrap it to close twice.
			if tc.check == "builds and closes" {
				hasClose := false
				for _, iss := range issues {
					if iss.Check == "builds and closes" && strings.Contains(iss.Err.Error(), "second Close") {
						hasClose = true
					}
				}
				if !hasClose {
					t.Fatalf("close idempotency not caught: %+v", issues)
				}
				return
			}
			found := false
			for _, iss := range issues {
				if iss.Check == tc.check {
					found = true
				}
			}
			if !found {
				t.Fatalf("issue %q not found among %+v", tc.check, issues)
			}
		})
	}
}

type plainWidget struct{}

func (plainWidget) Widget() string { return "p" }

type failingClose struct{}

func (*failingClose) Widget() string { return "c" }
func (*failingClose) Close() error   { return errors.New("close refused") }

// TestInspectReportsEveryCheck: each general check has its own failure path, and
// every one of them is reachable from a registry a plugin author can write.
func TestInspectReportsEveryCheck(t *testing.T) {
	type documented struct {
		Label string `json:"label" doc:"a label"`
	}
	type outer struct {
		Inner innerThing `json:"inner" doc:"a nested thing"`
	}
	cases := []struct {
		name  string
		check string
		reg   func(*instances.Registry) error
	}{
		{
			name:  "no example at all",
			check: "example decodes",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[documented, widget]{
					Type: "no_example",
					New:  func(context.Context, documented, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "the example is not an object",
			check: "unknown member rejected",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[[]string, widget]{
					Type: "array_example", Example: []byte(`["a"]`),
					New: func(context.Context, []string, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "a nested field without a doc",
			check: "fields documented",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[outer, widget]{
					Type: "nested_undocumented", Example: []byte(`{"inner":{"value":"v"}}`),
					New: func(context.Context, outer, instances.Deps) (widget, error) { return &widgetImpl{}, nil },
				})
			},
		},
		{
			name:  "the constructor refuses the example",
			check: "builds and closes",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[documented, widget]{
					Type: "refuses", Example: []byte(`{"label":"x"}`),
					New: func(context.Context, documented, instances.Deps) (widget, error) {
						return nil, errors.New("cannot start")
					},
				})
			},
		},
		{
			// An instance with nothing to close is not a failure: the check
			// passes, and no issue is reported at all.
			name:  "the instance is not a closer",
			check: "",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[documented, widget]{
					Type: "no_closer", Example: []byte(`{"label":"x"}`),
					New: func(context.Context, documented, instances.Deps) (widget, error) { return plainWidget{}, nil },
				})
			},
		},
		{
			name:  "the first Close fails",
			check: "builds and closes",
			reg: func(r *instances.Registry) error {
				return instances.Register(r, widgetKind, instances.FactorySpec[documented, widget]{
					Type: "bad_close", Example: []byte(`{"label":"x"}`),
					New: func(context.Context, documented, instances.Deps) (widget, error) { return &failingClose{}, nil },
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := instances.NewRegistry()
			if err := tc.reg(r); err != nil {
				t.Fatal(err)
			}
			issues := plugintest.Inspect(r)
			if tc.check == "" {
				if len(issues) != 0 {
					t.Fatalf("false positive: %+v", issues)
				}
				return
			}
			for _, iss := range issues {
				if iss.Check == tc.check {
					return
				}
			}
			t.Fatalf("check %q not reported among %+v", tc.check, issues)
		})
	}
}

type innerThing struct {
	Value string `json:"value"` // deliberately undocumented
}

// TestInspectWithUsesTheKindsOwnConstructor: the build override replaces
// r.BuildAny for the shared check, so a kind whose construction takes more than
// the config is checked by the same kit; without it the default path runs.
func TestInspectWithUsesTheKindsOwnConstructor(t *testing.T) {
	type cfg struct {
		Label string `json:"label" doc:"a label"`
	}
	r := instances.NewRegistry()
	err := instances.Register(r, widgetKind, instances.FactorySpec[cfg, widget]{
		Type: "refuses", Example: []byte(`{"label":"x"}`),
		New: func(context.Context, cfg, instances.Deps) (widget, error) {
			return nil, errors.New("cannot start")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if issues := plugintest.Inspect(r); len(issues) == 0 {
		t.Fatal("the failing constructor passed the default path")
	}
	calls := 0
	build := func(_ context.Context, _ *instances.Registry, d instances.Descriptor, _ instances.Deps) (any, error) {
		calls++
		if d.Type != "refuses" {
			t.Fatalf("descriptor = %+v", d)
		}
		return &widgetImpl{}, nil
	}
	issues := plugintest.InspectWith(r, plugintest.Options{Build: build})
	if len(issues) != 0 {
		t.Fatalf("issues with a build override: %+v", issues)
	}
	if calls != 1 {
		t.Fatalf("override calls = %d, want the one build check", calls)
	}
	plugintest.CheckSpecWith(t, r, plugintest.Options{Build: build})
}

func TestMustReturnWithin(t *testing.T) {
	err := plugintest.MustReturnWithin(t, time.Second, "fast", func(ctx context.Context) error {
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("a cancelled context must be reported to the caller")
	}
}

func TestErrWriterFailsWithinTwoWrites(t *testing.T) {
	w := &plugintest.ErrWriter{}
	var saw error
	for i := 0; i < 10; i++ {
		if _, err := w.Write([]byte("0123456789")); err != nil {
			saw = err
			break
		}
	}
	if saw == nil {
		t.Fatal("ErrWriter never failed")
	}
	if w.Wrote == 0 {
		t.Fatal("ErrWriter failed before writing anything")
	}
}

func TestErrReaderAlwaysFails(t *testing.T) {
	r := plugintest.NewErrReader()
	buf := make([]byte, 4)
	if _, err := r.Read(buf); err == nil || err == io.EOF {
		t.Fatalf("first read: %v", err)
	}
	if _, err := r.Read(buf); err == nil || err == io.EOF {
		t.Fatalf("second read: %v", err)
	}
}
