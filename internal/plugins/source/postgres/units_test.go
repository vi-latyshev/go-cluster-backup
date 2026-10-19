package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// unitsConfigPath is the enumeration config pointed at a password file the
// caller names — a path that need not exist, for the cases about reading it.
func unitsConfigPath(t *testing.T, addr, passwordPath string, exclude ...string) string {
	t.Helper()
	if exclude == nil {
		exclude = []string{}
	}
	raw, err := json.Marshal(map[string]any{
		"address": addr, "database": "app", "username": "app",
		"password": map[string]any{"file": passwordPath},
		"format":   "custom", "compression": "none",
		"connect_attempts": 2, "connect_interval": "20ms",
		"all_databases": true, "exclude_databases": exclude,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// unitsConfig is unitsConfigPath with a password file that exists.
func unitsConfig(t *testing.T, addr string, exclude ...string) string {
	t.Helper()
	return unitsConfigPath(t, addr, passwordFile(t), exclude...)
}

func enumerator(t *testing.T, s source.Source) source.Enumerator {
	t.Helper()
	e, ok := s.(source.Enumerator)
	if !ok {
		t.Fatal("the postgres source does not enumerate its databases")
	}
	return e
}

// databaseOf decodes one unit's config.
func databaseOf(t *testing.T, u source.Unit) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(u.Config, &m); err != nil {
		t.Fatalf("unit %q config does not decode: %v", u.Name, err)
	}
	return m
}

// TestUnitsEnumeratesEveryDatabase: one artifact per database the server lists,
// each built from a config of this same type that dumps exactly that one.
func TestUnitsEnumeratesEveryDatabase(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "out.psql", "app\nanalytics\nshows\n")
	s, err := build(t, unitsConfig(t, listener(t), "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	units, err := enumerator(t, s).Units(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"app", "analytics", "shows"} {
		if units[i].Name != want {
			t.Fatalf("units = %+v, want the server's own order", units)
		}
	}
	// The config a unit is dumped with: the instance's own, with the database
	// substituted and the enumeration off — a unit that enumerated again would
	// never end.
	cfg := databaseOf(t, units[0])
	if cfg["database"] != "app" || cfg["all_databases"] != false {
		t.Fatalf("unit config = %+v", cfg)
	}
	if list, ok := cfg["exclude_databases"].([]any); !ok || len(list) != 0 {
		t.Fatalf("unit config exclusions = %v, want an empty list", cfg["exclude_databases"])
	}
	if password, ok := cfg["password"].(map[string]any); !ok || password["file"] == "" {
		t.Fatalf("unit config password = %v, want the file reference", cfg["password"])
	}
	if cfg["connect_interval"] != "20ms" {
		t.Fatalf("unit config connect_interval = %v, want the duration the instance carries", cfg["connect_interval"])
	}
	// The strict decoder is the proof the round trip lost nothing: a Secret or
	// a duration marshalled into a shape the plugin does not decode would be
	// refused here.
	if err := testRegistry(t).Check("source", "postgres", units[0].Config); err != nil {
		t.Fatalf("a unit's config does not decode as a postgres source: %v", err)
	}
	// The enumeration is the server's own answer: the query asks for the
	// databases a dump can reach, and the password travels in the environment.
	argv := stubLog(t, "argv.psql")
	if !strings.Contains(argv, "datallowconn") || !strings.Contains(argv, "-d\napp") {
		t.Fatalf("psql argv = %q, want the enumeration query against the configured database", argv)
	}
	if env := stubLog(t, "env.psql"); !strings.Contains(env, "PGPASSWORD=s3cret-pw") {
		t.Fatalf("the password did not reach psql's environment: %q", env)
	}
	// A unit is a source of this same type that answers "I am one artifact".
	unit, err := build(t, string(units[0].Config))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enumerator(t, unit).Units(context.Background()); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("a unit's Units = %v, want errors.ErrUnsupported", err)
	}
}

// TestUnitsLeavesOutTheExcluded: the exclusion list is exact names, applied to
// the server's own answer rather than woven into the query.
func TestUnitsLeavesOutTheExcluded(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "out.psql", "app\nanalytics\npostgres\n")
	s, err := build(t, unitsConfig(t, listener(t), "postgres", "analytics"))
	if err != nil {
		t.Fatal(err)
	}
	units, err := enumerator(t, s).Units(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].Name != "app" {
		t.Fatalf("units = %+v, want only the database that is neither excluded", units)
	}
}

// TestUnitsSingleDatabaseIsOneArtifact: with all_databases false the instance is
// what it always was — one artifact — and the engine reads exactly that.
func TestUnitsSingleDatabaseIsOneArtifact(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enumerator(t, s).Units(context.Background()); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Units = %v, want errors.ErrUnsupported", err)
	}
}

// TestUnitsRefusals: every way an enumeration fails is classified, so the
// engine's retry policy can decide — a server that is not answering is worth
// another attempt, a refused credential, an impossible artifact name and a
// password file that cannot be read are not.
func TestUnitsRefusals(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(*testing.T)
		config    func(*testing.T) string
		want      string
		wantClass plugin.Class
		// wantUnits is the whole enumeration: set when the call succeeds, and
		// the exact names are the assertion.
		wantUnits []string
	}{
		{
			name: "no database left to dump",
			setup: func(t *testing.T) {
				stubSet(t, "out.psql", "app\npostgres\n")
			},
			config: func(t *testing.T) string { return unitsConfig(t, listener(t), "app", "postgres") },
			want:   "holds no database to dump",
			// The server answered; another attempt answers the same way.
			wantClass: plugin.ClassPermanent,
		},
		{
			name: "an empty answer",
			setup: func(t *testing.T) {
				stubSet(t, "out.psql", "\n")
			},
			config:    func(t *testing.T) string { return unitsConfig(t, listener(t)) },
			want:      "holds no database to dump",
			wantClass: plugin.ClassPermanent,
		},
		{
			// The name is the server's: this plugin reports it, and the engine
			// refuses it as that database's own failed artifact.
			name: "a name that cannot be an artifact",
			setup: func(t *testing.T) {
				stubSet(t, "out.psql", "app\nodd/name\n")
			},
			config:    func(t *testing.T) string { return unitsConfig(t, listener(t)) },
			wantUnits: []string{"app", "odd/name"},
		},
		{
			name: "the credential is refused",
			setup: func(t *testing.T) {
				stubSet(t, "exit.psql", "2")
				stubSet(t, "stderr.psql", `psql: error: connection to server failed: FATAL:  password authentication failed for user "app"`)
			},
			config:    func(t *testing.T) string { return unitsConfig(t, listener(t)) },
			want:      "password authentication failed",
			wantClass: plugin.ClassPermanent,
		},
		{
			name:      "the server is not answering",
			setup:     func(t *testing.T) {},
			config:    func(t *testing.T) string { return unitsConfig(t, "127.0.0.1:1") },
			want:      "connection refused",
			wantClass: plugin.ClassTransient,
		},
		{
			name:      "the password file cannot be read",
			setup:     func(t *testing.T) {},
			config:    func(t *testing.T) string { return unitsConfigPath(t, listener(t), "/nonexistent/nope/password") },
			wantClass: plugin.ClassPermanent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubs(t, "pg_dump", "psql", "pg_restore")
			tc.setup(t)
			s, err := build(t, tc.config(t))
			if err != nil {
				t.Fatal(err)
			}
			units, err := enumerator(t, s).Units(context.Background())
			if tc.wantUnits != nil {
				// A name the engine cannot use still leaves the enumeration whole:
				// the names are the server's and are handed back as they are.
				var got []string
				for _, u := range units {
					got = append(got, u.Name)
				}
				if err != nil || strings.Join(got, ",") != strings.Join(tc.wantUnits, ",") {
					t.Fatalf("units = %v, %v, want %v", got, err, tc.wantUnits)
				}
				return
			}
			if err == nil {
				t.Fatal("the enumeration was accepted")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
			if class, _ := plugin.ClassOf(err); class != tc.wantClass {
				t.Fatalf("class = %q, want %q (%v)", class, tc.wantClass, err)
			}
		})
	}
}

// TestUnitsReturnsOnCancellation: the enumeration is a plugin call, and a
// cancelled context is an error like everywhere else.
func TestUnitsReturnsOnCancellation(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, unitsConfig(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := enumerator(t, s).Units(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units = %v, want the cancellation", err)
	}
}
