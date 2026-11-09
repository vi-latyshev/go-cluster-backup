package main

import (
	"strings"
	"testing"
)

func TestR4_C2_ProjectionSweep(t *testing.T) {
	for name, body := range map[string]string{
		"address short":     `func mint() { o := &backup.Options{}; _ = o.Ledger.Authorize }`,
		"inferred var":      `func mint() { var o = backup.Options{}; _ = o.Ledger.Authorize }`,
		"aggregate alias":   `type Options = backup.Options; func mint(o Options) { _ = o.Ledger.Authorize }`,
		"indexed parameter": `func mint(o []backup.Options) { _ = o[0].Ledger.Authorize }`,
		"scope shadow":      `type Safe struct{ Ledger int }; func mint(o backup.Options) { _ = o.Ledger.Authorize; { var o Safe; _ = o } }`,
		"named issuer":      `type Issuer interface{ Authorize(time.Time) *guard.Authority }; func mint(a any) { _ = a.(Issuer).Authorize }`,
	} {
		t.Run(name, func(t *testing.T) {
			r := mustScan(t, map[string]string{
				"internal/plugins/workflow/backup/options.go": `package backup
import "example.test/app/internal/plugins/protection"
type Options struct{ Ledger *protection.Ledger }`,
				"internal/plugins/workflow/alternative/write.go": `package alternative
import ("time"; "example.test/app/internal/plugins/workflow/backup"; "example.test/app/internal/plugins/storage/guard")
` + body,
			})
			if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
				t.Fatal("usable issuer form passed both mutation rules")
			}
		})
	}
}

// TestR4_C2_TypeResolutionEdges proves the resolved-type pass handles map and
// slice elements, new(T), extra declaration names, non-AND unary values,
// non-struct and missing receivers, define vs assign, and an alias cycle,
// without accepting any of them as a ledger projection.
func TestR4_C2_TypeResolutionEdges(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/backup/options.go": `package backup

import "example.test/app/internal/plugins/protection"

type Options struct{ Ledger *protection.Ledger }
`,
		"internal/plugins/workflow/alternative/edges.go": `package alternative

import (
	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/workflow/backup"
)

type cycA cycB
type cycB cycA

type safe struct{ n int }

func mapIndex(m map[string]backup.Options) { _ = m["key"].Ledger }

func shortNew() {
	o := new(backup.Options)
	_ = o.Ledger
}

func extraNames() {
	var a, b = backup.Options{}
	_ = a
	_ = b
}

func negative() {
	var x = -1
	_ = x
}

func unknownIndex(u node) { _ = u[0].Ledger }

func nonStruct(n safe) {
	_ = n.n.Ledger
	_ = n.n.unknown.Ledger
	_ = n.unknown.Ledger
}

func badIndex(s int) { _ = s[0].Ledger }

func localNoType() {
	var x = 1
	_ = x
}

func nonDefine() {
	x := 1
	x = 2
	_ = x
}

func cycle() {
	var c cycA
	_ = c.Ledger
}

type ledgerAlias = *protection.Ledger

func localAlias() {
	var l ledgerAlias
	_ = l
}

func unknownIndex() { _ = missing[0].Ledger }

type custom struct{}

func (custom) Authorize() {}

func (*custom) Revoke() {}

func assertCustom(a any) { _ = a.(custom).Authorize }

func makeOptions() backup.Options { return backup.Options{} }

func viaCall() {
	o := makeOptions()
	_ = o.Ledger
}

func directCall() { _ = makeOptions().Ledger }

type factory struct{}

func (factory) Make() backup.Options { return backup.Options{} }

func (factory) Unknown() int { return 0 }

func viaMethod() {
	f := factory{}
	_ = f.Make().Ledger
	_ = f.Unknown()
	_ = factory{}.Make().Ledger
}

func callFallbacks() {
	_ = backup.Missing()
	_ = missing.Make()
	_ = missing.Factory().Ledger
	(func() backup.Options { return backup.Options{} })().Ledger
}

func localCall() {
	var o = makeOptions()
	_ = o
}

type Generic[T any] struct{ Data T }

func (g Generic[T]) Reset() {}

func (m Missing) Orphan() {}

func nonIdentLHS() {
	m["key"], ok := 1, true
	_, _ = m, ok
}
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	for _, finding := range findings {
		holds := strings.Contains(finding, "only protection, assembly and the Floor may hold protection.Ledger")
		asserts := strings.Contains(finding, "an authority issuer assertion is restricted")
		if !holds && !asserts {
			t.Fatalf("unexpected finding: %q", finding)
		}
	}
	if len(findings) != 18 {
		t.Fatalf("findings = %v", findings)
	}
}
