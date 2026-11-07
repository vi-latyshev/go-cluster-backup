package main

import (
	"strings"
	"testing"
)

// TestR2_C2_ZeroLedgerAuthorityMustBeRefused is the reviewer's probe, copied
// as written: a consumer mints authority with a zero Ledger and hands it to a
// read port. The rule must refuse the issuer.
func TestR2_C2_ZeroLedgerAuthorityMustBeRefused(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative
import (
 "time"
 "example.test/app/internal/plugins/protection"
 "example.test/app/internal/plugins/storage/guard"
)
func bypass(port guard.Port) {
 _, _ = port.Open(new(protection.Ledger).Authorize(time.Now().Add(time.Hour)))
}
`,
	})
	if findings := ruleRestrictedSymbols(scanned, nil); len(findings) == 0 {
		t.Fatal("consumer can mint authority with a zero Ledger; rule reports no findings")
	}
}

// TestR2_C2_AuthorityIssuers proves every issuer form the contract names is
// refused outside the Floor: zero and composite values, new, NewLedger, pointers,
// variables, parameters, struct fields, slice elements, aliases, method values
// and method expressions.
func TestR2_C2_AuthorityIssuers(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"time"

	"example.test/app/internal/plugins/protection"
)

type minter struct{ ledger *protection.Ledger }

type alias = *protection.Ledger

func direct() {
	new(protection.Ledger).Authorize(time.Time{})
	(&protection.Ledger{}).Authorize(time.Time{})
	protection.Ledger{}.Authorize(time.Time{})
	protection.NewLedger(protection.LedgerOptions{}).Authorize(time.Time{})
}

func methodValue(l *protection.Ledger) {
	f := l.Authorize
	_ = f
	g := (*protection.Ledger).Authorize
	_ = g
	h := protection.Ledger.Authorize
	_ = h
}

func fields(m minter, ledgers []*protection.Ledger) {
	m.ledger.Authorize(time.Time{})
	ledgers[0].Authorize(time.Time{})
}

func makeLedger() *protection.Ledger { return nil }

func produced() {
	makeLedger().Authorize(time.Time{})
}

func variables() {
	var value protection.Ledger
	value.Authorize(time.Time{})
	var pointer (protection.Ledger)
	pointer.Authorize(time.Time{})
	var viaAlias alias
	viaAlias.Authorize(time.Time{})
	var byName map[string]*protection.Ledger
	byName["key"].Authorize(time.Time{})
	assigned := protection.NewLedger(protection.LedgerOptions{})
	assigned.Authorize(time.Time{})
	(*assigned).Authorize(time.Time{})
	(new(protection.Ledger)).Authorize(time.Time{})
}
`,
	})
	findings := ruleRestrictedSymbols(scanned, nil)
	for _, finding := range findings {
		authorizes := strings.Contains(finding, "protection.Ledger.Authorize is restricted to internal/plugins/workflow/backup")
		holds := strings.Contains(finding, "only protection, assembly and the Floor may hold protection.Ledger")
		constructs := strings.Contains(finding, "protection.NewLedger is restricted")
		if !authorizes && !holds && !constructs {
			t.Fatalf("unexpected finding: %q", finding)
		}
	}
	if len(findings) != 44 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestR2_C2_FloorAndUnrelatedAuthorize proves the Floor may mint and another
// Authorize method is not a ledger issuer.
func TestR2_C2_FloorAndUnrelatedAuthorize(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/backup/floor.go": `package backup

import (
	"time"

	"example.test/app/internal/plugins/protection"
)

type floor struct{ ledger *protection.Ledger }

func (f floor) mint() {
	f.ledger.Authorize(time.Time{})
}
`,
		"internal/plugins/workflow/alternative/clean.go": `package alternative

import (
	"example.test/app/internal/plugins/library"
	"example.test/app/internal/plugins/protection"
)

type own struct{}

func (own) Authorize() {}

func clean(o own, factory func() own, a, b int) {
	o.Authorize()
	factory().Authorize()
	library.Authorize()
	(a + b).Authorize()
	var structurally struct{}
	var untyped = 1
	_ = structurally
	_ = untyped
	_ = protection.LedgerOptions{}
}
`,
	})
	if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestR2_C2_DeclarationLeaks proves a declaration that could carry a mutation
// value across a package boundary is refused outside protection, assembly and
// the Floor: results, parameters, struct fields, named types and package vars.
func TestR2_C2_DeclarationLeaks(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/leak.go": `package alternative

import (
	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/storage/guard"
)

func MakeLedger() *protection.Ledger { return nil }

func consume(remover *guard.Remover) {}

type sink struct{ removers map[string]guard.Remover }

type alias = *protection.Ledger

var held protection.Ledger
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	if len(findings) != 5 {
		t.Fatalf("findings = %v", findings)
	}
	for _, want := range []string{"guard.Remover", "protection.Ledger"} {
		wantFinding(t, findings, want)
	}
}

// TestR2_C2_DeclarationBoundary proves the designated packages may declare the
// mutation types and export producers; the consumer side is what the rule
// protects.
func TestR2_C2_DeclarationBoundary(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/assembly/assembly.go": `package assembly

import (
	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/storage/guard"
)

type holder struct {
	ledger  *protection.Ledger
	remover guard.Remover
}

func MakeLeaks() (*protection.Ledger, guard.Remover) { return nil, guard.Remover{} }

func take(ledger *protection.Ledger, remover guard.Remover) {}
`,
		"internal/plugins/workflow/backup/floor.go": `package backup

import (
	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/storage/guard"
)

type floor struct{ ledger *protection.Ledger }
`,
	})
	if findings := ruleLeakedMutations(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestR2_C2_CrossPackageLaunder is the coordinator fixture: an allowed package
// exports a producer, a consumer calls it and mints authority; only the consumer
// is refused. The alias chain proves the producer scan resolves type aliases.
func TestR2_C2_CrossPackageLaunder(t *testing.T) {
	t.Run("direct producer", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/assembly/mint.go": `package assembly

import "example.test/app/internal/plugins/protection"

func MakeLedger() *protection.Ledger { return nil }
`,
			"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"time"

	a "example.test/app/internal/plugins/assembly"
)

func launder() {
	a.MakeLedger().Authorize(time.Time{})
}
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 2 {
			t.Fatalf("findings = %v", findings)
		}
		wantFinding(t, findings, "write.go")
		wantFinding(t, findings, "protection.Ledger.Authorize is restricted")
	})
	t.Run("alias producer", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/assembly/mint.go": `package assembly

import "example.test/app/internal/plugins/protection"

type ledger = *protection.Ledger

func Make() ledger { return nil }
`,
			"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"time"

	a "example.test/app/internal/plugins/assembly"
)

func launder() {
	a.Make().Authorize(time.Time{})
}
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 2 {
			t.Fatalf("findings = %v", findings)
		}
		wantFinding(t, findings, "protection.Ledger.Authorize is restricted")
	})
	t.Run("remover producer", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/assembly/mint.go": `package assembly

import "example.test/app/internal/plugins/storage/guard"

func MakeRemover() guard.Remover { return guard.Remover{} }
`,
			"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"context"

	a "example.test/app/internal/plugins/assembly"
)

func drop() {
	a.MakeRemover().Delete(context.Background(), "key")
}
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 2 {
			t.Fatalf("findings = %v", findings)
		}
		wantFinding(t, findings, "Remover.Delete is restricted")
	})
	t.Run("non-producers stay clean", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/assembly/mint.go": `package assembly

import "example.test/app/internal/plugins/protection"

func MakeLedger() *protection.Ledger { return nil }
`,
			"internal/plugins/workflow/alternative/clean.go": `package alternative

import (
	"time"

	a "example.test/app/internal/plugins/assembly"
	other "example.test/app/internal/plugins/library"
)

func build() *object { return nil }

func clean() {
	other.NotProducer()
	a.NotProducer().Authorize(time.Time{})
	build().Authorize()
	(noProducers()).Authorize()
	receiver().Make().Authorize()
	object{}.Remover()
}

func receiver() object { return object{} }

type object struct{}

func (object) Remover() {}
`,
		})
		if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
}

// TestR2_C2_CrossPackageAliasLeak proves an exported producer alias cannot be
// re-declared by a consumer to launder the type: the declaration rule resolves
// cross-package aliases, container forms and a local alias of the stolen one,
// and the consumer still cannot mint through the allowed producer.
func TestR2_C2_CrossPackageAliasLeak(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/assembly/mint.go": `package assembly

import "example.test/app/internal/plugins/protection"

type ledger = *protection.Ledger

func Make() ledger { return nil }
`,
		"internal/plugins/workflow/alternative/steal.go": `package alternative

import (
	"time"

	a "example.test/app/internal/plugins/assembly"
)

type stolen a.ledger
type paren (a.ledger)
type wrapper []a.ledger
type byName map[string]a.ledger
type stream chan a.ledger
type own int
type empty struct{}

func leak() stolen { return nil }

func launder() {
	a.Make().Authorize(time.Time{})
}
`,
	})
	leaks := ruleLeakedMutations(scanned, nil)
	if len(leaks) != 8 {
		t.Fatalf("leaks = %v", leaks)
	}
	for _, finding := range leaks {
		if !strings.Contains(finding, "steal.go") {
			t.Fatalf("a finding must name the consumer: %q", finding)
		}
	}
	minted := ruleRestrictedSymbols(scanned, nil)
	if len(minted) != 8 {
		t.Fatalf("minted = %v", minted)
	}
	wantFinding(t, minted, "protection.Ledger.Authorize is restricted")
}
