package protection_test

import (
	"reflect"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// TestR4_C2_OptionsCarriesNoLedgerField: the public Floor aggregate must not
// project a usable issuer. An aliased, inferred or indexed backup.Options value
// holds no field of a restricted type, and Workflow exposes no accessor that
// returns the ledger — the ledger reaches the Floor only as a New argument.
func TestR4_C2_OptionsCarriesNoLedgerField(t *testing.T) {
	ledgerType := reflect.TypeOf((*protection.Ledger)(nil))
	opts := reflect.TypeOf(backup.Options{})
	for i := 0; i < opts.NumField(); i++ {
		f := opts.Field(i)
		if f.Type == ledgerType {
			t.Fatalf("Options.%s projects a usable issuer (*protection.Ledger)", f.Name)
		}
		if f.Type.Kind() == reflect.Interface {
			if _, ok := f.Type.MethodByName("Authorize"); ok {
				t.Fatalf("Options.%s projects an issuer interface", f.Name)
			}
		}
	}
	workflow := reflect.TypeOf(backup.Workflow{})
	for i := 0; i < workflow.NumMethod(); i++ {
		m := workflow.Method(i)
		if m.Type.NumOut() > 0 && m.Type.Out(0) == ledgerType {
			t.Fatalf("Workflow.%s returns a usable issuer", m.Name)
		}
	}
}
