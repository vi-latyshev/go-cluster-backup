package webhook

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestSampleMatrixCoversKindsAndRefusals: every branch a template may take has a
// sample that reaches it. A manual run_started, a lease refusal and a cap
// refusal are the three the matrix gained last, and each is one a template that
// only ever saw a scheduled night would render wrong.
func TestSampleMatrixCoversKindsAndRefusals(t *testing.T) {
	var startedManual, finishedManual, leaseRefused, capRefused bool
	for _, s := range samples() {
		if s.ev.Run.Kind == model.RunKindManual {
			if s.ev.Name == model.EventStarted {
				startedManual = true
			}
			if s.ev.Name == model.EventFinished {
				finishedManual = true
			}
		}
		if s.ev.Run.Lease != nil && s.ev.Run.Lease.Refused {
			leaseRefused = true
		}
		for _, st := range s.ev.Run.Storages {
			if st.CapExceeded {
				capRefused = true
			}
		}
	}
	for _, c := range []struct {
		ok   bool
		name string
	}{
		{startedManual, "a manual run_started sample (the manual start branch never renders without one)"},
		{finishedManual, "a manual run_finished sample"},
		{leaseRefused, "a lease-refusal sample (Run.Lease.Refused)"},
		{capRefused, "a cap-refusal sample (StorageResult.CapExceeded)"},
	} {
		if !c.ok {
			t.Errorf("the sample matrix has no %s", c.name)
		}
	}
}
