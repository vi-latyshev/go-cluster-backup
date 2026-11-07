package webhook

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
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

// TestStartedSamplesCarryNoPrevious: the previous-run record is filled only for
// the finished event, so a started sample that carried one would let a template
// pass validation on data no started event ever has. The finished side keeps a
// filled record, or a template reading it would never be rendered against one.
func TestStartedSamplesCarryNoPrevious(t *testing.T) {
	var finishedWithPrevious bool
	for _, s := range samples() {
		switch s.ev.Name {
		case model.EventStarted:
			if s.ev.Run.Previous != (model.PreviousRun{}) {
				t.Errorf("%s: a started event carries no previous run, and this sample has %+v", s.name, s.ev.Run.Previous)
			}
		case model.EventFinished:
			if s.ev.Run.Previous != (model.PreviousRun{}) {
				finishedWithPrevious = true
			}
		}
	}
	if !finishedWithPrevious {
		t.Error("no finished sample carries a previous run, so a template reading one is never rendered against it")
	}
}
