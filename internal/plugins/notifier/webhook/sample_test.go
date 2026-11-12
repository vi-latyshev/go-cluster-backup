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

// previousShapes records which shapes of the previous-run record a sample
// carries.
type previousShapes struct {
	failure       bool // a previous Job failure with every container shape and its events
	containerless bool // failed earlier attempts with a pod that has no container status
	gone          bool // failed earlier attempts whose pods are all deleted
	unavailable   bool // a status the cluster could not answer
}

// TestSamplesCarryEveryPreviousShape: the previous-run record has two halves and
// a template may read both in either event. The storage half exists only for
// run_finished — a started sample carrying it would pass validation on data no
// started event ever has — while the cluster half reaches both: a previous Job's
// failure with each container shape, its events and its Consecutive count;
// failed earlier attempts with a containerless pod and with every pod gone; and
// an unreadable status.
func TestSamplesCarryEveryPreviousShape(t *testing.T) {
	var started, finished previousShapes
	for _, s := range samples() {
		prev := s.ev.Run.Previous
		var shapes *previousShapes
		switch s.ev.Name {
		case model.EventStarted:
			shapes = &started
			if prev.RunID != "" || prev.Unfinished || !prev.LastGoodAt.IsZero() {
				t.Errorf("%s: a started event carries no storage half, and this sample has %+v", s.name, prev)
			}
		case model.EventFinished:
			shapes = &finished
		}
		if prev.Failure != nil {
			var initTerminated, waiting, terminated bool
			for _, c := range prev.Failure.Containers {
				switch {
				case c.Init && c.Terminated != nil:
					initTerminated = true
				case c.Waiting != "":
					waiting = true
				case c.Terminated != nil:
					terminated = true
				}
			}
			if initTerminated && waiting && terminated && len(prev.Failure.Events) > 0 && prev.Failure.Consecutive > 1 {
				shapes.failure = true
			}
		}
		if prev.Attempts != nil {
			if prev.Attempts.Deleted > 0 && len(prev.Attempts.Pods) == 0 {
				shapes.gone = true
			}
			for _, pod := range prev.Attempts.Pods {
				if len(pod.Containers) == 0 && pod.Reason != "" {
					shapes.containerless = true
				}
			}
		}
		if prev.Unavailable != "" {
			shapes.unavailable = true
		}
	}
	for _, c := range []struct {
		ok   bool
		name string
	}{
		{started.failure, "a run_started sample with a previous Job failure (init terminated, waiting and terminated containers, events, Consecutive)"},
		{started.containerless, "a run_started sample with an attempt whose pod left no container status"},
		{started.gone, "a run_started sample with attempts whose pods are all deleted"},
		{started.unavailable, "a run_started sample with an unreadable cluster status"},
		{finished.failure, "a run_finished sample with a previous Job failure"},
		{finished.containerless, "a run_finished sample with an attempt whose pod left no container status"},
		{finished.gone, "a run_finished sample with attempts whose pods are all deleted"},
		{finished.unavailable, "a run_finished sample with an unreadable cluster status"},
	} {
		if !c.ok {
			t.Errorf("the sample matrix has no %s", c.name)
		}
	}
}
