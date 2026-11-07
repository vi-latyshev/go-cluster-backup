package lifecycle

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// GraceInputs are the effective values of the chart grace formula, each read from
// the effective configuration: the chart's own preStop hook, plugins.config.lifecycle
// (join, abort_total, close, margin) and run.finish_allowance for the release.
type GraceInputs struct {
	PreStop    time.Duration
	Join       time.Duration
	Release    time.Duration
	AbortTotal time.Duration
	Close      time.Duration
	Margin     time.Duration
}

// RequiredGrace is the least terminationGracePeriodSeconds this configuration may
// be terminated under: every cleanup bound added up and rounded UP to a whole
// second. The chart sets that literal explicitly and passes the same value to the
// config gate as --termination-grace-seconds; the gate refuses a smaller one.
//
// A budget outside its allowed range, or a sum that does not fit, is an error:
// there is no zero or negative fallback that a caller could read as "no
// deadline", so the caller must refuse the configuration instead of running an
// unbounded cleanup.
func RequiredGrace(in GraceInputs) (time.Duration, error) {
	for _, budget := range []struct {
		name     string
		value    time.Duration
		positive bool
	}{
		{"pre_stop", in.PreStop, false},
		{"join", in.Join, true},
		{"release", in.Release, false},
		{"abort_total", in.AbortTotal, true},
		{"close", in.Close, true},
		{"margin", in.Margin, false},
	} {
		if err := checkBudget(budget.name, budget.value, budget.positive); err != nil {
			return 0, err
		}
	}
	sum := in.PreStop
	for _, v := range []time.Duration{in.Join, in.Release, in.AbortTotal, in.Close, in.Margin} {
		if sum > math.MaxInt64-v {
			return 0, errGraceOverflow
		}
		sum += v
	}
	return roundUpToSecond(sum)
}

var (
	errGraceOverflow = errors.New("lifecycle: cleanup bound overflows time.Duration")
	errGraceRounding = errors.New("lifecycle: cleanup bound cannot be rounded up in time.Duration")
)

// roundUpToSecond converts the nanosecond sum into the whole seconds the chart
// field carries, never rounding a fraction of a second away.
func roundUpToSecond(sum time.Duration) (time.Duration, error) {
	if sum > time.Duration(math.MaxInt64)-(time.Second-1) {
		return 0, errGraceRounding
	}
	return (sum + time.Second - 1) / time.Second * time.Second, nil
}

// checkBudget enforces one cleanup budget's range.
func checkBudget(name string, v time.Duration, positive bool) error {
	if v < 0 || (positive && v == 0) {
		kind := "must not be negative"
		if positive {
			kind = "must be positive"
		}
		return fmt.Errorf("lifecycle: %s %s, got %s", name, kind, v)
	}
	return nil
}
