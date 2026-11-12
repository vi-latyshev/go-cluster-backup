package protection

import "testing"

func TestR2_C7_EveryPendingPrefixStaysProtected(t *testing.T) {
	// A zero retention state grows its set on first use instead of dropping a
	// holder into a nil map.
	var zero retentionState
	zero.markUncertain("fresh")
	state := newRetentionState(nil, "")
	for _, id := range []string{"holder-a", "holder-b"} {
		state.markUncertain(id)
	}
	for _, id := range []string{"holder-a", "holder-b"} {
		if allowed, _ := state.canDrop(id, "primary"); allowed {
			t.Errorf("pending prefix %s may be deleted", id)
		}
	}
}
