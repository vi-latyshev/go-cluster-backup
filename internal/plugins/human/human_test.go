package human

import (
	"testing"
	"time"
)

func TestSize(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{-1, "0 B"},
		{512, "512 B"},
		{1024, "1 KiB"},
		{4812, "4.7 KiB"},
		{660362481, "629.8 MiB"},
		{74665056, "71.2 MiB"},
		{3 * 1 << 30, "3 GiB"},
		{20 * 1 << 30, "20 GiB"},
		{461 * 1 << 20, "461 MiB"},
		// The largest value the KiB unit can carry rounds to a whole unit, and
		// drops its decimal the way every value at or above a thousand does.
		{1<<20 - 1, "1024 KiB"},
	}
	for _, c := range cases {
		if got := Size(c.in); got != c.want {
			t.Errorf("Size(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDur(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{-time.Second, "0s"},
		{120 * time.Millisecond, "100ms"},
		{1500 * time.Millisecond, "1.5s"},
		{6200 * time.Millisecond, "6.2s"},
		{25 * time.Second, "25s"},
		// The nanosecond tail a raw Duration prints is what the log must never
		// carry: 2m39.620943507s reads as 2m40s.
		{2*time.Minute + 39*time.Second + 620943507*time.Nanosecond, "2m40s"},
	}
	for _, c := range cases {
		if got := Dur(c.in); got != c.want {
			t.Errorf("Dur(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestRatio(t *testing.T) {
	cases := []struct {
		raw, stored int64
		want        string
	}{
		{710058014, 10063665, "70.6x"},
		{20, 33, "0.6x"},
		{100, 0, "—"},
	}
	for _, c := range cases {
		if got := Ratio(c.raw, c.stored); got != c.want {
			t.Errorf("Ratio(%d, %d) = %q, want %q", c.raw, c.stored, got, c.want)
		}
	}
}

// TestLineIsUTCAndClocked pins the one clock every log line opens with.
func TestLineIsUTCAndClocked(t *testing.T) {
	// A moment handed to the log in Moscow time still prints as the UTC clock:
	// one clock for a log read from anywhere.
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.FixedZone("MSK", 3*3600))
	got := Line(at, "run %s starting", "20261018T011351Z")
	want := "[01:13:51] run 20261018T011351Z starting"
	if got != want {
		t.Errorf("Line = %q, want %q", got, want)
	}
}
