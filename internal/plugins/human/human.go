// Package human renders the numbers of one backup the way a person reads them:
// a rounded size with a unit, a duration at the tenth of a second, a
// compression ratio, and the [HH:MM:SS] clock every log line opens with. The
// exact values — the byte count, the sha256, the object key — stay in the
// manifest and the catalog; this package is only the log's rendering of them,
// and it is the one place that decides how.
package human

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ClockLayout is the timestamp every human line opens with. It is UTC: a run
// carries no timezone of its own, the container runs in UTC, and one clock for
// a log read from anywhere beats the reader's own.
const ClockLayout = "15:04:05"

// Stamp renders one moment as the log's clock.
func Stamp(t time.Time) string { return t.UTC().Format(ClockLayout) }

// Line prefixes one rendered message with the log's clock.
func Line(t time.Time, format string, args ...any) string {
	return "[" + Stamp(t) + "] " + fmt.Sprintf(format, args...)
}

// Size renders a byte count in binary units: one decimal below a thousand of a
// unit, and that decimal dropped when it is zero — 629.8 MiB, 71.2 MiB,
// 440 MiB, 512 B. A negative count reads as 0 B.
func Size(n int64) string {
	if n < 0 {
		n = 0
	}
	for _, u := range []struct {
		unit int64
		name string
	}{{1 << 40, "TiB"}, {1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}} {
		if n >= u.unit {
			return rounded(float64(n)/float64(u.unit)) + " " + u.name
		}
	}
	return strconv.FormatInt(n, 10) + " B"
}

func rounded(v float64) string {
	if v >= 1000 {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strings.TrimSuffix(strconv.FormatFloat(v, 'f', 1, 64), ".0")
}

// Dur renders a duration to the tenth of a second below ten seconds and to the
// second above, never the nanosecond tail of the raw value: 0.4s, 6.2s, 2m40s.
func Dur(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d < 10*time.Second:
		return d.Round(100 * time.Millisecond).String()
	default:
		return d.Round(time.Second).String()
	}
}

// Ratio renders how much a pipeline shrank its input: raw/stored as "3.4x". A
// stored size of zero — nothing was produced — is "—".
func Ratio(raw, stored int64) string {
	if stored <= 0 {
		return "—"
	}
	return strconv.FormatFloat(float64(raw)/float64(stored), 'f', 1, 64) + "x"
}
