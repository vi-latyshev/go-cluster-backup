package model

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	json "encoding/json/v2"
)

// ByteSize is a byte count with the one JSON form json/v2 does not provide
// itself: a string with a unit. An integer is a count of bytes; a string
// carries a unit — KB, MB, GB, TB are powers of 1000, KiB, MiB, GiB, TiB
// powers of 1024. Nothing else decodes: a bare "1024" (is it bytes?) and a
// lowercase "5gb" are refused by name rather than guessed, and pgBackRest's
// reading of "5GB" as 5 GiB is not copied.
type ByteSize int64

// Int64 returns the count in bytes.
func (b ByteSize) Int64() int64 { return int64(b) }

// String renders the count exactly: the largest binary unit it divides by,
// otherwise the plain byte count. It never rounds, so it never lies about the
// value a config carries.
func (b ByteSize) String() string {
	for _, u := range []struct {
		unit int64
		name string
	}{{1 << 40, "TiB"}, {1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}} {
		if b%ByteSize(u.unit) == 0 {
			return strconv.FormatInt(int64(b)/u.unit, 10) + u.name
		}
	}
	return strconv.FormatInt(int64(b), 10) + " B"
}

func (b *ByteSize) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		v, err := ParseByteSize(s)
		if err != nil {
			return err
		}
		*b = ByteSize(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("size must be a whole number of bytes or a string with a unit like \"70GB\": %w", err)
	}
	*b = ByteSize(n)
	return nil
}

func (b ByteSize) MarshalJSON() ([]byte, error) { return json.Marshal(int64(b)) }

// sizeUnits is every unit a size string may carry: the 3-character ones first,
// so that "GB" is read as gigabytes and not as "G" bytes.
var sizeUnits = []struct {
	suffix string
	scale  int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"B", 1},
}

// ParseByteSize reads the string form: digits followed by one unit of
// sizeUnits. Case matters — KB is 1000 bytes and KiB is 1024, so the unit is
// spelled as it is meant; anything else is an error.
func ParseByteSize(s string) (int64, error) {
	for _, u := range sizeUnits {
		digits, ok := strings.CutSuffix(s, u.suffix)
		if !ok {
			continue
		}
		n, err := allDigits(digits)
		if err != nil {
			return 0, fmt.Errorf("size %q: %w", s, err)
		}
		if n > math.MaxInt64/u.scale {
			return 0, fmt.Errorf("size %q overflows a byte count", s)
		}
		return n * u.scale, nil
	}
	return 0, fmt.Errorf("size %q: use a whole number of bytes (as a JSON number) or digits with a unit — KB, MB, GB, TB (1000^n) or KiB, MiB, GiB, TiB (1024^n), e.g. 70GB", s)
}

// allDigits reads a non-empty run of ASCII digits; a sign, a dot or an empty
// string is not a size.
func allDigits(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("the unit carries no digits")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("use digits only, no sign and no fraction")
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the number is too large")
	}
	return n, nil
}
