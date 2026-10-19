package model

import (
	"strings"
	"testing"

	json "encoding/json/v2"
)

// TestByteSizeDecodesEveryAcceptedForm pins the grammar: an integer is bytes,
// KB/MB/GB/TB are 1000^n, KiB/MiB/GiB/TiB are 1024^n, and nothing else is a
// size. Removing any one of these lines is the mutant this test catches.
func TestByteSizeDecodesEveryAcceptedForm(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{`1073741824`, 1073741824},
		{`0`, 0},
		{`"70GB"`, 70000000000},
		{`"14GB"`, 14000000000},
		{`"900KB"`, 900000},
		{`"64MiB"`, 67108864},
		{`"16MiB"`, 16777216},
		{`"1GiB"`, 1073741824},
		{`"5GiB"`, 5368709120},
		{`"5TiB"`, 5497558138880},
		{`"512B"`, 512},
	}
	for _, c := range cases {
		var got ByteSize
		if err := json.Unmarshal([]byte(c.in), &got); err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got.Int64() != c.want {
			t.Errorf("%s = %d, want %d", c.in, got.Int64(), c.want)
		}
	}
}

func TestByteSizeRefusesWhatIsNotASize(t *testing.T) {
	bad := []string{
		`"1024"`,  // digits without a unit: ambiguous, and the integer form exists
		`"5gb"`,   // case matters: KiB is not KB
		`"5 GB"`,  // no spaces in the grammar
		`"-5GB"`,  // no sign
		`"5.5GB"`, // no fractions
		`"GB"`,    // no digits
		`"5XB"`,   // no such unit
		`"5KiBx"`, // trailing text
		`"5PiB"`,  // not a unit this build reads
		`1.5`,     // a fraction of a byte
		`true`,    // not a size
		`{}`,      // not a size
		`"5GiB "`, // trailing space
		`""`,      // empty
	}
	for _, in := range bad {
		var got ByteSize
		if err := json.Unmarshal([]byte(in), &got); err == nil {
			t.Errorf("%s decoded as %d, want an error", in, got.Int64())
		}
	}
}

// TestByteSizeRefusesOverflow: a count past int64 must be an error, never a
// silently wrapped value.
func TestByteSizeRefusesOverflow(t *testing.T) {
	var got ByteSize
	err := json.Unmarshal([]byte(`"9999999999TiB"`), &got)
	if err == nil {
		t.Fatalf("an overflowing size decoded as %d", got.Int64())
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("error does not name the overflow: %v", err)
	}
}

// TestByteSizeMarshalsAsWholeBytes: the wire form is the plain byte count, the
// one representation json/v2 does not give ByteSize itself. It is what a
// manifest round-trips and what `config effective` prints.
func TestByteSizeMarshalsAsWholeBytes(t *testing.T) {
	b, err := json.Marshal(ByteSize(67108864))
	if err != nil || string(b) != "67108864" {
		t.Fatalf("MarshalJSON = %s, %v", b, err)
	}
}

// TestByteSizeUnmarshalJSONReportsMalformedInput: called directly with bytes
// that are not a JSON string or number the method reports it rather than
// decoding a zero.
func TestByteSizeUnmarshalJSONReportsMalformedInput(t *testing.T) {
	var got ByteSize
	if err := got.UnmarshalJSON([]byte(`"5GiB`)); err == nil {
		t.Fatalf("an unterminated string decoded as %d", got.Int64())
	}
}

// TestByteSizeRefusesDigitsPastInt64: the digit run overflows before any unit
// multiplication, so it is named here and not only at the multiply.
func TestByteSizeRefusesDigitsPastInt64(t *testing.T) {
	var got ByteSize
	err := json.Unmarshal([]byte(`"99999999999999999999B"`), &got)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("a digit run past int64 = %d, %v", got.Int64(), err)
	}
}

func TestByteSizeStringIsExact(t *testing.T) {
	cases := map[ByteSize]string{
		67108864:   "64MiB",
		5368709120: "5GiB",
		900000:     "900000 B", // 900 KB decimal does not divide by 1024
		512:        "512 B",
		1 << 40:    "1TiB",
	}
	for size, want := range cases {
		if got := size.String(); got != want {
			t.Errorf("%d renders %q, want %q", int64(size), got, want)
		}
	}
}
