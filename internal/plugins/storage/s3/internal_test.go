package s3

// Unit tests for the classification and decoding helpers the request paths all
// route through. They are internal because these are unexported: an SDK error
// the fake endpoint cannot produce (a code with no HTTP status, a Retry-After
// in HTTP-date form) still has to be classified the way the engine will read it.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// apiErr is a scripted smithy API error: the code and the HTTP status the
// classifier reads, with no dependency on a real endpoint.
type apiErr struct {
	code   string
	status int
}

func (e apiErr) Error() string                 { return e.code }
func (e apiErr) ErrorCode() string             { return e.code }
func (e apiErr) ErrorMessage() string          { return "scripted" }
func (e apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }
func (e apiErr) HTTPStatusCode() int           { return e.status }

// responseErr is the shape the SDK's transport hands back: a status, and the
// response headers a Retry-After hint arrives in.
func responseErr(status int, retryAfter string) error {
	header := http.Header{}
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	return &smithyhttp.ResponseError{Response: &smithyhttp.Response{
		Response: &http.Response{StatusCode: status, Header: header},
	}}
}

func TestClassifyOfNilIsNil(t *testing.T) {
	s := &Storage{}
	if err := s.classify(context.Background(), nil, "x"); err != nil {
		t.Fatalf("classify(nil) = %v, want nil", err)
	}
}

func TestClassifyMapsEveryAnswer(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class plugin.Class
		after time.Duration
	}{
		{"absent bucket", apiErr{code: "NoSuchBucket", status: 404}, plugin.ClassPermanent, 0},
		{"request timeout code", apiErr{code: "RequestTimeout"}, plugin.ClassTransient, 0},
		{"unauthorized", apiErr{code: "AccessDenied", status: 401}, plugin.ClassPermanent, 0},
		{"forbidden", apiErr{code: "AccessDenied", status: 403}, plugin.ClassPermanent, 0},
		{"slow down", apiErr{code: "SlowDown", status: 429}, plugin.ClassTransient, 0},
		{"server error", apiErr{code: "InternalError", status: 500}, plugin.ClassTransient, 0},
		{"absent key", apiErr{code: "NoSuchKey", status: 404}, plugin.ClassNotFound, 0},
		{"bad request", apiErr{code: "InvalidRequest", status: 400}, plugin.ClassPermanent, 0},
		{"no status at all", apiErr{code: "SomethingElse"}, plugin.ClassTransient, 0},
		{"retry-after seconds", responseErr(503, "7"), plugin.ClassTransient, 7 * time.Second},
		{"retry-after date", responseErr(503, time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)), plugin.ClassTransient, time.Hour},
		{"retry-after in the past", responseErr(503, time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)), plugin.ClassTransient, 0},
		{"retry-after unreadable", responseErr(503, "soon"), plugin.ClassTransient, 0},
		{"transport failure", errors.New("connection reset"), plugin.ClassTransient, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Storage{}
			class, after := plugin.ClassOf(s.classify(context.Background(), tc.err, "op"))
			if class != tc.class {
				t.Errorf("class %q, want %q", class, tc.class)
			}
			if tc.after == 0 {
				if after != 0 {
					t.Errorf("delay %s, want none", after)
				}
				return
			}
			// An HTTP-date hint is a duration from now, so it only has to be
			// the right order of magnitude.
			if after <= 0 || after > tc.after {
				t.Errorf("delay %s, want a positive one no larger than %s", after, tc.after)
			}
		})
	}
}

func TestClassifyKeepsTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &Storage{}
	err := s.classify(ctx, apiErr{code: "InternalError", status: 500}, "op")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("classify with a dead context = %v, want the context's own error", err)
	}
	if class, _ := plugin.ClassOf(err); class != "" {
		t.Errorf("class %q, want unclassified", class)
	}
}

func TestAPICodeAndStatus(t *testing.T) {
	if got := apiErrorCode(apiErr{code: "NoSuchBucket", status: 404}); got != "NoSuchBucket" {
		t.Errorf("apiErrorCode = %q", got)
	}
	if got := apiErrorCode(errors.New("plain")); got != "" {
		t.Errorf("apiErrorCode of a plain error = %q", got)
	}
	if got := apiStatus(apiErr{code: "NoSuchBucket", status: 404}); got != 404 {
		t.Errorf("apiStatus = %d", got)
	}
	if got := apiStatus(responseErr(503, "")); got != 503 {
		t.Errorf("apiStatus of a bare response error = %d", got)
	}
	if got := apiStatus(errors.New("plain")); got != 0 {
		t.Errorf("apiStatus of a plain error = %d", got)
	}
	if got := apiStatus(nil); got != 0 {
		t.Errorf("apiStatus(nil) = %d", got)
	}
}

func TestIsNotFoundOnlyForTheObject(t *testing.T) {
	if !isNotFound(apiErr{code: "NoSuchKey", status: 404}) {
		t.Error("a 404 for a key was not read as absence")
	}
	if isNotFound(apiErr{code: "NoSuchBucket", status: 404}) {
		t.Error("a 404 for the bucket was read as absence")
	}
	if isNotFound(apiErr{code: "AccessDenied", status: 403}) {
		t.Error("a 403 was read as absence")
	}
}

func TestRetryAfterHint(t *testing.T) {
	if d := retryAfterHint(errors.New("plain")); d != 0 {
		t.Errorf("hint of a plain error = %s", d)
	}
	if d := retryAfterHint(responseErr(503, "3")); d != 3*time.Second {
		t.Errorf("hint = %s, want 3s", d)
	}
	if d := retryAfterHint(responseErr(503, "nonsense")); d != 0 {
		t.Errorf("hint of an unreadable value = %s", d)
	}
	if d := retryAfterHint(responseErr(503, time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat))); d != 0 {
		t.Errorf("hint of a past date = %s", d)
	}
	if d := retryAfterHint(responseErr(503, time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))); d <= 0 {
		t.Errorf("hint of a future date = %s, want a positive one", d)
	}
}

func TestETagMD5(t *testing.T) {
	cases := map[string]string{
		`"d41d8cd98f00b204e9800998ecf8427e"`:   "d41d8cd98f00b204e9800998ecf8427e",
		`"D41D8CD98F00B204E9800998ECF8427E"`:   "d41d8cd98f00b204e9800998ecf8427e",
		`"d41d8cd98f00b204e9800998ecf8427e-3"`: "",
		`"not-hex-at-all-but-32-characters"`:   "",
		`"short"`:                              "",
		`""`:                                   "",
	}
	for etag, want := range cases {
		got := etagMD5(&etag)
		if got != want {
			t.Errorf("etagMD5(%q) = %q, want %q", etag, got, want)
		}
	}
	if got := etagMD5(nil); got != "" {
		t.Errorf("etagMD5(nil) = %q", got)
	}
}

func TestReadSecret(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	if err := os.WriteFile(good, []byte("  tenant:key-id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := readSecret(plugin.Secret{File: good})
	if err != nil || v != "tenant:key-id" {
		t.Fatalf("readSecret = %q, %v", v, err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecret(plugin.Secret{File: empty}); err == nil {
		t.Error("an empty secret file was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("class %q, want permanent", class)
	}
	if _, err := readSecret(plugin.Secret{File: filepath.Join(dir, "absent")}); err == nil {
		t.Error("a missing secret file was accepted")
	}
}

func TestKeyBuildsUnderThePrefix(t *testing.T) {
	withPrefix := &Storage{cfg: Config{Prefix: "root"}}
	if got := withPrefix.key("a/b"); got != "root/a/b" {
		t.Errorf("key = %q", got)
	}
	if got := withPrefix.key(""); got != "root/" {
		t.Errorf("the storage root key = %q", got)
	}
	noPrefix := &Storage{}
	if got := noPrefix.key("a"); got != "a" {
		t.Errorf("key without a prefix = %q", got)
	}
}

// shortBodyRT answers every request with a 200 whose declared length is larger
// than the body it carries: the one shape Get's length guard exists for, and
// one no real transport produces — net/http errors on a body shorter than
// Content-Length rather than ending it cleanly.
type shortBodyRT struct{}

func (shortBodyRT) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Length": {"100"}},
		Body:          io.NopCloser(strings.NewReader("abc")),
		ContentLength: 100,
		Request:       req,
	}, nil
}

func secretFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGetRejectsABodyShorterThanItsDeclaredLength(t *testing.T) {
	s := &Storage{cfg: Config{
		Endpoint: "https://s3.example.com", Region: "ru-central-1", Bucket: "b", Prefix: "root",
		AccessKeyID:     plugin.Secret{File: secretFile(t, "tenant:key-id")},
		SecretAccessKey: plugin.Secret{File: secretFile(t, "secret-key")},
		Addressing:      AddressingPath, StorageClass: "STANDARD",
		PartBytes:       model.ByteSize(minPartBytes),
		MaxRequestBytes: 1 << 30, MaxObjectBytes: 1 << 40,
		RequestTimeout: model.Duration(time.Second),
	}}
	s.http = &http.Client{Transport: shortBodyRT{}}
	err := s.Get(context.Background(), "a.bin", io.Discard)
	if err == nil {
		t.Fatal("a download shorter than its declared length was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
}
