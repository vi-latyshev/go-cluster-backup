package age_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	agecrypto "filippo.io/age"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor/age"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor/processortest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := age.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// writeIdentities writes one secret-key file and returns its path.
func writeIdentities(t *testing.T, identities ...*agecrypto.X25519Identity) string {
	t.Helper()
	var b strings.Builder
	for _, id := range identities {
		b.WriteString(id.String())
		b.WriteString("\n")
	}
	path := filepath.Join(t.TempDir(), "identity.txt")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func build(t *testing.T, recipients []string, identityFile string) processor.Processor {
	t.Helper()
	cfg := map[string]any{"recipients": recipients, "identities": []map[string]string{}}
	if identityFile != "" {
		cfg["identities"] = []map[string]string{{"file": identityFile}}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := plugin.Build(context.Background(), testRegistry(t), processor.Kind, "age", raw,
		plugin.Deps{Instance: "age", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

// TestProcessorConformance builds the fixture from the plugin's own example
// with one substitution: the example's identity path is an absolute path of a
// mounted secret, and a test cannot mount one, so the identity file the
// example names is written to a temp directory and pointed at instead. Every
// other value is the example's.
func TestProcessorConformance(t *testing.T) {
	processortest.Run(t, func(t *testing.T) processortest.Fixture {
		identity, err := agecrypto.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		return processortest.Fixture{
			Proc:  build(t, []string{identity.Recipient().String()}, writeIdentities(t, identity)),
			Plain: []byte("the control-plane snapshot's plaintext, as a fixture"),
		}
	})
}

func TestSealsAndExt(t *testing.T) {
	traits, err := build(t, []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"}, "").Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !traits.Seals {
		t.Error("Describe does not declare Seals: the engine's secret-artifact rule depends on it")
	}
	if traits.Ext != ".age" {
		t.Errorf("Ext is %q, want .age", traits.Ext)
	}
}

// TestEncodeOnlyRefusesDecode is the CronJob's configuration: recipients, no
// identity, so the running backup can seal and cannot open.
func TestEncodeOnlyRefusesDecode(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	p := build(t, []string{identity.Recipient().String()}, "")
	ctx := context.Background()
	var sealed bytes.Buffer
	if err := p.Encode(ctx, &sealed, strings.NewReader("secret payload")); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	err = p.Decode(ctx, &bytes.Buffer{}, bytes.NewReader(sealed.Bytes()))
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Decode on an instance without identities returned %v, want errors.ErrUnsupported", err)
	}
}

// TestDecodeOnlyRefusesEncode is the restore-side configuration.
func TestDecodeOnlyRefusesEncode(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	p := build(t, nil, writeIdentities(t, identity))
	if err := p.Encode(context.Background(), &bytes.Buffer{}, strings.NewReader("x")); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Encode on an instance without recipients returned %v, want errors.ErrUnsupported", err)
	}
}

func TestRoundTrip(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("datastore bytes, token bytes, and nothing else")
	sealer := build(t, []string{identity.Recipient().String()}, writeIdentities(t, identity))
	ctx := context.Background()
	var sealed bytes.Buffer
	if err := sealer.Encode(ctx, &sealed, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed.Bytes(), plain) {
		t.Fatal("the ciphertext contains the plaintext")
	}
	var back bytes.Buffer
	if err := sealer.Decode(ctx, &back, bytes.NewReader(sealed.Bytes())); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back.Bytes(), plain) {
		t.Fatalf("round trip returned %q, want %q", back.String(), plain)
	}
}

func TestWrongIdentityIsPermanent(t *testing.T) {
	sealedTo, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	other, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var sealed bytes.Buffer
	if err := build(t, []string{sealedTo.Recipient().String()}, "").Encode(ctx, &sealed, strings.NewReader("payload")); err != nil {
		t.Fatal(err)
	}
	err = build(t, nil, writeIdentities(t, other)).Decode(ctx, &bytes.Buffer{}, bytes.NewReader(sealed.Bytes()))
	if err == nil {
		t.Fatal("Decode with the wrong identity succeeded")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("wrong identity is %q, want permanent: another attempt with the same key cannot succeed", class)
	}
	// The failure must not leak key material.
	if strings.Contains(err.Error(), other.String()) || strings.Contains(err.Error(), sealedTo.Recipient().String()) {
		t.Errorf("the error names key material: %v", err)
	}
}

func TestTruncatedCiphertext(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sealer := build(t, []string{identity.Recipient().String()}, writeIdentities(t, identity))
	var sealed bytes.Buffer
	if err := sealer.Encode(ctx, &sealed, strings.NewReader(strings.Repeat("payload ", 1000))); err != nil {
		t.Fatal(err)
	}
	truncated := sealed.Bytes()[:sealed.Len()-20]
	err = sealer.Decode(ctx, &bytes.Buffer{}, bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("Decode accepted a truncated ciphertext")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("truncated ciphertext is %q, want permanent: the same bytes cannot decode on a retry", class)
	}
}

// TestIdentityErrorOmitsKeyMaterial pins age's own property, which this
// package relies on: a malformed identity file is reported without the line
// that failed to parse. The preflight is where that error must surface.
func TestIdentityErrorOmitsKeyMaterial(t *testing.T) {
	const secret = "AGE-SECRET-KEY-1THISISNOTAVALIDKEYBUTLOOKSLIKEONEWITHMATERIAL"
	path := filepath.Join(t.TempDir(), "identity.txt")
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := jsontext.Value(`{"recipients": ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], "identities": [{"file": "` + path + `"}]}`)
	p, err := plugin.Build(context.Background(), testRegistry(t), processor.Kind, "age", raw,
		plugin.Deps{Instance: "age", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("construction must not touch the filesystem, so it may not fail here: %v", err)
	}
	checker, ok := p.(processor.DecodeChecker)
	if !ok {
		t.Fatal("the age processor does not implement processor.DecodeChecker, so a bad identity file would only surface mid-restore")
	}
	err = checker.CheckDecode(context.Background())
	if err == nil {
		t.Fatal("a malformed identity file passed the preflight")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("the error echoes private key material: %v", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("the error does not name the identity file, so it cannot be fixed: %v", err)
	}
}

func TestMissingIdentityFile(t *testing.T) {
	raw := jsontext.Value(`{"recipients": [], "identities": [{"file": "/nonexistent/identity.txt"}]}`)
	p, err := plugin.Build(context.Background(), testRegistry(t), processor.Kind, "age", raw,
		plugin.Deps{Instance: "age", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	err = p.(processor.DecodeChecker).CheckDecode(context.Background())
	if err == nil {
		t.Fatal("a missing identity file passed the restore-side check")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("missing identity file is %q, want permanent", class)
	}
	// And a decode attempted without a preflight fails just as loudly.
	if err := p.Decode(context.Background(), &bytes.Buffer{}, strings.NewReader("x")); err == nil {
		t.Fatal("Decode with a missing identity file succeeded")
	}
}

func TestConfigValidation(t *testing.T) {
	r := testRegistry(t)
	cases := map[string]string{
		"neither direction": `{"recipients": [], "identities": []}`,
		"bad recipient":     `{"recipients": ["not-a-key"], "identities": []}`,
		"empty recipient":   `{"recipients": [""], "identities": []}`,
		"empty identity":    `{"recipients": [], "identities": [{"file": ""}]}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("processor", "age", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
}

// TestBuildRefusesAMalformedRecipient: a public key that does not parse is
// refused before the processor exists, so `validate` never has to build one to
// report it.
func TestBuildRefusesAMalformedRecipient(t *testing.T) {
	_, err := plugin.Build(context.Background(), testRegistry(t), processor.Kind, "age",
		jsontext.Value(`{"recipients": ["not-a-key"], "identities": []}`),
		plugin.Deps{Instance: "age", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("a malformed recipient was accepted")
	}
	if !strings.Contains(err.Error(), "recipients/0") {
		t.Errorf("the error does not name the offending index: %v", err)
	}
}

// TestBuildReturnsOnCancelledContext: construction owes a cancelled context the
// same prompt return as every other call.
func TestBuildReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := plugin.Build(ctx, testRegistry(t), processor.Kind, "age",
		jsontext.Value(`{"recipients": ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], "identities": []}`),
		plugin.Deps{Instance: "age", Log: slog.New(slog.DiscardHandler)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Build with a cancelled context returned %v, want context.Canceled", err)
	}
}

// TestCheckEncode: the encode-side preflight refuses an instance that holds no
// recipient — a configuration fault the run must not discover one artifact at a
// time.
func TestCheckEncode(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sealer := build(t, []string{identity.Recipient().String()}, "").(processor.EncodeChecker)
	if err := sealer.CheckEncode(ctx); err != nil {
		t.Fatalf("CheckEncode on a sealing instance: %v", err)
	}
	err = build(t, nil, writeIdentities(t, identity)).(processor.EncodeChecker).CheckEncode(ctx)
	if err == nil {
		t.Fatal("CheckEncode passed an instance with no recipients")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("no recipients is %q, want permanent", class)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := sealer.CheckEncode(cancelled); err == nil {
		t.Fatal("CheckEncode ignored a cancelled context")
	}
}

// TestCheckDecode: the restore-side preflight reads every identity file, and an
// instance with none has nothing to check.
func TestCheckDecode(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	reader := build(t, nil, writeIdentities(t, identity)).(processor.DecodeChecker)
	if err := reader.CheckDecode(ctx); err != nil {
		t.Fatalf("CheckDecode on a readable identity file: %v", err)
	}
	if err := build(t, []string{identity.Recipient().String()}, "").(processor.DecodeChecker).CheckDecode(ctx); err != nil {
		t.Fatalf("CheckDecode with no identities: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := reader.CheckDecode(cancelled); err == nil {
		t.Fatal("CheckDecode ignored a cancelled context")
	}
}

func TestDescribeReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := build(t, []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"}, "").Describe(ctx); err == nil {
		t.Fatal("Describe ignored a cancelled context")
	}
}

// cancelAtEnd hands over every byte, then cancels the context and reports the
// cancellation: a copy that reaches the end of its input just as the context
// ends, so the plugin's own check — not its entry guard — is what returns.
type cancelAtEnd struct {
	ctx    context.Context
	cancel context.CancelFunc
	data   []byte
	off    int
}

func (c *cancelAtEnd) Read(p []byte) (int, error) {
	if c.off < len(c.data) {
		n := copy(p, c.data[c.off:])
		c.off += n
		return n, nil
	}
	c.cancel()
	return 0, c.ctx.Err()
}

func TestEncodeReturnsOnCancellationMidStream(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	p := build(t, []string{identity.Recipient().String()}, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &cancelAtEnd{ctx: ctx, cancel: cancel, data: bytes.Repeat([]byte("payload "), 100_000)}
	if err := p.Encode(ctx, io.Discard, src); !errors.Is(err, context.Canceled) {
		t.Fatalf("Encode cancelled mid-stream returned %v, want context.Canceled", err)
	}
}

func TestDecodeReturnsOnCancellationMidStream(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	p := build(t, []string{identity.Recipient().String()}, writeIdentities(t, identity))
	var sealed bytes.Buffer
	if err := p.Encode(context.Background(), &sealed, bytes.NewReader(bytes.Repeat([]byte("payload "), 100_000))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &cancelAtEnd{ctx: ctx, cancel: cancel, data: sealed.Bytes()}
	if err := p.Decode(ctx, io.Discard, src); !errors.Is(err, context.Canceled) {
		t.Fatalf("Decode cancelled mid-stream returned %v, want context.Canceled", err)
	}
}

// failAfter accepts writes while the cumulative count stays within limit, then
// refuses: a destination that dies on the final flush.
type failAfter struct {
	limit   int
	written int
}

func (w *failAfter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, errors.New("destination refused the write")
	}
	w.written += len(p)
	return len(p), nil
}

// TestEncodeCloseErrorIsPermanent: Close writes the authentication tag, so a
// destination that refuses only that last write must fail the encode — a
// truncated ciphertext no identity can open must never read as success.
func TestEncodeCloseErrorIsPermanent(t *testing.T) {
	identity, err := agecrypto.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	p := build(t, []string{identity.Recipient().String()}, "")
	plain := []byte("payload payload payload")
	var full bytes.Buffer
	if err := p.Encode(context.Background(), &full, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	w := &failAfter{limit: full.Len() - 1}
	err = p.Encode(context.Background(), w, bytes.NewReader(plain))
	if err == nil {
		t.Fatal("Encode reported success although the destination refused the final write")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("a failing destination is %q, want permanent", class)
	}
}
