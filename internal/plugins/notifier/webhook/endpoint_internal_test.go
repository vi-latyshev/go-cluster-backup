package webhook

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
)

// TestEndpointFuncsPublishesTheSecretName: the exported set is the deployment-
// visible surface the framework's function pin reads. Execution never uses it
// directly — parseEndpoint binds the same name to the allow-list reader — but a
// rename or a second function would break a config file, so the name is proven
// here beside the implementation.
func TestEndpointFuncsPublishesTheSecretName(t *testing.T) {
	if _, ok := EndpointFuncs()["secret"]; !ok {
		t.Fatal("the published endpoint function set carries no secret reader")
	}
}

// TestSecretRefusesARelativePathWithoutEchoingIt: the runtime reader is the
// second line behind the parse-time literal rule. A hand-built template could
// hand it computed bytes, and those bytes may be a secret, so the refusal names
// neither the argument nor a file.
func TestSecretRefusesARelativePathWithoutEchoingIt(t *testing.T) {
	_, err := funcSecret("SECRET-SENTINEL")
	if err == nil {
		t.Fatal("a relative secret path was accepted")
	}
	if strings.Contains(err.Error(), "SECRET-SENTINEL") {
		t.Fatalf("the computed path reached the error: %v", err)
	}
}

// TestBoundSecretRefusesAPathOutsideTheLiteralSet: the runtime set is the
// allow-list the walk collected. A tree that never went through the walk — the
// hand-built case here — cannot open a path the source did not declare, and the
// refusal does not echo the argument.
func TestBoundSecretRefusesAPathOutsideTheLiteralSet(t *testing.T) {
	allowed := map[string]struct{}{}
	tmpl, err := template.New("url").Option("missingkey=error").
		Funcs(endpointSecretFuncs(allowed)).Parse(`{{ secret "/SECRET-SENTINEL" }}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = renderEndpoint(tmpl, "url")
	if err == nil {
		t.Fatal("a path outside the literal set was opened")
	}
	if strings.Contains(err.Error(), "SECRET-SENTINEL") {
		t.Fatalf("the undeclared path reached the error: %v", err)
	}
	var se *secretReadError
	if !errors.As(err, &se) {
		t.Fatalf("the refusal is not the secret reader's: %v", err)
	}
}

// TestLiteralSecretPathsCoversEveryNodeForm: the allow-list walk must follow
// every place the template grammar can hold a call — the body of if, range and
// with, their else branches, a template invocation's own arguments, a
// parenthesized pipeline, a chain's own node, a variable assignment — or a
// computed secret path could hide in one of them. Every refusal stays free of
// the computed value.
func TestLiteralSecretPathsCoversEveryNodeForm(t *testing.T) {
	valid := []string{
		`{{ secret "/run/token" }}`,
		`{{ if true }}{{ secret "/run/token" }}{{ end }}`,
		`{{ if false }}{{ else }}{{ secret "/run/token" }}{{ end }}`,
		`{{ range . }}{{ secret "/run/token" }}{{ end }}`,
		`{{ with . }}{{ secret "/run/token" }}{{ end }}`,
		`{{ define "x" }}{{ . }}{{ end }}{{ template "x" (secret "/run/token") }}`,
		`{{ define "x" }}{{ . }}{{ end }}{{ template "x" }}`,
		`{{ secret "/run/token" | printf "%s" }}`,
		`{{ printf "%s" . }}`,
	}
	for _, src := range valid {
		parsed, err := parseEndpoint("url", src, true)
		if err != nil {
			t.Errorf("%s: a literal secret call was refused: %v", src, err)
			continue
		}
		if parsed.tmpl == nil {
			t.Errorf("%s: parsed to nil", src)
		}
	}
	invalid := []string{
		// Data references: the endpoint set runs with no data, so a field can
		// only fail at delivery. Every spelling the grammar offers is refused.
		`{{ .X }}`,
		`{{ $.X }}`,
		`{{ $p := secret "/run/token" }}{{ $p.X }}`,
		`{{ $p := . }}{{ $p.X }}`,
		`{{ with . }}{{ .X }}{{ end }}`,
		`{{ if .X }}{{ end }}`,
		`{{ define "d" }}{{ .X }}{{ end }}`,
		`{{ (secret "/run/token").Missing }}`,
		// Computed secret paths.
		`{{ secret (secret "/run/token") }}`,
		`{{ (secret (printf "/%s" (secret "/run/token"))).Missing }}`,
		`{{ secret.X }}`,
		`{{ $p := secret "/run/token" }}{{ secret $p }}`,
		`{{ printf "%s" secret }}`,
		`{{ secret }}`,
		`{{ secret . }}`,
		`{{ "/run/token" | secret }}`,
		`{{ "x" | secret "/run/token" }}`,
		`{{ if secret }}{{ end }}`,
		`{{ if true }}{{ secret . }}{{ end }}`,
		`{{ printf "%s" (secret .) }}`,
	}
	for _, src := range invalid {
		if _, err := parseEndpoint("url", src, true); err == nil {
			t.Errorf("%s: a computed secret path was accepted", src)
		}
	}
	if _, err := parseEndpoint("url", `{{ secret "relative/token" }}`, true); err == nil ||
		!strings.Contains(err.Error(), `"relative/token"`) {
		t.Errorf("a literal relative path = %v, want a refusal naming the config's own text", err)
	}
}

// TestStubSecretReadsNothing: Validate parses with a function that answers an
// empty value, so a parse bug can never turn validation into a file read.
func TestStubSecretReadsNothing(t *testing.T) {
	secret, ok := stubFuncs()["secret"].(func(string) (string, error))
	if !ok {
		t.Fatal("the stub function set has no secret reader")
	}
	if got, err := secret("/etc/shadow"); got != "" || err != nil {
		t.Fatalf("the offline stub answered %q, %v; it must read nothing", got, err)
	}
}

// TestRenderURLRefusesARenderedAuthorityChange: the offline rule keeps every
// action out of the authority, so validation refuses such a source already; the
// render still re-parses what came out and compares it with the literal
// endpoint. This is the second line: a template built without Validate — a
// future caller, a bug — still cannot post the run's report to a host a secret
// chose.
func TestRenderURLRefusesARenderedAuthorityChange(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "endpoint-secret")
	if err := os.WriteFile(file, []byte("@169.254.169.254/"), 0o600); err != nil {
		t.Fatal(err)
	}
	whole, err := parseEndpoint("url", `{{ secret "`+file+`" }}`, true)
	if err != nil {
		t.Fatal(err)
	}
	e := &endpoint{url: whole.tmpl, scheme: "https", host: "hooks.example.com"}
	if _, err := e.renderURL(); err == nil || !strings.Contains(err.Error(), "changed the literal scheme, host or port") {
		t.Fatalf("a render that replaced the authority = %v, want a refusal", err)
	}

	static, err := parseEndpoint("url", "https://user:pass@hooks.example.com/x", true)
	if err != nil {
		t.Fatal(err)
	}
	e = &endpoint{url: static.tmpl, scheme: "https", host: "hooks.example.com"}
	if _, err := e.renderURL(); err == nil || !strings.Contains(err.Error(), "carries userinfo") {
		t.Fatalf("a rendered url with userinfo = %v, want a refusal", err)
	}
}
