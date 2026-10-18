package kuberneteslabels

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
)

// TestTemplateFuncsWithoutAnObject: every rule function answers through the
// object the render bound, and one called outside a render says so instead of
// reading a nil object. TemplateFuncs is the surface the framework parses a
// config's templates with, so the refusal is the function's own contract.
func TestTemplateFuncsWithoutAnObject(t *testing.T) {
	funcs := (&Config{}).TemplateFuncs()
	calls := map[string]func() error{
		"label": func() error {
			_, err := funcs["label"].(func(string) (string, error))("backup.example/type")
			return err
		},
		"envValue": func() error {
			_, err := funcs["envValue"].(func(int, string) (string, error))(0, "PW")
			return err
		},
		"envFile": func() error {
			_, err := funcs["envFile"].(func(int, string) (string, error))(0, "PW")
			return err
		},
		"port": func() error {
			_, err := funcs["port"].(func(int, int) (string, error))(0, 0)
			return err
		},
		"volumePath": func() error {
			_, err := funcs["volumePath"].(func() (string, error))()
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil || !strings.Contains(err.Error(), "no object is bound to this render") {
				t.Fatalf("%s = %v, want the unbound-render refusal", name, err)
			}
		})
	}
}

// TestParserRefusesMalformedInput: parseSourceConfig and parseNode walk the
// bytes of a source config, and every way those bytes can fail to be the JSON
// they claim to be is an error naming the path — never a panic and never a
// silently half-parsed config. The strict decode refuses such a document before
// a plugin sees it, which is why these are reached from here rather than
// through a config file.
func TestParserRefusesMalformedInput(t *testing.T) {
	funcs := template.FuncMap{}
	cases := []struct {
		name string
		raw  jsontext.Value
		want string
	}{
		{"an absent config", nil, "/c is empty"},
		{"a whitespace-only config", jsontext.Value("   "), "/c is empty"},
		{"an unterminated object", jsontext.Value("{"), "/c: "},
		{"an unterminated array", jsontext.Value("[1,"), "/c: "},
		{"an unterminated string", jsontext.Value(`"abc`), "/c: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSourceConfig(tc.raw, funcs, "/c")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseSourceConfig(%q) = %v, want a message containing %q", tc.raw, err, tc.want)
			}
		})
	}

	// A nested value that is not valid JSON fails at its own path, so a reader
	// is pointed at the member that carries it.
	if _, err := parseNode(jsontext.Value(`{"a": {`), "/c", funcs); err == nil || !strings.Contains(err.Error(), `"/a"`) {
		t.Fatalf("parseNode = %v, want the nested path named", err)
	}
	if _, err := parseNode(jsontext.Value(`[`), "/c", funcs); err == nil || !strings.Contains(err.Error(), "/c") {
		t.Fatalf("parseNode = %v, want the array's own path named", err)
	}
}

// TestSecretValueThatIsNotBase64: the API stores a Secret's values as base64,
// and a value that is not decodable is refused by name instead of reaching a
// source as garbage. The fake API server always encodes, so the refusal is
// exercised against a server that answers the way a broken one would.
func TestSecretValueThatIsNotBase64(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"kind":"Secret","apiVersion":"v1","metadata":{"name":"cred","namespace":"ns"},"data":{"PW":"!!!not-base64!!!"}}`)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, ca, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	const hostEnv, portEnv = "GCB_TEST_INTERNAL_HOST", "GCB_TEST_INTERNAL_PORT"
	t.Setenv(hostEnv, u.Hostname())
	t.Setenv(portEnv, u.Port())

	st := &state{ctx: context.Background(), opts: kubeapi.Options{
		HostEnv: hostEnv, PortEnv: portEnv, TokenFile: tokenPath, CAFile: caPath, Timeout: 10 * time.Second,
	}}
	if err := st.clientFor(); err != nil {
		t.Fatal(err)
	}
	o := object{resource: "pod", namespace: "ns", name: "db-0"}
	e := kubeapi.EnvVar{Name: "PW", ValueFrom: map[string]jsontext.Value{
		"secretKeyRef": jsontext.Value(`{"name":"cred","key":"PW"}`),
	}}
	_, err = st.fromEnvVar(o, 0, e)
	if err == nil || !strings.Contains(err.Error(), `secret ns/cred: key "PW" is not valid base64`) {
		t.Fatalf("fromEnvVar = %v, want the undecodable value refused", err)
	}
}
