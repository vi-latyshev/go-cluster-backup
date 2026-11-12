package kuberneteslabels

import (
	"context"
	"encoding/pem"
	"errors"
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

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
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

// TestAffinityMatchesEvaluatesEveryOperator: a required node affinity is read
// with the API's own operators, and anything this build cannot evaluate matches
// no node — an affinity that cannot be read must never read as compatible.
func TestAffinityMatchesEvaluatesEveryOperator(t *testing.T) {
	node := nodeInfo{name: "node-a", labels: map[string]string{
		"kubernetes.io/hostname": "node-a",
		"disk":                   "ssd",
		"cpu":                    "8",
	}}
	req := func(key, op string, values ...string) kubeapi.NodeSelectorRequirement {
		return kubeapi.NodeSelectorRequirement{Key: key, Operator: op, Values: values}
	}
	terms := func(exprs ...kubeapi.NodeSelectorRequirement) []kubeapi.NodeSelectorTerm {
		return []kubeapi.NodeSelectorTerm{{MatchExpressions: exprs}}
	}
	cases := []struct {
		name  string
		terms []kubeapi.NodeSelectorTerm
		want  bool
	}{
		{"In holds", terms(req("kubernetes.io/hostname", "In", "node-a")), true},
		{"In with another value", terms(req("kubernetes.io/hostname", "In", "node-b")), false},
		{"In on an absent key matches nothing", terms(req("gpu", "In", "a100")), false},
		{"NotIn holds", terms(req("disk", "NotIn", "hdd")), true},
		{"NotIn on an absent key holds", terms(req("gpu", "NotIn", "a100")), true},
		{"NotIn with the node's value", terms(req("disk", "NotIn", "ssd")), false},
		{"Exists holds", terms(req("disk", "Exists")), true},
		{"Exists on an absent key", terms(req("gpu", "Exists")), false},
		{"DoesNotExist holds", terms(req("gpu", "DoesNotExist")), true},
		{"DoesNotExist on a present key", terms(req("disk", "DoesNotExist")), false},
		{"Gt holds", terms(req("cpu", "Gt", "4")), true},
		{"Gt fails", terms(req("cpu", "Gt", "9")), false},
		{"Lt holds", terms(req("cpu", "Lt", "16")), true},
		{"Lt fails", terms(req("cpu", "Lt", "4")), false},
		{"Gt on a non-integer value", terms(req("disk", "Gt", "4")), false},
		{"Gt with two operands", terms(req("cpu", "Gt", "1", "2")), false},
		{"Gt with a non-integer operand", terms(req("cpu", "Gt", "four")), false},
		{"an unknown operator matches nothing", terms(req("disk", "Between", "ssd")), false},
		{"every expression in one term must hold",
			terms(req("kubernetes.io/hostname", "In", "node-a"), req("disk", "In", "hdd")), false},
		{"terms are ORed", append(terms(req("disk", "In", "hdd")), kubeapi.NodeSelectorTerm{
			MatchExpressions: []kubeapi.NodeSelectorRequirement{req("kubernetes.io/hostname", "In", "node-a")},
		}), true},
		{"an empty term matches nothing", []kubeapi.NodeSelectorTerm{{}}, false},
		{"no terms match nothing", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := affinityMatches(tc.terms, node); got != tc.want {
				t.Fatalf("affinityMatches = %t, want %t", got, tc.want)
			}
		})
	}

	// MatchFields names the node's own field — metadata.name — and nothing
	// else: a field a node does not have can never prove compatibility.
	fields := func(key, op string, values ...string) []kubeapi.NodeSelectorTerm {
		return []kubeapi.NodeSelectorTerm{{MatchFields: []kubeapi.NodeSelectorRequirement{req(key, op, values...)}}}
	}
	for _, tc := range []struct {
		name  string
		terms []kubeapi.NodeSelectorTerm
		want  bool
	}{
		{"metadata.name In holds", fields("metadata.name", "In", "node-a"), true},
		{"metadata.name In with another name", fields("metadata.name", "In", "node-b"), false},
		{"metadata.name NotIn holds", fields("metadata.name", "NotIn", "node-b"), true},
		{"an unsupported field matches nothing", fields("spec.providerID", "Exists"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := affinityMatches(tc.terms, node); got != tc.want {
				t.Fatalf("affinityMatches = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestNodeByProvesTheAnsweredIdentityAndCachesIt: the API answering for a
// different node than the one asked is a refusal — the name is the whole proof
// — and the failed read is shared by every later ask of that collect.
func TestNodeByProvesTheAnsweredIdentityAndCachesIt(t *testing.T) {
	requests := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/nodes/") {
			http.NotFound(w, r)
			return
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"metadata":{"name":"node-z"}}`)
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
	for range 2 {
		if _, err := st.nodeBy(context.Background(), "node-a"); err == nil ||
			!strings.Contains(err.Error(), `the API answered node "node-z" for the request of node "node-a"`) {
			t.Fatalf("nodeBy = %v, want the mismatched identity refused", err)
		}
	}
	if requests != 1 {
		t.Fatalf("the API was asked %d times, want the failed read cached", requests)
	}
}

// TestMountingNodesWithoutACollect: a state outside a collect carries no
// pod evidence, and asking for mounting nodes answers nothing instead of
// dereferencing the books a collect owns.
func TestMountingNodesWithoutACollect(t *testing.T) {
	if nodes := (&state{}).mountingNodes(object{namespace: "ns", name: "media"}); nodes != nil {
		t.Fatalf("mountingNodes = %v, want none", nodes)
	}
}

// TestAffinityOnlyNodeProvesExclusivePlacement: outside node scope there is no
// declared set to compare with, so an affinity authorizes this node's local
// read only when every alternative pins the identity to this node by a
// single-value metadata.name or kubernetes.io/hostname requirement.
func TestAffinityOnlyNodeProvesExclusivePlacement(t *testing.T) {
	node := nodeInfo{name: "node-a", labels: map[string]string{
		"kubernetes.io/hostname": "node-a",
		"cpu":                    "8",
	}}
	req := func(key, op string, values ...string) kubeapi.NodeSelectorRequirement {
		return kubeapi.NodeSelectorRequirement{Key: key, Operator: op, Values: values}
	}
	expr := func(reqs ...kubeapi.NodeSelectorRequirement) kubeapi.NodeSelectorTerm {
		return kubeapi.NodeSelectorTerm{MatchExpressions: reqs}
	}
	field := func(reqs ...kubeapi.NodeSelectorRequirement) kubeapi.NodeSelectorTerm {
		return kubeapi.NodeSelectorTerm{MatchFields: reqs}
	}
	cases := []struct {
		name  string
		terms []kubeapi.NodeSelectorTerm
		want  bool
	}{
		{"no terms prove nothing", nil, false},
		{"a single hostname value pins the node", []kubeapi.NodeSelectorTerm{
			expr(req("kubernetes.io/hostname", "In", "node-a"))}, true},
		{"a single metadata.name value pins the node", []kubeapi.NodeSelectorTerm{
			field(req("metadata.name", "In", "node-a"))}, true},
		{"extra filters beside the pin do not widen it", []kubeapi.NodeSelectorTerm{
			expr(req("kubernetes.io/hostname", "In", "node-a"), req("cpu", "Gt", "4"))}, true},
		{"several hostname values admit other nodes", []kubeapi.NodeSelectorTerm{
			expr(req("kubernetes.io/hostname", "In", "node-a", "node-b"))}, false},
		{"a filter that does not name the node does not pin it", []kubeapi.NodeSelectorTerm{
			expr(req("cpu", "Gt", "4"))}, false},
		{"a second term pinned elsewhere admits another node", []kubeapi.NodeSelectorTerm{
			expr(req("kubernetes.io/hostname", "In", "node-a")),
			field(req("metadata.name", "In", "node-b"))}, false},
		{"a second term naming a hostname this node does not carry", []kubeapi.NodeSelectorTerm{
			expr(req("metadata.name", "In", "node-a")),
			expr(req("kubernetes.io/hostname", "In", "node-z"))}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := affinityOnlyNode(tc.terms, node); got != tc.want {
				t.Fatalf("affinityOnlyNode = %t, want %t", got, tc.want)
			}
		})
	}

	// A pin on kubernetes.io/hostname proves exclusion only when the node
	// object itself carries that value: an unlabelled node proves nothing.
	unlabelled := nodeInfo{name: "node-a"}
	if affinityOnlyNode([]kubeapi.NodeSelectorTerm{
		field(req("metadata.name", "In", "node-a")),
		expr(req("kubernetes.io/hostname", "In", "node-a")),
	}, unlabelled) {
		t.Fatal("an absent hostname label was read as the node's own value")
	}

	// The selector namespaces are never merged: metadata.name as a
	// matchExpression is an ordinary label, however this node is labelled.
	labelled := nodeInfo{name: "node-a", labels: map[string]string{"metadata.name": "node-a"}}
	if affinityOnlyNode([]kubeapi.NodeSelectorTerm{
		expr(req("metadata.name", "In", "node-a")),
	}, labelled) {
		t.Fatal("a shared metadata.name label was read as the node's identity")
	}
	// ...and matchFields never name the hostname label.
	if affinityOnlyNode([]kubeapi.NodeSelectorTerm{
		field(req("kubernetes.io/hostname", "In", "node-a")),
	}, node) {
		t.Fatal("a matchFields hostname requirement was read as identity")
	}
	// An empty single value is not an identity: every node with an empty
	// label would share it.
	emptyHostname := nodeInfo{name: "node-a", labels: map[string]string{"kubernetes.io/hostname": ""}}
	if affinityOnlyNode([]kubeapi.NodeSelectorTerm{
		expr(req("kubernetes.io/hostname", "In", "")),
	}, emptyHostname) {
		t.Fatal("an empty hostname value was read as identity")
	}
	// A genuine field pin beside a hostname filter still forces the name.
	mixed := nodeInfo{name: "node-a", labels: map[string]string{"kubernetes.io/hostname": "node-z"}}
	if !affinityOnlyNode([]kubeapi.NodeSelectorTerm{{
		MatchFields:      []kubeapi.NodeSelectorRequirement{req("metadata.name", "In", "node-a")},
		MatchExpressions: []kubeapi.NodeSelectorRequirement{req("kubernetes.io/hostname", "In", "node-z")},
	}}, mixed) {
		t.Fatal("a genuine metadata.name field pin was not read as identity")
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

// TestProofErrorKeepsEveryClass: the helper that wraps a node read keeps the
// API's own class — 403 permanent with the remedy, 404 not-found, a timeout or
// 5xx transient with the retry wording — and marks only a cause with no class
// of its own permanent, because a retry would ask the same question.
func TestProofErrorKeepsEveryClass(t *testing.T) {
	const remedy = "grant get on nodes"
	cases := []struct {
		name    string
		cause   error
		class   pluginerrors.Class
		remedy  bool
		retried bool
	}{
		{"permanent", pluginerrors.Permanent(errors.New("denied")), pluginerrors.ClassPermanent, true, false},
		{"transient", pluginerrors.Transient(errors.New("503")), pluginerrors.ClassTransient, false, true},
		{"not found", pluginerrors.NotFound(errors.New("missing")), pluginerrors.ClassNotFound, false, false},
		{"unclassified", errors.New("mismatch"), pluginerrors.ClassPermanent, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := proofError(tc.cause, "cannot prove the node", remedy)
			if class, _ := pluginerrors.ClassOf(err); class != tc.class {
				t.Errorf("class = %q, want %q: %v", class, tc.class, err)
			}
			if got := strings.Contains(err.Error(), remedy); got != tc.remedy {
				t.Errorf("remedy present = %t, want %t: %v", got, tc.remedy, err)
			}
			if got := strings.Contains(err.Error(), "read again"); got != tc.retried {
				t.Errorf("retry wording present = %t, want %t: %v", got, tc.retried, err)
			}
		})
	}
}
