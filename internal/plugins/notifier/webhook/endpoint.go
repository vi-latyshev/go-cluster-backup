package webhook

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
)

// maxEndpointSecretBytes is the most one endpoint secret file may carry. An
// endpoint value is a URL fragment or a header value, never a document: a
// larger file is a wrong mount, and refusing it keeps a runaway file from
// becoming the request.
const maxEndpointSecretBytes = 4096

// EndpointFuncs is the function set of the endpoint templates — the url and
// every header value, and only those, when endpoint_templates is true. It is
// deliberately not Config.TemplateFuncs: a message or payload template can
// never read a secret file.
//
//	secret PATH -> the contents of the mounted secret file at the absolute PATH,
//	               at most 4096 bytes, a trailing CR/LF trimmed
func EndpointFuncs() template.FuncMap {
	return template.FuncMap{"secret": funcSecret}
}

// secretReadError marks one secret file a render could not read, so the render
// site can name the file without echoing the template executor's own text.
type secretReadError struct{ err error }

func (e *secretReadError) Error() string { return e.err.Error() }

func funcSecret(path string) (string, error) {
	if !filepath.IsAbs(path) {
		// The argument is never quoted: this function may only run on a path
		// the parse-time rule proved literal, but a hand-built template could
		// hand it computed bytes, and those bytes may be a secret.
		return "", &secretReadError{err: errors.New("the secret path must be absolute")}
	}
	v, err := extcmd.ReadSecretFile(path, maxEndpointSecretBytes)
	if err != nil {
		return "", &secretReadError{err: err}
	}
	return v, nil
}

// endpoint is one notifier's endpoint templates. A nil *endpoint is the literal
// behaviour: the url and the headers are used exactly as the config wrote them.
type endpoint struct {
	url     *template.Template
	headers map[string]*template.Template
	// scheme and host are the literal authority of the url template: no render
	// may move the request to another endpoint.
	scheme string
	host   string
}

// newEndpoint prepares the endpoint of one notifier. It runs after Validate,
// which parsed the same sources and enforced the same allow-list, so a parse
// failure here is impossible and must.Do says so.
func newEndpoint(cfg Config) *endpoint {
	if !cfg.EndpointTemplates {
		return nil
	}
	auth := must.Do(literalAuthority(cfg.URL))
	ep := &endpoint{
		url:     must.Do(parseEndpoint("url", cfg.URL, true)).tmpl,
		headers: make(map[string]*template.Template, len(cfg.Headers)),
		scheme:  auth.scheme,
		host:    auth.host,
	}
	for name, src := range cfg.Headers {
		ep.headers[name] = must.Do(parseEndpoint("headers/"+name, src, true)).tmpl
	}
	return ep
}

// parsedEndpoint is one endpoint field ready for execution: the tree and the
// literal paths its secret calls declared, which are the only paths its reader
// will ever open.
type parsedEndpoint struct {
	tmpl    *template.Template
	allowed map[string]struct{}
}

// parseEndpoint parses one endpoint field and enforces the secret allow-list.
// read=false is Validate's stub that reads nothing, because validating a config
// is offline; read=true binds the runtime reader to the collected set. The
// parsed call graph must satisfy the literal-path rule, so a value a secret
// produced can never become another secret's path — nor an error's text.
func parseEndpoint(name, src string, read bool) (parsedEndpoint, error) {
	allowed := make(map[string]struct{})
	funcs := stubFuncs()
	if read {
		funcs = endpointSecretFuncs(allowed)
	}
	tmpl, err := template.New(name).Option("missingkey=error").Funcs(funcs).Parse(src)
	if err != nil {
		return parsedEndpoint{}, fmt.Errorf("%s: the endpoint template is not valid: %w", name, err)
	}
	if err := literalSecretPaths(tmpl, allowed); err != nil {
		return parsedEndpoint{}, fmt.Errorf("%s: %w", name, err)
	}
	// The endpoint executes with no data; the checker walks every definition,
	// called or not, and refuses any field or method read on the dot or on $.
	if err := configuration.CheckTemplate(tmpl, nil, funcs); err != nil {
		return parsedEndpoint{}, fmt.Errorf("%s: %w", name, err)
	}
	return parsedEndpoint{tmpl: tmpl, allowed: allowed}, nil
}

// endpointSecretFuncs is the runtime function set of one endpoint: secret opens
// only a path the parse-time walk collected from this very source. That is the
// second line behind the walk — a hand-built tree, or a grammar form the walk
// does not know, cannot turn a file's contents into another file's path. The
// refusal never echoes the argument: it may be a secret's bytes.
func endpointSecretFuncs(allowed map[string]struct{}) template.FuncMap {
	return template.FuncMap{"secret": func(path string) (string, error) {
		if _, ok := allowed[path]; !ok {
			return "", &secretReadError{err: errors.New("the secret path is not a literal this template declares")}
		}
		return funcSecret(path)
	}}
}

// errSecretArgument refuses a computed secret path. It never quotes the value:
// a computed argument can be a secret's own bytes, and diagnostics must not
// carry them.
var errSecretArgument = errors.New("secret takes one literal absolute path; a computed value could put secret bytes into diagnostics")

// literalSecretPaths enforces the one safe form of the secret function and
// collects its allow-list: every call is a command whose sole argument is a
// string literal absolute path, that path joins allowed, and the identifier in
// any other position — a nested pipeline, a chain, a variable, a bare argument
// — is refused. The walk follows every node type the grammar can nest: action
// and command pipelines, if/range/with including their else branches, template
// invocations and the bodies of define, and ChainNode's own node, which is how
// `(secret (…)).Missing` used to hide a computed call. A literal path is the
// config's own text, which is safe to name.
func literalSecretPaths(tmpl *template.Template, allowed map[string]struct{}) error {
	for _, t := range tmpl.Templates() {
		if err := literalSecretNodes(t.Root, allowed); err != nil {
			return err
		}
	}
	return nil
}

// literalSecretNodes walks one node and its children, whatever the grammar
// form. A nil child is a typed nil the caller passes deliberately: an if with
// no else, a template invocation with no arguments.
func literalSecretNodes(n parse.Node, allowed map[string]struct{}) error {
	switch t := n.(type) {
	case *parse.ListNode:
		if t == nil {
			return nil
		}
		for _, c := range t.Nodes {
			if err := literalSecretNodes(c, allowed); err != nil {
				return err
			}
		}
	case *parse.ActionNode:
		return literalSecretNodes(t.Pipe, allowed)
	case *parse.PipeNode:
		if t == nil {
			return nil
		}
		// A command after the first one receives the previous command's output
		// as one more argument, so a secret call there is never the checked
		// one-argument form even when its own literal looks right:
		// `{{ "x" | secret "/run/token" }}` is two arguments.
		for i, c := range t.Cmds {
			if err := literalSecretCommand(c, i > 0, allowed); err != nil {
				return err
			}
		}
	case *parse.IfNode:
		return literalSecretBranch(t.Pipe, t.List, t.ElseList, allowed)
	case *parse.RangeNode:
		return literalSecretBranch(t.Pipe, t.List, t.ElseList, allowed)
	case *parse.WithNode:
		return literalSecretBranch(t.Pipe, t.List, t.ElseList, allowed)
	case *parse.TemplateNode:
		return literalSecretNodes(t.Pipe, allowed)
	}
	// A bare identifier outside a command's own argument list is not the checked
	// call form: an identifier named secret there has no arguments, and
	// literalSecretCommand refuses it before this walk reaches it.
	return nil
}

// literalSecretCommand checks one command's arguments: a secret call must be the
// command itself, take exactly one string literal, and not be fed by a pipeline.
func literalSecretCommand(c *parse.CommandNode, piped bool, allowed map[string]struct{}) error {
	for i, arg := range c.Args {
		if id, ok := arg.(*parse.IdentifierNode); ok && id.Ident == "secret" {
			if piped || i != 0 || len(c.Args) != 2 {
				return errSecretArgument
			}
			str, ok := c.Args[1].(*parse.StringNode)
			if !ok {
				return errSecretArgument
			}
			if !filepath.IsAbs(str.Text) {
				return fmt.Errorf("the secret path %q must be absolute", str.Text)
			}
			allowed[str.Text] = struct{}{}
			continue
		}
		if err := literalSecretNodes(arg, allowed); err != nil {
			return err
		}
	}
	return nil
}

func literalSecretBranch(pipe *parse.PipeNode, list, elseList *parse.ListNode, allowed map[string]struct{}) error {
	if err := literalSecretNodes(pipe, allowed); err != nil {
		return err
	}
	if err := literalSecretNodes(list, allowed); err != nil {
		return err
	}
	return literalSecretNodes(elseList, allowed)
}

func stubFuncs() template.FuncMap {
	return template.FuncMap{"secret": func(string) (string, error) { return "", nil }}
}

// validateEndpoint checks the endpoint templates offline: every source parses
// with the endpoint function set, and the url's scheme and authority are
// literal and terminated before the first action.
func validateEndpoint(cfg Config) []error {
	if !cfg.EndpointTemplates {
		return nil
	}
	var errs []error
	if _, err := parseEndpoint("url", cfg.URL, false); err != nil {
		errs = append(errs, err)
	} else if _, err := literalAuthority(cfg.URL); err != nil {
		errs = append(errs, err)
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Headers)) {
		if _, err := parseEndpoint("headers/"+name, cfg.Headers[name], false); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// authority is the literal scheme and host one endpoint template may not move.
type authority struct {
	scheme string
	host   string
}

// literalAuthority reads the scheme and host of one endpoint template and
// refuses a source whose authority is not complete in the template's own text:
// the authority must be terminated by /, ? or # before the first action, so a
// secret can land only in the path, the query or the fragment. Without this a
// source like https://hooks.example.com{{ secret "/run/url" }} would let the
// file decide the host — an SSRF hole with a green validation.
func literalAuthority(src string) (authority, error) {
	prefix, hasAction := src, false
	if i := strings.Index(src, "{{"); i >= 0 {
		prefix, hasAction = src[:i], true
	}
	u, err := url.Parse(prefix)
	if err != nil {
		return authority{}, errors.New("url: the literal endpoint before the first template action is not a URL")
	}
	if u.User != nil {
		return authority{}, errors.New("url: userinfo is not part of the endpoint, templated or not")
	}
	if u.Host == "" {
		return authority{}, errors.New("url: the template must name a literal host")
	}
	rest := prefix
	if j := strings.Index(rest, "://"); j >= 0 {
		rest = rest[j+3:]
	}
	if hasAction && !strings.ContainsAny(rest, "/?#") {
		return authority{}, fmt.Errorf("url: the literal scheme and host must end with %q, %q or %q before the first template action", "/", "?", "#")
	}
	return authority{scheme: u.Scheme, host: u.Host}, nil
}

// renderURL renders the url template and proves the render stayed on the
// configured endpoint: the scheme, host and port are the literal ones and no
// userinfo arrived from a secret.
func (e *endpoint) renderURL() (string, error) {
	rendered, err := renderEndpoint(e.url, "url")
	if err != nil {
		return "", err
	}
	u, err := url.Parse(rendered)
	if err != nil {
		return "", errors.New("url: the rendered endpoint is not a URL")
	}
	if u.User != nil {
		return "", errors.New("url: the rendered endpoint carries userinfo")
	}
	if !strings.EqualFold(u.Scheme, e.scheme) || !strings.EqualFold(u.Host, e.host) {
		return "", errors.New("url: the rendered endpoint changed the literal scheme, host or port")
	}
	return rendered, nil
}

// renderHeaders renders every header value and refuses one a header cannot
// carry: a rendered CR, LF or NUL would end the header at the receiver or split
// the request, and a secret file is exactly where such a byte can come from.
func (e *endpoint) renderHeaders() (map[string]string, error) {
	out := make(map[string]string, len(e.headers))
	for name, tmpl := range e.headers {
		v, err := renderEndpoint(tmpl, "headers/"+name)
		if err != nil {
			return nil, err
		}
		if strings.ContainsAny(v, "\r\n\x00") {
			return nil, fmt.Errorf("headers/%s: the rendered value carries a control character a header cannot", name)
		}
		out[name] = v
	}
	return out, nil
}

// renderEndpoint executes one endpoint template. It runs with no data: the
// endpoint set is the secret reader alone. An error from that reader names the
// file and its path; any other failure is reported by stage only, never by the
// executor's text, which can quote what the template rendered.
func renderEndpoint(tmpl *template.Template, name string) (string, error) {
	var b strings.Builder
	if err := tmpl.Execute(&b, nil); err != nil {
		var se *secretReadError
		if errors.As(err, &se) {
			return "", fmt.Errorf("%s: %w", name, se)
		}
		return "", fmt.Errorf("%s: the endpoint template cannot be rendered", name)
	}
	return b.String(), nil
}
