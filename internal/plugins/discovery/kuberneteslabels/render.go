package kuberneteslabels

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// state is the render context shared by every template of one config instance.
// plugin.Decode parses a Template field at decode time with the functions
// TemplateFuncs returns, and the decoded config is copied afterwards — so the
// functions reach the object they resolve against through this shared pointer,
// which TemplateFuncs allocates and New fills in.
type state struct {
	ctx        context.Context
	secretsDir string
	pageSize   int
	exclude    ExcludeConfig
	opts       kubeapi.Options
	client     *kubeapi.Client
	rules      []preparedRule
	log        *slog.Logger
	object     *object
	// volumes is the node path of each PersistentVolume by name — "" for a
	// volume that has none — read once per collect by volumePath. listed says
	// whether that read was attempted and volumeErr what it answered: one
	// attempt per plugin call, so the objects after a failure share the error
	// and its class instead of hammering the API.
	volumes      map[string]string
	volumeListed bool
	volumeErr    error
}

// forgetVolumes drops the previous collect's listing: each collect reads the
// volumes once, so a retry pass sees the cluster as it is now.
func (s *state) forgetVolumes() {
	s.volumes = nil
	s.volumeListed = false
	s.volumeErr = nil
}

// preparedRule is a validated rule with its source config parsed into the
// template tree that renders per object.
type preparedRule struct {
	rule   rule
	source *node
}

func (c *Config) stateOrNew() *state {
	if c.state == nil {
		c.state = &state{}
	}
	return c.state
}

// TemplateFuncs adds the rule functions to every template of this config:
//
//	label      NAME                   -> the object's label NAME
//	envValue   CONTAINER VAR          -> the resolved environment variable
//	envFile    CONTAINER VAR          -> path of a 0600 file holding it
//	port       CONTAINER PORT         -> the declared containerPort
//	volumePath                        -> the node path of the claim's volume
//
// volumePath is the one function a claim rule needs: it answers where the
// claim's data lives on the node, so a file-tree source reads the volume itself
// and not a mount made for this pod. The PersistentVolumes are listed once per
// discovery, and a listing that fails is a problem of the objects that asked
// for a path — never of the discovery, whose other rules must still run.
//
// CONTAINER and PORT are explicit indices into the pod spec, so the positional
// convention of a rule is in its config, not here. The environment forms are
// the ones a pod may use: a literal value, secretKeyRef and configMapKeyRef;
// fieldRef, resourceFieldRef and envFrom are refused with an error naming the
// object and the variable. A missing container, variable or port is an error
// too. A value resolved for envFile is written under secrets_dir and never
// appears in a rendered config, a log line or an error.
func (c *Config) TemplateFuncs() template.FuncMap { return c.stateOrNew().funcs() }

func (s *state) funcs() template.FuncMap {
	return template.FuncMap{
		"label":      s.fnLabel,
		"envValue":   s.fnEnvValue,
		"envFile":    s.fnEnvFile,
		"port":       s.fnPort,
		"volumePath": s.fnVolumePath,
	}
}

// object is one listed resource as the templates see it.
type object struct {
	resource   string // "pod" or "persistentvolumeclaim", for error messages
	namespace  string
	name       string
	labels     map[string]string
	phase      string
	containers []kubeapi.Container
	// claims is a pod's own list: the claims its volumes mount, which its dump
	// covers or its marker excludes.
	claims []string
	// volumeName is a claim's binding: the PersistentVolume its data lives on.
	volumeName string
}

func (o object) id() string { return fmt.Sprintf("%s %s/%s", o.resource, o.namespace, o.name) }

// objectData is the dot of every template: the matched object's identity.
type objectData struct {
	Namespace string
	Name      string
	Labels    map[string]string
	Phase     string
}

func (o object) data() objectData {
	return objectData{Namespace: o.namespace, Name: o.name, Labels: o.labels, Phase: o.phase}
}

func (s *state) current() (object, error) {
	if s.object == nil {
		return object{}, errors.New("no object is bound to this render")
	}
	return *s.object, nil
}

func (s *state) fnLabel(name string) (string, error) {
	o, err := s.current()
	if err != nil {
		return "", err
	}
	value, ok := o.labels[name]
	if !ok {
		return "", fmt.Errorf("%s has no label %q", o.id(), name)
	}
	return value, nil
}

func (s *state) fnEnvValue(container int, name string) (string, error) {
	resolved, err := s.resolveEnv(container, name)
	if err != nil {
		return "", err
	}
	return resolved.value, nil
}

func (s *state) fnEnvFile(container int, name string) (string, error) {
	resolved, err := s.resolveEnv(container, name)
	if err != nil {
		return "", err
	}
	return s.writeSecretFile(resolved)
}

func (s *state) fnPort(container, port int) (string, error) {
	o, err := s.current()
	if err != nil {
		return "", err
	}
	if o.resource != "pod" {
		return "", fmt.Errorf("%s declares no containers, so it declares no ports", o.id())
	}
	if container < 0 || container >= len(o.containers) {
		return "", fmt.Errorf("%s has no container %d (it has %d)", o.id(), container, len(o.containers))
	}
	ports := o.containers[container].Ports
	if port < 0 || port >= len(ports) {
		return "", fmt.Errorf("%s container %d (%s) has no port %d (it has %d)", o.id(), container, o.containers[container].Name, port, len(ports))
	}
	return strconv.Itoa(ports[port].ContainerPort), nil
}

// fnVolumePath answers the path the object's volume lives at on the node: for a
// claim, the directory the PersistentVolume it is bound to stores data in.
// Every refusal names the object — a claim with no binding, a binding without a
// volume, a volume that is not node-local, the function called on a pod — so the
// Problem lands on the artifact that asked and on no other.
func (s *state) fnVolumePath() (string, error) {
	o, err := s.current()
	if err != nil {
		return "", err
	}
	if o.resource != "persistentvolumeclaim" {
		return "", fmt.Errorf("%s declares no volume path; volumePath reads the volume a persistentvolumeclaim is bound to", o.id())
	}
	if o.volumeName == "" {
		return "", fmt.Errorf("%s is bound to no volume (spec.volumeName is empty)", o.id())
	}
	paths, err := s.volumePaths()
	if err != nil {
		return "", err
	}
	path, known := paths[o.volumeName]
	switch {
	case !known:
		return "", plugin.Permanent(fmt.Errorf("%s: the cluster has no persistentvolume named %q", o.id(), o.volumeName))
	case path == "":
		return "", plugin.Permanent(fmt.Errorf("%s: persistentvolume %q holds no spec.local.path and no spec.hostPath.path, so it is not a volume on a node", o.id(), o.volumeName))
	}
	return path, nil
}

// volumePaths lists the cluster's PersistentVolumes once per collect, mapping
// each name to the node path its data lives on — "" when it has none. A failed
// listing is cached with its class: the objects of this collect share the
// failure instead of each making the call the engine counts as one attempt.
func (s *state) volumePaths() (map[string]string, error) {
	if s.volumeListed {
		return s.volumes, s.volumeErr
	}
	s.volumeListed = true
	pvs, err := s.client.ListPVs(s.ctx, kubeapi.ListRequest{PageSize: s.pageSize})
	if err != nil {
		s.volumeErr = fmt.Errorf("reading the persistent volumes: %w", err)
		return nil, s.volumeErr
	}
	paths := make(map[string]string, len(pvs))
	for _, pv := range pvs {
		switch {
		case pv.Spec.Local != nil:
			paths[pv.Metadata.Name] = pv.Spec.Local.Path
		case pv.Spec.HostPath != nil:
			paths[pv.Metadata.Name] = pv.Spec.HostPath.Path
		default:
			paths[pv.Metadata.Name] = ""
		}
	}
	s.volumes = paths
	return paths, nil
}

// envValue is a resolved environment variable and where its file belongs.
type envValue struct {
	value     string
	namespace string
	// file is the path under secrets_dir/<namespace>: the referenced Secret or
	// ConfigMap key, or the literal's own slot.
	file string
}

func (s *state) resolveEnv(container int, name string) (envValue, error) {
	o, err := s.current()
	if err != nil {
		return envValue{}, err
	}
	if o.resource != "pod" {
		return envValue{}, fmt.Errorf("%s declares no containers, so envValue and envFile cannot read one", o.id())
	}
	if container < 0 || container >= len(o.containers) {
		return envValue{}, fmt.Errorf("%s has no container %d (it has %d)", o.id(), container, len(o.containers))
	}
	c := o.containers[container]
	for _, e := range c.Env {
		if e.Name == name {
			return s.fromEnvVar(o, container, e)
		}
	}
	message := fmt.Sprintf("%s container %d (%s) has no environment variable %q", o.id(), container, c.Name, name)
	if len(c.EnvFrom) > 0 {
		// envFrom imports a whole Secret or ConfigMap; the variable may exist
		// at run time and is still not readable here, so say so instead of
		// pretending the variable is absent.
		message += "; envFrom imports are not resolved"
	}
	return envValue{}, errors.New(message)
}

func (s *state) fromEnvVar(o object, container int, e kubeapi.EnvVar) (envValue, error) {
	if e.Value != nil {
		// An explicit literal, empty or not: the pod spec chose it, and no
		// reference is involved. The value belongs to ONE pod's spec, so the pod
		// is part of the slot: two pods of one namespace may state different
		// literals for the same variable, and a namespace-wide slot would let
		// the last render overwrite the first pod's file.
		return envValue{value: *e.Value, namespace: o.namespace, file: "literal/" + o.name + "/" + strconv.Itoa(container) + "-" + e.Name}, nil
	}
	keys := make([]string, 0, len(e.ValueFrom))
	for k := range e.ValueFrom {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) != 1 {
		return envValue{}, fmt.Errorf("%s container %d: environment variable %q has valueFrom sources %v, want exactly one", o.id(), container, e.Name, keys)
	}
	raw := e.ValueFrom[keys[0]]
	switch keys[0] {
	case "secretKeyRef":
		var ref kubeapi.KeySelector
		if err := json.Unmarshal(raw, &ref); err != nil {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q: %w", o.id(), container, e.Name, err)
		}
		if ref.Name == "" || ref.Key == "" {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q names an incomplete secretKeyRef", o.id(), container, e.Name)
		}
		secret, err := s.secret(o, container, e.Name, ref)
		if err != nil {
			return envValue{}, err
		}
		value, found, err := secret.Value(ref.Key)
		if err != nil {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q: %w", o.id(), container, e.Name, err)
		}
		if !found {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q: secret %s/%s has no key %q", o.id(), container, e.Name, o.namespace, ref.Name, ref.Key)
		}
		return envValue{value: value, namespace: o.namespace, file: "secret-" + ref.Name + "/" + ref.Key}, nil
	case "configMapKeyRef":
		var ref kubeapi.KeySelector
		if err := json.Unmarshal(raw, &ref); err != nil {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q: %w", o.id(), container, e.Name, err)
		}
		if ref.Name == "" || ref.Key == "" {
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q names an incomplete configMapKeyRef", o.id(), container, e.Name)
		}
		cm, err := s.configMap(o, container, e.Name, ref)
		if err != nil {
			return envValue{}, err
		}
		value, found := cm.Data[ref.Key]
		if !found {
			if _, binary := cm.BinaryData[ref.Key]; binary {
				return envValue{}, fmt.Errorf("%s container %d: environment variable %q: configmap %s/%s key %q holds binaryData, which is not supported", o.id(), container, e.Name, o.namespace, ref.Name, ref.Key)
			}
			return envValue{}, fmt.Errorf("%s container %d: environment variable %q: configmap %s/%s has no key %q", o.id(), container, e.Name, o.namespace, ref.Name, ref.Key)
		}
		return envValue{value: value, namespace: o.namespace, file: "configmap-" + ref.Name + "/" + ref.Key}, nil
	default:
		return envValue{}, fmt.Errorf("%s container %d: environment variable %q uses valueFrom.%s, which is not supported (supported: value, secretKeyRef, configMapKeyRef)", o.id(), container, e.Name, keys[0])
	}
}

// secret and configMap read one referenced object; both name the object, the
// container and the variable in every error, so a refused read points at the
// rule that asked for it.
func (s *state) secret(o object, container int, variable string, ref kubeapi.KeySelector) (kubeapi.Secret, error) {
	secret, err := s.client.GetSecret(s.ctx, o.namespace, ref.Name)
	if err != nil {
		return kubeapi.Secret{}, fmt.Errorf("%s container %d: environment variable %q: secret %s/%s: %w", o.id(), container, variable, o.namespace, ref.Name, err)
	}
	return secret, nil
}

func (s *state) configMap(o object, container int, variable string, ref kubeapi.KeySelector) (kubeapi.ConfigMap, error) {
	cm, err := s.client.GetConfigMap(s.ctx, o.namespace, ref.Name)
	if err != nil {
		return kubeapi.ConfigMap{}, fmt.Errorf("%s container %d: environment variable %q: configmap %s/%s: %w", o.id(), container, variable, o.namespace, ref.Name, err)
	}
	return cm, nil
}

// clientFor resolves the API client once per discovery. collect calls it before
// any listing or render, so every later use of s.client — the listings, the
// Secret and ConfigMap reads, the volume listing — is of a client that is there.
func (s *state) clientFor() error {
	if s.client != nil {
		return nil
	}
	client, err := kubeapi.New(s.opts)
	if err != nil {
		// Missing environment, an unreadable CA: config, not a transient fault.
		return plugin.Permanent(err)
	}
	s.client = client
	return nil
}

// writeSecretFile writes one resolved value under secrets_dir as a file only
// this process can read and returns its path. The 0600 file mode and the 0700
// directories are a security invariant, not a tunable: there is no config
// field for either, and none may be added.
//
// Its failures are permanent: an unusable secrets path is a deployment fault
// that another attempt answers the same way, and failing only the artifacts
// that need a secret file keeps the rest of the night — a rule that writes
// none (a volume tree) is unaffected.
func (s *state) writeSecretFile(v envValue) (string, error) {
	namespace, err := safeRel(v.namespace)
	if err != nil {
		return "", fmt.Errorf("namespace %q: %w", v.namespace, err)
	}
	rel, err := safeRel(v.file)
	if err != nil {
		return "", err
	}
	path := filepath.Join(s.secretsDir, namespace, rel)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", plugin.Permanent(fmt.Errorf("prepare %s: %w", dir, err))
	}
	// A directory created inside a setgid parent (the tmpfs mounted with fsGroup)
	// inherits the setgid bit on Linux, so the mode is set explicitly: exactly
	// 0700, owner-only, nothing inherited. WriteFile keeps the mode of an
	// existing file, so a file written by an older run is tightened too. The
	// first step that fails is the one reported; each is a permanent fault.
	err = os.Chmod(dir, 0o700)
	if err == nil {
		err = os.WriteFile(path, []byte(v.value), 0o600)
	}
	if err == nil {
		err = os.Chmod(path, 0o600)
	}
	if err != nil {
		return "", plugin.Permanent(fmt.Errorf("prepare %s: %w", path, err))
	}
	return path, nil
}

// safeRel refuses anything that would leave secrets_dir; the names it joins
// come from the cluster, so a path separator in one is a bug or an attack.
func safeRel(p string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(p))
	if p == "" || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || strings.ContainsAny(p, `\`) {
		return "", fmt.Errorf("secret path %q escapes the secrets directory", p)
	}
	return clean, nil
}

// node is one JSON value of a source config: a string leaf parsed as a
// template, an object or an array of nodes, or a literal copied verbatim — a
// number keeps its exact text, which decoding to float64 would not.
type node struct {
	tmpl *template.Template
	obj  map[string]*node
	arr  []*node
	raw  jsontext.Value
}

// parseSourceConfig parses every string of a source config as a template, so a
// syntax error is reported by `validate`, offline.
func parseSourceConfig(raw jsontext.Value, funcs template.FuncMap, path string) (*node, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}
	n, err := parseNode(raw, path, funcs)
	if err != nil {
		return nil, err
	}
	if n.obj == nil {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}
	return n, nil
}

func parseNode(raw jsontext.Value, path string, funcs template.FuncMap) (*node, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}
	switch trimmed[0] {
	case '{':
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]*node, len(obj))
		for _, k := range keys {
			child, err := parseNode(obj[k], path+"/"+k, funcs)
			if err != nil {
				return nil, err
			}
			out[k] = child
		}
		return &node{obj: out}, nil
	case '[':
		var arr []jsontext.Value
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out := make([]*node, len(arr))
		for i, v := range arr {
			child, err := parseNode(v, fmt.Sprintf("%s/%d", path, i), funcs)
			if err != nil {
				return nil, err
			}
			out[i] = child
		}
		return &node{arr: out}, nil
	case '"':
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		tmpl, err := template.New(path).Option("missingkey=error").Funcs(funcs).Parse(text)
		if err != nil {
			return nil, err
		}
		return &node{tmpl: tmpl}, nil
	default:
		return &node{raw: jsontext.Value(trimmed)}, nil
	}
}

// renderJSON executes every template against the object and rebuilds the
// config as JSON. Object keys are written in sorted order and a literal keeps
// its exact text, so the bytes are as stable as the values they carry —
// marshalling a map would not be (encoding/json/v2 does not sort map keys).
func (n *node) renderJSON(data objectData) (jsontext.Value, error) {
	var buf bytes.Buffer
	w := &jsonWriter{enc: jsontext.NewEncoder(&buf)}
	n.write(w, data)
	if w.err != nil {
		return nil, w.err
	}
	// The encoder terminates every top-level value with a newline.
	return jsontext.Value(bytes.TrimSpace(buf.Bytes())), nil
}

// jsonWriter is one render's JSON output: the encoder, and the failure the
// render stopped at — a template that could not be executed, or the one value
// the encoder refused to write.
type jsonWriter struct {
	enc *jsontext.Encoder
	err error
}

// token writes one structural token — a brace, a bracket, an object key. The
// encoder cannot refuse one: its state is the one this writer maintains, and
// the writer underneath it is a bytes.Buffer, so a refusal would be this
// package's own bug, and must.Do is where that claim is made.
func (w *jsonWriter) token(t jsontext.Token) {
	must.Do(struct{}{}, w.enc.WriteToken(t))
}

// value writes one literal of a source config. It is the exact text of a value
// the strict decoder already read, so the encoder cannot refuse it either.
func (w *jsonWriter) value(v jsontext.Value) {
	must.Do(struct{}{}, w.enc.WriteValue(v))
}

// write renders one node. A member that cannot be rendered ends its object's or
// its array's traversal there: the members after it are not worth rendering into
// a document that is discarded, and the encoder's state is the one this writer
// maintains only as long as every token it wrote was accepted.
func (n *node) write(w *jsonWriter, data objectData) {
	switch {
	case n.tmpl != nil:
		var b strings.Builder
		if err := n.tmpl.Execute(&b, data); err != nil {
			w.err = err
			return
		}
		// What a template rendered is the one thing the encoder can refuse: a
		// label, a Secret's bytes or a variable's value may not be UTF-8, and
		// JSON is — so a value it will not carry fails this object's render.
		if err := w.enc.WriteToken(jsontext.String(b.String())); err != nil {
			w.err = err
		}
	case n.obj != nil:
		w.token(jsontext.BeginObject)
		keys := make([]string, 0, len(n.obj))
		for k := range n.obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			w.token(jsontext.String(k))
			n.obj[k].write(w, data)
			if w.err != nil {
				return
			}
		}
		w.token(jsontext.EndObject)
	case n.arr != nil:
		w.token(jsontext.BeginArray)
		for _, child := range n.arr {
			child.write(w, data)
			if w.err != nil {
				return
			}
		}
		w.token(jsontext.EndArray)
	default:
		w.value(n.raw)
	}
}
