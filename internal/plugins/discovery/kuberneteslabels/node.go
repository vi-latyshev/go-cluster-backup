package kuberneteslabels

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// errNoNode is the environment the config names holding no node name: a caller
// outside Kubernetes, where node checks keep the old behaviour and never fail.
var errNoNode = errors.New("the node environment variable is empty or unset")

// nodeInfo is this writer's node as the API answered for it: the name, and the
// labels a required node affinity is matched against.
type nodeInfo struct {
	name   string
	labels map[string]string
}

// nodeState is what one collect knows about this pod's node. The API is asked
// once per collect, and a failure is shared by every object of that collect
// instead of each making the call execution counts as one attempt.
type nodeState struct {
	listed bool
	info   nodeInfo
	err    error
	warned bool
}

// forgetNode drops the previous collect's node reads, so a retry pass sees the
// cluster as it is now.
func (s *state) forgetNode() {
	s.node = nodeState{}
	s.nodes = nil
	s.placements = nil
}

// nodeName is the node name the environment carries, "" when the config names
// no environment variable at all.
func (s *state) nodeName() string {
	if s.nodeEnv == "" {
		return ""
	}
	return os.Getenv(s.nodeEnv)
}

// nodeChecksOff reports whether this collect proves nothing about nodes: the
// config opted out with an empty node_env, or the environment holds no node
// name and node_scope is false. Both are the old, unproven behaviour.
func (s *state) nodeChecksOff() bool {
	return !s.nodeScope && s.nodeName() == ""
}

// proof resolves this pod's node once per collect. errNoNode means node checks
// are off, not that the cluster answered anything.
func (s *state) proof(ctx context.Context) (nodeInfo, error) {
	if s.node.listed {
		return s.node.info, s.node.err
	}
	s.node.listed = true
	name := s.nodeName()
	if name == "" {
		s.node.err = errNoNode
		return nodeInfo{}, errNoNode
	}
	s.node.info, s.node.err = s.nodeBy(ctx, name)
	return s.node.info, s.node.err
}

// nodeBy reads one node — this writer's, or a declared peer's — and verifies
// the API answered for the name that was asked.
func (s *state) nodeBy(ctx context.Context, name string) (nodeInfo, error) {
	if s.nodes == nil {
		s.nodes = map[string]nodeResult{}
	}
	if r, ok := s.nodes[name]; ok {
		return r.info, r.err
	}
	var r nodeResult
	n, err := s.client.GetNode(ctx, name)
	switch {
	case err != nil:
		r.err = fmt.Errorf("reading node %q: %w", name, err)
	case n.Metadata.Name != name:
		r.err = fmt.Errorf("the API answered node %q for the request of node %q", n.Metadata.Name, name)
	default:
		r.info = nodeInfo{name: name, labels: n.Metadata.Labels}
	}
	s.nodes[name] = r
	return r.info, r.err
}

// nodeResult is one cached node read.
type nodeResult struct {
	info nodeInfo
	err  error
}

// warnNodeUnreadable is the one warning a collect says about a node it cannot
// read. The affected artifacts carry the failure themselves; a permanent
// failure names the remedy, so a missing RBAC grant is never mistaken for a
// cluster without nodes, and a transient one says the API did not answer and
// the discovery is read again.
func (s *state) warnNodeUnreadable(err error) {
	if s.node.warned {
		return
	}
	s.node.warned = true
	reason := err.Error()
	switch class, _ := pluginerrors.ClassOf(err); class {
	case pluginerrors.ClassPermanent:
		reason += "; grant get on nodes, or set node_env to an empty string to opt out of node checks"
	case pluginerrors.ClassTransient:
		reason += "; the API did not answer, and the discovery is read again"
	}
	s.log.Warn("discovery.node.unreadable", slog.Any("event", human.Warning("discovery.node.unreadable",
		human.Text("env", s.nodeEnv),
		human.Text("reason", reason))))
}

// proofError keeps the class of the API's own node-read failure: a 403 stays
// permanent, a 404 stays not-found, and a timeout, 409/429/5xx or transport
// failure stays transient — planning's retry pass then reads the node again, so
// an API blip costs a pass, not the night. Only a cause with no class of its own
// (a name mismatch, a cancelled context) is marked permanent: a retry would ask
// the same question. The remedy text belongs to the permanent answer alone.
func proofError(err error, msg, remedy string) error {
	switch class, _ := pluginerrors.ClassOf(err); class {
	case pluginerrors.ClassPermanent:
		return fmt.Errorf("%s: %w; %s", msg, err, remedy)
	case pluginerrors.ClassTransient:
		return fmt.Errorf("%s: %w; the API did not answer, and the discovery is read again", msg, err)
	case "":
		return pluginerrors.Permanent(fmt.Errorf("%s: %w; %s", msg, err, remedy))
	default:
		return fmt.Errorf("%s: %w", msg, err)
	}
}

// bindNode is the proof a node-scoped discovery owes before it lists anything:
// the environment names this pod's node, the API answers for that node, and
// the deployment declares it as one of the writers. Any of the three missing
// refuses the discovery — node scope that cannot name its own node would widen
// to the whole cluster, which is exactly the silent wrong-node backup it
// exists to prevent.
func (d *Discoverer) bindNode(ctx context.Context) error {
	st := d.state
	if !st.nodeScope {
		return nil
	}
	info, err := st.proof(ctx)
	class, _ := pluginerrors.ClassOf(err)
	switch {
	case errors.Is(err, errNoNode):
		return pluginerrors.Permanent(fmt.Errorf("node_scope is on but %s is empty or unset: set it from spec.nodeName, or set node_env to an empty string and node_scope: false to opt out",
			st.nodeEnv))
	case class == pluginerrors.ClassNotFound:
		// A 404 is a wrong name, not a missing grant: silence about which is
		// which is how a node scope decides about the wrong node.
		return pluginerrors.Permanent(fmt.Errorf("node_scope names node %q in %s, but the API has no such node: set it from spec.nodeName, or set node_scope: false to opt out",
			st.nodeName(), st.nodeEnv))
	case err != nil:
		return proofError(err, "node_scope cannot prove this pod's node",
			"grant get on nodes, or set node_env to an empty string and node_scope: false to opt out")
	case !slices.Contains(st.writerNodes, info.name):
		return pluginerrors.Permanent(fmt.Errorf("node_scope is on but this pod's node %q is not in writer_nodes (%s): add it there, or set node_scope: false",
			info.name, strings.Join(st.writerNodes, ", ")))
	}
	return nil
}

// placement is where one claim's data lives relative to this writer: the node
// path when the volume is this writer's, the declared peer node when it is that
// peer's, and nothing when no node is involved.
type placement struct {
	path string
	peer string
}

// claimPlacement decides whose one claim's volume is, and remembers the
// verdict: the claim's render asks volumePath for the very path this decided.
// It is the node half only: the claim's own source still states what it needs
// from the volume, and a volume that is not on a node at all has nothing to
// prove here.
func (s *state) claimPlacement(o object) (placement, error) {
	if s.nodeChecksOff() {
		return placement{}, nil
	}
	v, err := s.claimVolume(o)
	if err != nil {
		return placement{}, err
	}
	if v.path == "" {
		return placement{}, nil
	}
	own, proofErr := s.proof(s.ctx)
	p, err := s.volumePlacement(o, v, own, proofErr)
	if err != nil {
		return placement{}, err
	}
	if s.placements == nil {
		s.placements = map[string]placement{}
	}
	s.placements[o.namespace+"/"+o.name] = p
	return p, nil
}

// volumePlacement compares one volume's node with this writer's. The verdicts
// are exclusive: a volume whose required affinity admits exactly this node is
// this writer's; one that admits exactly a declared peer is a delegation; an
// affinity that admits several nodes is decided only by the Running pod that
// mounts the claim. Anything else — another node, no admitted node, no
// affinity to prove any node, or no mounting evidence where the affinity is
// ambiguous — fails the artifact instead of reading a directory that is not
// its data.
func (s *state) volumePlacement(o object, v volume, own nodeInfo, proofErr error) (placement, error) {
	switch {
	case proofErr != nil:
		s.warnNodeUnreadable(proofErr)
		return placement{}, proofError(proofErr, o.id(), "grant get on nodes, or set node_env to an empty string to opt out of node checks")
	case v.pinned:
		candidates, err := s.affinityCandidates(o, v)
		if err != nil {
			return placement{}, err
		}
		switch len(candidates) {
		case 0:
			if !s.nodeScope {
				return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: persistentvolume %q is pinned by its required node affinity to a node other than %q: reading the path here would read another node's directory", o.id(), o.volumeName, own.name))
			}
			return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: persistentvolume %q is pinned by its required node affinity to a node outside writer_nodes (%s): no declared writer backs it up", o.id(), o.volumeName, strings.Join(s.writerNodes, ", ")))
		case 1:
			// Under scope a single matching declared node places the volume
			// only when the affinity itself names declared writers alone: a
			// label selector or a value outside writer_nodes may admit a node
			// nobody declared, and only mounting evidence may decide then.
			// Outside scope the same holds with this node as the only
			// declared identity.
			switch {
			case s.nodeScope && !s.declaredPins(v.affinity):
				return s.mountingPlacement(o, v, own, candidates)
			case !s.nodeScope && candidates[0] == own.name && !affinityOnlyNode(v.affinity, own):
				return s.mountingPlacement(o, v, own, candidates)
			}
			if conflict := s.mountingConflict(o, candidates[0]); conflict != "" {
				return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: %s", o.id(), conflict))
			}
			if candidates[0] == own.name {
				return placement{path: v.path}, nil
			}
			return placement{peer: candidates[0]}, nil
		default:
			// The affinity names several declared nodes and cannot say which
			// one holds this volume: only the pod that mounts it can.
			return s.mountingPlacement(o, v, own, candidates)
		}
	case v.local:
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: persistentvolume %q is a local volume with no required node affinity: the node holding its data cannot be proven", o.id(), o.volumeName))
	}
	// An unpinned hostPath volume is a directory of one node without saying
	// which: the Running pod that mounts it is the proof.
	return s.mountingPlacement(o, v, own, nil)
}

// affinityCandidates is every declared node the volume's required affinity
// admits: this writer's own, and — under node scope — each declared peer's.
// Without node scope there is no declared set whose labels could be read, so
// only this node can be a candidate; under scope ambiguity is a fact a caller
// must resolve, never a default.
func (s *state) affinityCandidates(o object, v volume) ([]string, error) {
	var candidates []string
	if affinityMatches(v.affinity, s.node.info) {
		candidates = append(candidates, s.node.info.name)
	}
	if !s.nodeScope {
		return candidates, nil
	}
	for _, name := range s.writerNodes {
		if name == s.node.info.name {
			continue
		}
		node, err := s.nodeBy(s.ctx, name)
		if err != nil {
			return nil, proofError(err,
				fmt.Sprintf("%s: persistentvolume %q is pinned away from this node and node %q cannot be read to prove it is a declared peer", o.id(), o.volumeName, name),
				"grant get on nodes, or remove the peer from writer_nodes")
		}
		if affinityMatches(v.affinity, node) {
			candidates = append(candidates, name)
		}
	}
	return candidates, nil
}

// mountingPlacement decides a volume the affinity alone cannot place — an
// unpinned hostPath, or one whose affinity admits several declared nodes. The
// Running pod(s) that mount the claim are the evidence; no pod, pods on
// conflicting nodes, or a pod outside the admitted set prove nothing.
func (s *state) mountingPlacement(o object, v volume, own nodeInfo, candidates []string) (placement, error) {
	nodes := s.mountingNodes(o)
	switch len(nodes) {
	case 0:
		if candidates != nil {
			// The affinity nominates several nodes, or one node it cannot
			// prove exclusive: without a mounting pod nothing places the
			// volume, and the remedy is the deployment's, not a guess.
			remedy := "; narrow its required node affinity to a node in writer_nodes"
			if !s.nodeScope {
				remedy = "; narrow its required node affinity to one node, or enable node_scope with writer_nodes"
			}
			return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: persistentvolume %q admits nodes %s and no Running pod mounts it, so the node holding its data cannot be proven%s", o.id(), o.volumeName, quoteNodes(candidates), remedy))
		}
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: no Running pod mounts it, so the node whose directory persistentvolume %q is cannot be proven", o.id(), o.volumeName))
	case 1:
	default:
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: Running pods on nodes %s mount it: the node holding persistentvolume %q cannot be proven", o.id(), quoteNodes(nodes), o.volumeName))
	}
	node := nodes[0]
	if candidates != nil && !slices.Contains(candidates, node) {
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: the required node affinity admits nodes %s, but a Running pod on node %q mounts it: the placement conflicts", o.id(), quoteNodes(candidates), node))
	}
	switch {
	case node == own.name:
		return placement{path: v.path}, nil
	case !s.nodeScope:
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: a Running pod on node %q mounts it, not on this pod's node %q: reading the path here would read another node's directory", o.id(), node, own.name))
	case slices.Contains(s.writerNodes, node):
		return placement{peer: node}, nil
	default:
		return placement{}, pluginerrors.Permanent(fmt.Errorf("%s: a Running pod on node %q mounts it, which is not in writer_nodes (%s): add the node there, or set node_scope: false", o.id(), node, strings.Join(s.writerNodes, ", ")))
	}
}

// mountingConflict is the contradiction of a volume pinned to this node and a
// Running pod elsewhere mounting it: one of the two facts is stale, and neither
// directory can be read with confidence.
func (s *state) mountingConflict(o object, own string) string {
	for _, node := range s.mountingNodes(o) {
		if node != own {
			return fmt.Sprintf("persistentvolume %q belongs to node %q, but a Running pod on node %q mounts it: its placement cannot be proven", o.volumeName, own, node)
		}
	}
	return ""
}

// mountingNodes is every node a Running pod that mounts the claim stands on.
func (s *state) mountingNodes(o object) []string {
	if s.acct == nil {
		return nil
	}
	return s.acct.mountNodes[o.namespace+"/"+o.name]
}

func quoteNodes(nodes []string) string {
	quoted := make([]string, len(nodes))
	for i, n := range nodes {
		quoted[i] = strconv.Quote(n)
	}
	return strings.Join(quoted, ", ")
}

// affinityMatches reports whether one required node affinity admits the node:
// the terms are ORed, and every requirement inside one term must hold.
func affinityMatches(terms []kubeapi.NodeSelectorTerm, node nodeInfo) bool {
	for i := range terms {
		if termMatches(terms[i], node) {
			return true
		}
	}
	return false
}

func termMatches(term kubeapi.NodeSelectorTerm, node nodeInfo) bool {
	// A null or empty term matches no objects — the API's own rule, and the
	// safe reading: an affinity nobody wrote must not admit every node.
	if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
		return false
	}
	for _, req := range term.MatchExpressions {
		value, present := node.labels[req.Key]
		if !requirementHolds(req, value, present) {
			return false
		}
	}
	for _, req := range term.MatchFields {
		// metadata.name is the only field a node selector supports.
		if req.Key != "metadata.name" || !requirementHolds(req, node.name, true) {
			return false
		}
	}
	return true
}

// affinityOnlyNode reports whether the required affinity can be satisfied by
// this node alone, without reading any other node's labels: every alternative
// term pins the identity to this node by a single-value metadata.name or
// kubernetes.io/hostname requirement. A term naming several nodes, a selector
// that does not force identity, or a term pinned to another node leaves room
// for a different node, so it proves nothing on its own.
func affinityOnlyNode(terms []kubeapi.NodeSelectorTerm, own nodeInfo) bool {
	if len(terms) == 0 || !affinityMatches(terms, own) {
		return false
	}
	for _, term := range terms {
		key, value, ok := nodePin(term)
		if !ok {
			return false
		}
		switch key {
		case "metadata.name":
			if value != own.name {
				return false
			}
		case "kubernetes.io/hostname":
			if own.labels[key] != value {
				return false
			}
		}
	}
	return true
}

// declaredPins reports whether the required affinity can place a volume on
// declared writers alone: every alternative term pins identity by a single
// value — metadata.name or kubernetes.io/hostname — and every pinned value is
// one of writer_nodes. A label selector, a multi-value In or a value outside
// the declared set may admit an undeclared node, so it proves no placement
// under scope.
func (s *state) declaredPins(terms []kubeapi.NodeSelectorTerm) bool {
	for _, term := range terms {
		_, value, ok := nodePin(term)
		if !ok || !slices.Contains(s.writerNodes, value) {
			return false
		}
	}
	return true
}

// nodePin is the requirement of one term that forces a node's identity: a
// metadata.name field selector, or a kubernetes.io/hostname label selector, In
// with exactly one non-empty value. The selector namespaces are never merged —
// metadata.name as a matchExpression is an ordinary node label another node may
// carry, and matchFields never name kubernetes.io/hostname — so neither is an
// identity pin. A field pin wins over a hostname pin in one term: the field
// forces the name alone, whatever the label filter beside it says.
func nodePin(term kubeapi.NodeSelectorTerm) (key, value string, ok bool) {
	for _, req := range term.MatchFields {
		if req.Key == "metadata.name" && req.Operator == "In" && len(req.Values) == 1 && req.Values[0] != "" {
			return "metadata.name", req.Values[0], true
		}
	}
	for _, req := range term.MatchExpressions {
		if req.Key == "kubernetes.io/hostname" && req.Operator == "In" && len(req.Values) == 1 && req.Values[0] != "" {
			return "kubernetes.io/hostname", req.Values[0], true
		}
	}
	return "", "", false
}

// requirementHolds evaluates one requirement with the API's own operators.
// Anything this build cannot evaluate — an unknown operator, a non-integer
// comparison operand — matches nothing: an affinity that cannot be read must
// never read as compatible.
func requirementHolds(req kubeapi.NodeSelectorRequirement, value string, present bool) bool {
	switch req.Operator {
	case "In":
		return present && slices.Contains(req.Values, value)
	case "NotIn":
		return !present || !slices.Contains(req.Values, value)
	case "Exists":
		return present
	case "DoesNotExist":
		return !present
	case "Gt", "Lt":
		if !present || len(req.Values) != 1 {
			return false
		}
		want, err := strconv.Atoi(req.Values[0])
		if err != nil {
			return false
		}
		got, err := strconv.Atoi(value)
		if err != nil {
			return false
		}
		if req.Operator == "Gt" {
			return got > want
		}
		return got < want
	default:
		return false
	}
}

// elsewhereFound is an object this writer deliberately does not back up
// because a declared peer owns it. The peer's node is part of the record: an
// elsewhere exclusion that names no node is a silent skip.
func (r *preparedRule) elsewhereFound(st *state, o object, node, reason string) discovery.Found {
	f, _ := r.envelope(st, o)
	f.Excluded = model.ExclusionElsewhere
	f.ExcludedNode = node
	f.ExcludedReason = reason
	return f
}
