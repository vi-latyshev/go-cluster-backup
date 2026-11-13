package configuration

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// The static template check is deliberately narrow. It refuses only what is
// certain in every evaluation of the refused node, plus two declared policy
// rules of the owner rulings:
//
//	certain   (a) a field or method NAME that names nothing on a statically
//	              certain, non-interface, non-map type;
//	          (b) every selector in an endpoint template (it runs with no data);
//	          (c1) a pick/pickdef argument count funcPick/funcPickDef reject.
//	policy P1 the same defect inside a branch or definition that may never run.
//	policy P2 a constant pick/pickdef key on a direct enum field outside the
//	          field's value list (∪ "").
//
// Nothing else is judged here: call arity, argument types, builtin signatures,
// comparisons, and/or, representability, addressability and numeric syntax are
// left to the 9e6dbe8 sample render, which catches call errors exactly as
// 2.2.0 did.

// CheckTemplate type-checks every definition of one parsed template and names
// each refusal with its rule class ("certain", "policy P1", "policy P2"). A nil
// root is the no-data mode: an endpoint, whose every selector is refused.
func CheckTemplate(t *template.Template, root reflect.Type, funcs template.FuncMap) error {
	return checkTemplate(t, root, funcs)
}

// enumField is one registered report field: the owning struct type and the
// field name a template spells. The registry is keyed by field, so a plain
// string outcome field is covered without changing the field's type.
type enumField struct {
	owner  reflect.Type
	name   string
	values []model.EnumValue
}

var enumRegistry = map[reflect.Type]map[string]enumField{
	reflect.TypeFor[model.RunReport](): {
		"Outcome": {owner: reflect.TypeFor[model.RunReport](), name: "Outcome", values: model.OutcomeValues},
		"Kind":    {owner: reflect.TypeFor[model.RunReport](), name: "Kind", values: model.RunKindValues},
	},
	reflect.TypeFor[model.LeaseState](): {
		"HolderKind": {owner: reflect.TypeFor[model.LeaseState](), name: "HolderKind", values: model.RunKindValues},
	},
	reflect.TypeFor[model.Exclusion](): {
		"Kind": {owner: reflect.TypeFor[model.Exclusion](), name: "Kind", values: model.ExclusionKindValues},
	},
	reflect.TypeFor[model.Artifact](): {
		"Importance":  {owner: reflect.TypeFor[model.Artifact](), name: "Importance", values: model.ImportanceValues},
		"Sensitivity": {owner: reflect.TypeFor[model.Artifact](), name: "Sensitivity", values: model.SensitivityValues},
	},
	reflect.TypeFor[model.ArtifactResult](): {
		"Outcome": {owner: reflect.TypeFor[model.ArtifactResult](), name: "Outcome", values: model.ArtifactOutcomeValues},
	},
	reflect.TypeFor[model.StorageResult](): {
		"Outcome": {owner: reflect.TypeFor[model.StorageResult](), name: "Outcome", values: model.StorageOutcomeValues},
	},
}

// registeredField answers the registry entry for one resolved struct field.
func registeredField(owner reflect.Type, name string) (enumField, bool) {
	fields, ok := enumRegistry[owner]
	if !ok {
		return enumField{}, false
	}
	field, ok := fields[name]
	return field, ok
}

// enumValuesByType is the runtime-type registry JudgedPick uses for the five
// named enum types. The two plain-string outcome fields are judged by their
// runtime judge instead.
var enumValuesByType = map[reflect.Type][]model.EnumValue{
	reflect.TypeFor[model.Outcome]():       model.OutcomeValues,
	reflect.TypeFor[model.RunKind]():       model.RunKindValues,
	reflect.TypeFor[model.ExclusionKind](): model.ExclusionKindValues,
	reflect.TypeFor[model.Importance]():    model.ImportanceValues,
	reflect.TypeFor[model.Sensitivity]():   model.SensitivityValues,
}

// Check type-checks this field's template against root with the functions it was
// parsed with. A template that was never parsed has no text to judge.
func (t Template) Check(root reflect.Type) error {
	if t.tmpl == nil {
		return nil
	}
	return CheckTemplate(t.tmpl, root, t.funcs)
}

// RenderJudged executes the template as Render does on a validation copy: the
// field-selector picks receive their receiver and field name, every other pick
// is JudgedPick by runtime type, and the template a run executes is never touched.
func (t Template) RenderJudged(root reflect.Type, data any) (string, error) {
	if t.tmpl == nil {
		if t.text == "" {
			return "", nil
		}
		return "", fmt.Errorf("template was not parsed at decode time")
	}
	tmpl, err := JudgeTemplate(t.tmpl, root, t.funcs)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if err := tmpl.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

// JudgeTemplate builds the validation copy of one parsed template: every
// definition's parse tree is copied (Clone shares trees), every pick/pickdef
// whose first argument is a field selector receives its receiver and field
// name, and "pick" is JudgedPick. The runtime judge looks the (receiver,
// field) pair up in the registry then, so one shared definition is judged by
// the context each call actually runs in. The original template is never
// modified.
func JudgeTemplate(t *template.Template, root reflect.Type, funcs template.FuncMap) (*template.Template, error) {
	if err := checkTemplate(t, root, funcs); err != nil {
		return nil, err
	}
	judged := template.New(t.Name()).Option("missingkey=error")
	merged := make(template.FuncMap, len(funcs)+3)
	for name, fn := range funcs {
		merged[name] = fn
	}
	merged["pick"] = JudgedPick
	merged[judgePick] = JudgedFieldPick
	merged[judgePickDef] = JudgedFieldPickDef
	judged.Funcs(merged)
	for _, def := range t.Templates() {
		if def.Tree == nil {
			continue
		}
		tree := def.Copy()
		rewriteFieldPicks(tree.Root)
		// AddParseTree only stores the tree; it never validates one, so this
		// error is provably nil.
		_, _ = judged.AddParseTree(def.Name(), tree)
	}
	return judged, nil
}

// The reserved names a field-selector pick is rewritten to.
const (
	judgePick    = "__judged_field_pick"
	judgePickDef = "__judged_field_pick_def"
)

// rewriteFieldPicks rewrites every pick/pickdef in one copied tree whose first
// explicit argument is a field selector: the judge then receives the receiver
// and the field name along with the value, and decides from the runtime type.
func rewriteFieldPicks(node parse.Node) {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil {
			return
		}
		for _, child := range n.Nodes {
			rewriteFieldPicks(child)
		}
	case *parse.ActionNode:
		rewriteFieldPicks(n.Pipe)
	case *parse.PipeNode:
		if n == nil {
			return
		}
		for _, cmd := range n.Cmds {
			rewriteFieldPicks(cmd)
		}
	case *parse.CommandNode:
		rewriteFieldPick(n)
		for _, arg := range n.Args {
			rewriteFieldPicks(arg)
		}
	case *parse.ChainNode:
		rewriteFieldPicks(n.Node)
	case *parse.IfNode:
		rewriteFieldPicks(n.Pipe)
		rewriteFieldPicks(n.List)
		rewriteFieldPicks(n.ElseList)
	case *parse.RangeNode:
		rewriteFieldPicks(n.Pipe)
		rewriteFieldPicks(n.List)
		rewriteFieldPicks(n.ElseList)
	case *parse.WithNode:
		rewriteFieldPicks(n.Pipe)
		rewriteFieldPicks(n.List)
		rewriteFieldPicks(n.ElseList)
	case *parse.TemplateNode:
		rewriteFieldPicks(n.Pipe)
	}
}

// rewriteFieldPick turns one pick/pickdef command into its receiver-carrying
// judge form. A first argument that names no field is left alone: the ordinary
// runtime-type JudgedPick judges it.
func rewriteFieldPick(cmd *parse.CommandNode) {
	if len(cmd.Args) < 2 {
		return
	}
	id, ok := cmd.Args[0].(*parse.IdentifierNode)
	if !ok || (id.Ident != "pick" && id.Ident != "pickdef") {
		return
	}
	receiver, field, ok := fieldSelector(cmd.Args[1])
	if !ok {
		return
	}
	if id.Ident == "pickdef" {
		id.Ident = judgePickDef
	} else {
		id.Ident = judgePick
	}
	// The receiver is the command's argument, so the executor evaluates it
	// exactly once; the judge resolves the last segment on that one value. The
	// original value expression is replaced, never kept beside the receiver.
	args := make([]parse.Node, 0, len(cmd.Args)+1)
	args = append(args, cmd.Args[0], receiver, fieldNameNode(field, cmd.Pos))
	args = append(args, cmd.Args[2:]...)
	cmd.Args = args
}

// fieldSelector splits one first-argument expression into its receiver and its
// final field name. Parentheses around the whole expression are transparent; a
// plain field chain qualifies: `.Field`, `.A.Field`, `$v.Field` and
// `(expr).Field`.
func fieldSelector(node parse.Node) (parse.Node, string, bool) {
	switch n := transparentExpr(node).(type) {
	case *parse.FieldNode:
		if len(n.Ident) == 0 {
			return nil, "", false
		}
		field := n.Ident[len(n.Ident)-1]
		if len(n.Ident) == 1 {
			return &parse.DotNode{NodeType: parse.NodeDot, Pos: n.Pos}, field, true
		}
		return &parse.FieldNode{NodeType: parse.NodeField, Pos: n.Pos,
			Ident: append([]string{}, n.Ident[:len(n.Ident)-1]...)}, field, true
	case *parse.VariableNode:
		if len(n.Ident) < 2 {
			return nil, "", false
		}
		field := n.Ident[len(n.Ident)-1]
		return &parse.VariableNode{NodeType: parse.NodeVariable, Pos: n.Pos,
			Ident: append([]string{}, n.Ident[:len(n.Ident)-1]...)}, field, true
	case *parse.ChainNode:
		if len(n.Field) == 0 {
			return nil, "", false
		}
		field := n.Field[len(n.Field)-1]
		if len(n.Field) == 1 {
			return n.Node, field, true
		}
		return &parse.ChainNode{NodeType: parse.NodeChain, Pos: n.Pos, Node: n.Node,
			Field: append([]string{}, n.Field[:len(n.Field)-1]...)}, field, true
	}
	return nil, "", false
}

// transparentExpr unwraps parentheses that add no operation: a pipeline with
// no declaration or assignment, one command and one argument.
func transparentExpr(node parse.Node) parse.Node {
	for {
		pipe, ok := node.(*parse.PipeNode)
		if !ok || pipe.IsAssign || len(pipe.Decl) != 0 || len(pipe.Cmds) != 1 || len(pipe.Cmds[0].Args) != 1 {
			return node
		}
		node = pipe.Cmds[0].Args[0]
	}
}

// fieldNameNode is the literal field name handed to a runtime field judge.
func fieldNameNode(field string, pos parse.Pos) *parse.StringNode {
	return &parse.StringNode{NodeType: parse.NodeString, Pos: pos, Quoted: strconv.Quote(field), Text: field}
}

// JudgedPick is pick with validate's completeness judge: a value of a named
// model enum whose runtime type is known that every deployment produces and no
// pair names is refused; any other unmatched value takes the first text. A
// string-building function erases the runtime type, so its result is not judged.
func JudgedPick(v any, pairs ...any) (string, error) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return "", fmt.Errorf("pick: %d arguments after the value; it takes key/text pairs", len(pairs))
	}
	return pickText(v, pairs, enumValuesByType[reflect.TypeOf(v)], true)
}

// JudgedFieldPick is pick for a rewritten selector. The executor hands it the
// receiver value, evaluated exactly once, and the last selector segment: the
// judge resolves that segment with the executor's own lookup order and judges
// the result by the receiver's actual (struct, field) registry entry. A
// receiver that is not a registry field falls back to the runtime enum judge of
// the value's named type, and to the tolerant pick for anything else.
func JudgedFieldPick(receiver any, field string, pairs ...any) (string, error) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return "", fmt.Errorf("pick: %d arguments after the value; it takes key/text pairs", len(pairs))
	}
	v, err := selectorValue(receiver, field)
	if err != nil {
		return "", err
	}
	return pickText(v, pairs, fieldValues(receiver, field, v), true)
}

// JudgedFieldPickDef mirrors pickdef: it resolves the selector the same way,
// then takes the default when no pair matches and never judges completeness.
func JudgedFieldPickDef(receiver any, field string, def string, pairs ...any) (string, error) {
	if len(pairs)%2 != 0 {
		return "", fmt.Errorf("pickdef: %d key/text arguments; it takes pairs", len(pairs))
	}
	v, err := selectorValue(receiver, field)
	if err != nil {
		return "", err
	}
	for i := 0; i < len(pairs); i += 2 {
		if fmt.Sprint(pairs[i]) == fmt.Sprint(v) {
			return fmt.Sprint(pairs[i+1]), nil
		}
	}
	return def, nil
}

// selectorValue resolves one final selector segment on a runtime receiver with
// the executor's evalField order: deref pointers and interfaces, a method on
// the value or its pointer (called niladically), an exported struct field or a
// map key, and the executor's own messages when the name names nothing.
func selectorValue(receiver any, field string) (any, error) {
	rv := reflect.ValueOf(receiver)
	if !rv.IsValid() {
		return nil, fmt.Errorf("nil data; no entry for key %q", field)
	}
	typ := rv.Type()
	rv, isNil := indirectValue(rv)
	// The executor looks for the method on the indirected value, taking its
	// address only when that value is addressable. A pointer receiver keeps its
	// own method set, so no copy is ever made: a copied receiver would change
	// the object a pointer method runs on.
	ptr := rv
	if ptr.Kind() != reflect.Interface && ptr.Kind() != reflect.Pointer && ptr.CanAddr() {
		ptr = ptr.Addr()
	}
	if method := ptr.MethodByName(field); method.IsValid() {
		return callNiladic(field, method)
	}
	switch rv.Kind() {
	case reflect.Struct:
		tField, ok := rv.Type().FieldByName(field)
		if ok {
			if !tField.IsExported() {
				return nil, fmt.Errorf("%s is an unexported field of struct type %s", field, typ)
			}
			fieldValue, err := rv.FieldByIndexErr(tField.Index)
			if err != nil {
				return nil, err
			}
			return fieldValue.Interface(), nil
		}
	case reflect.Map:
		if reflect.TypeFor[string]().AssignableTo(rv.Type().Key()) {
			result := rv.MapIndex(reflect.ValueOf(field))
			if !result.IsValid() {
				return nil, fmt.Errorf("map has no entry for key %q", field)
			}
			return result.Interface(), nil
		}
	case reflect.Pointer:
		elem := rv.Type().Elem()
		if elem.Kind() == reflect.Struct {
			if _, ok := elem.FieldByName(field); !ok {
				break
			}
		}
		if isNil {
			return nil, fmt.Errorf("nil pointer evaluating %s.%s", typ, field)
		}
	}
	return nil, fmt.Errorf("can't evaluate field %s in type %s", field, typ)
}

// indirectValue strips pointers and interfaces, reporting a nil one.
func indirectValue(v reflect.Value) (reflect.Value, bool) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return v, true
		}
		v = v.Elem()
	}
	return v, false
}

// callNiladic calls one bound method without arguments, unwrapping the
// executor's reflect.Value result convention.
func callNiladic(name string, method reflect.Value) (any, error) {
	typ := method.Type()
	switch {
	case typ.NumOut() == 1:
	case typ.NumOut() == 2 && typ.Out(1) == reflect.TypeFor[error]():
	case typ.NumOut() == 2:
		return nil, fmt.Errorf("invalid function signature for %s: second return value should be error; is %s", name, typ.Out(1))
	default:
		return nil, fmt.Errorf("function %s has %d return values; should be 1 or 2", name, typ.NumOut())
	}
	ret := method.Call(nil)
	if len(ret) == 2 && !ret[1].IsNil() {
		return nil, ret[1].Interface().(error)
	}
	if ret[0].Type() == reflect.TypeFor[reflect.Value]() {
		return ret[0].Interface().(reflect.Value).Interface(), nil
	}
	return ret[0].Interface(), nil
}

// fieldValues answers the value list one runtime field read is judged against:
// the registry entry of (receiver's struct type, field) when there is one, the
// named enum type of the value otherwise, and nil — the tolerant pick — for a
// non-enum.
func fieldValues(receiver any, field string, v any) []model.EnumValue {
	if t := reflect.TypeOf(receiver); t != nil {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct {
			if registered, ok := enumRegistry[t][field]; ok {
				return registered.values
			}
		}
	}
	return enumValuesByType[reflect.TypeOf(v)]
}

// pickText renders one pick: a matching pair's text, otherwise the first text
// unless the unmatched value is produced everywhere in the judged list.
func pickText(v any, pairs []any, values []model.EnumValue, judge bool) (string, error) {
	key, keys := fmt.Sprint(v), make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		k := fmt.Sprint(pairs[i])
		keys = append(keys, k)
		if k == key {
			return fmt.Sprint(pairs[i+1]), nil
		}
	}
	if judge {
		for _, value := range values {
			if value.Value == key && value.Everywhere {
				return "", fmt.Errorf("pick: %q matches none of %s; every deployment produces it, so name it", key, strings.Join(keys, ", "))
			}
		}
	}
	return fmt.Sprint(pairs[1]), nil
}

// visitKey is one (definition, dot type) analysis; a nil type is the unknown
// dot.
type visitKey struct {
	name string
	typ  reflect.Type
}

// certainMark is the rule class of one refusal.
const (
	classCertain = "certain"
	classP1      = "policy P1"
	classP2      = "policy P2"
)

// checker walks every definition with its static types and names each defect.
type checker struct {
	funcs       template.FuncMap
	root        reflect.Type
	noData      bool
	visited     map[visitKey]bool
	errs        []error
	definition  string
	certainVars map[string]bool
	currentDead bool
}

// expr is one expression's static type; nil is unknown. field is set when the
// selector chain ended in a registered report field.
type expr struct {
	typ   reflect.Type
	field *enumField
}

// binding is one variable's static type; nil is unknown.
type binding struct {
	typ reflect.Type
}

// scope is one execution path's bindings.
type scope struct {
	dot  reflect.Type
	root reflect.Type
	vars map[string]binding
	// dead marks a path that may never run (a branch or a definition): its
	// CERTAIN-class defects are reported under the P1 policy.
	dead bool
}

func (s scope) child() scope {
	vars := make(map[string]binding, len(s.vars))
	for name, value := range s.vars {
		vars[name] = value
	}
	return scope{dot: s.dot, root: s.root, vars: vars, dead: s.dead}
}

// checkTemplate walks the whole template and reports every refusal.
func checkTemplate(t *template.Template, root reflect.Type, funcs template.FuncMap) error {
	c := &checker{funcs: funcs, root: root, visited: map[visitKey]bool{}, noData: root == nil}
	defs := map[string]*template.Template{}
	for _, tmpl := range t.Templates() {
		defs[tmpl.Name()] = tmpl
	}
	queue := []visitKey{{name: t.Name(), typ: c.rootType()}}
	analyzed := map[string]bool{}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		if c.visited[key] {
			continue
		}
		c.visited[key] = true
		analyzed[key.name] = true
		tmpl, ok := defs[key.name]
		if !ok {
			continue
		}
		edges, err := c.walk(tmpl, key.typ, key.name != t.Name())
		if err != nil {
			c.errs = append(c.errs, fmt.Errorf("%s: %w", key.name, err))
		}
		queue = append(queue, edges...)
	}
	// Every definition is walked, called or not; one with no call site (or
	// reached only through an unknown dot) is checked with an unknown dot.
	for name := range defs {
		if analyzed[name] {
			continue
		}
		c.visited[visitKey{name: name}] = true
		if _, err := c.walk(defs[name], nil, true); err != nil {
			c.errs = append(c.errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(c.errs...)
}

// rootType is the certain type of the Execute data, unknown in no-data mode.
func (c *checker) rootType() reflect.Type {
	if c.noData {
		return nil
	}
	return c.root
}

// walk analyses one definition body. A definition body may never run, so its
// name defects are P1; the root template's top level is certain.
func (c *checker) walk(tmpl *template.Template, dot reflect.Type, definition bool) ([]visitKey, error) {
	if tmpl.Tree == nil || tmpl.Root == nil {
		return nil, nil
	}
	s := scope{dot: dot, root: dot, vars: map[string]binding{}, dead: definition}
	if c.noData {
		s.dot = nil
	}
	certainVars, assignedDollar := collectCertainVariables(tmpl.Root)
	if assignedDollar {
		// "$" is the definition's root variable; one "=" assignment anywhere in
		// the definition makes every later read unknown.
		s.vars["$"] = binding{}
	}
	previousDefinition, previousVars, previousDead := c.definition, c.certainVars, c.currentDead
	c.definition, c.certainVars, c.currentDead = tmpl.Name(), certainVars, definition
	edges, err := c.nodes(tmpl.Root, s)
	c.definition, c.certainVars, c.currentDead = previousDefinition, previousVars, previousDead
	return edges, err
}

// collectCertainVariables answers the variables whose static type is certain:
// exactly one declaration and no "=" assignment anywhere in the body, and
// whether "$" is assigned anywhere. A nested pipeline in an argument is walked
// like a top-level one.
func collectCertainVariables(list *parse.ListNode) (map[string]bool, bool) {
	counts := map[string]int{}
	assigned := map[string]bool{}
	var pipe func(*parse.PipeNode)
	pipe = func(p *parse.PipeNode) {
		if p == nil {
			return
		}
		for _, decl := range p.Decl {
			if p.IsAssign {
				assigned[decl.Ident[0]] = true
			} else {
				counts[decl.Ident[0]]++
			}
		}
		for _, cmd := range p.Cmds {
			for _, arg := range cmd.Args {
				walkNode(arg, pipe)
			}
		}
	}
	for _, node := range list.Nodes {
		walkNode(node, pipe)
	}
	certain := map[string]bool{}
	for name, count := range counts {
		certain[name] = count == 1 && !assigned[name]
	}
	return certain, assigned["$"]
}

// walkNode visits one node and every pipeline it nests.
func walkNode(node parse.Node, visit func(*parse.PipeNode)) {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil {
			return
		}
		for _, child := range n.Nodes {
			walkNode(child, visit)
		}
	case *parse.ActionNode:
		visit(n.Pipe)
	case *parse.PipeNode:
		visit(n)
	case *parse.ChainNode:
		walkNode(n.Node, visit)
	case *parse.IfNode:
		visit(n.Pipe)
		walkNode(n.List, visit)
		walkNode(n.ElseList, visit)
	case *parse.RangeNode:
		visit(n.Pipe)
		walkNode(n.List, visit)
		walkNode(n.ElseList, visit)
	case *parse.WithNode:
		visit(n.Pipe)
		walkNode(n.List, visit)
		walkNode(n.ElseList, visit)
	case *parse.TemplateNode:
		visit(n.Pipe)
	}
}

func (c *checker) nodes(list *parse.ListNode, s scope) ([]visitKey, error) {
	if list == nil {
		return nil, nil
	}
	var edges []visitKey
	var errs []error
	for _, node := range list.Nodes {
		e, err := c.node(node, s)
		edges = append(edges, e...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return edges, errors.Join(errs...)
}

func (c *checker) node(node parse.Node, s scope) ([]visitKey, error) {
	switch n := node.(type) {
	case *parse.TextNode, *parse.CommentNode, *parse.BreakNode, *parse.ContinueNode:
		return nil, nil
	case *parse.ActionNode:
		_, err := c.pipeline(n.Pipe, s)
		return nil, err
	case *parse.IfNode:
		branch := s.child()
		branch.dead = true
		errs := []error{}
		if _, err := c.pipeline(n.Pipe, branch); err != nil {
			errs = append(errs, err)
		}
		edges, err := c.nodes(n.List, branch)
		errs = append(errs, err)
		more, err := c.nodes(n.ElseList, branch)
		edges = append(edges, more...)
		errs = append(errs, err)
		return edges, errors.Join(errs...)
	case *parse.WithNode:
		branch := s.child()
		branch.dead = true
		value, err := c.pipeline(n.Pipe, branch)
		if err != nil {
			return nil, err
		}
		body := branch
		body.dot = value.typ
		edges, errs := c.nodes(n.List, body)
		more, elseErr := c.nodes(n.ElseList, branch)
		edges = append(edges, more...)
		return edges, errors.Join(errs, elseErr)
	case *parse.RangeNode:
		return c.rangeNode(n, s)
	case *parse.TemplateNode:
		var arg expr
		if n.Pipe != nil {
			value, err := c.pipeline(n.Pipe, s.child())
			if err != nil {
				return nil, err
			}
			arg = value
		}
		return []visitKey{{name: n.Name, typ: arg.typ}}, nil
	}
	return nil, nil
}

// rangeNode checks one range: the body's dot is the element type for a slice,
// array, map, chan or int; an else branch keeps the outer dot and the pipeline
// value in every declared variable.
func (c *checker) rangeNode(n *parse.RangeNode, s scope) ([]visitKey, error) {
	branch := s.child()
	branch.dead = true
	value, err := c.pipeline(n.Pipe, branch)
	if err != nil {
		return nil, err
	}
	elem, index := rangeTypes(value.typ)
	// The else branch may run with no iteration: its range declarations are
	// unknown there, and its variable map stays independent of the body's.
	for _, decl := range n.Pipe.Decl {
		branch.vars[decl.Ident[0]] = binding{}
	}
	body := branch.child()
	body.dot = elem
	if len(n.Pipe.Decl) == 1 {
		if c.certainVars[n.Pipe.Decl[0].Ident[0]] {
			body.vars[n.Pipe.Decl[0].Ident[0]] = binding{typ: elem}
		}
	} else if len(n.Pipe.Decl) >= 2 {
		if c.certainVars[n.Pipe.Decl[0].Ident[0]] {
			body.vars[n.Pipe.Decl[0].Ident[0]] = binding{typ: index}
		}
		if c.certainVars[n.Pipe.Decl[1].Ident[0]] {
			body.vars[n.Pipe.Decl[1].Ident[0]] = binding{typ: elem}
		}
	}
	edges, errs := c.nodes(n.List, body)
	more, elseErr := c.nodes(n.ElseList, branch)
	edges = append(edges, more...)
	return edges, errors.Join(errs, elseErr)
}

// rangeTypes answers the element and index types a range binds; nil is unknown.
func rangeTypes(t reflect.Type) (elem, index reflect.Type) {
	if t == nil {
		return nil, nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Chan:
		return t.Elem(), reflect.TypeFor[int]()
	case reflect.Map:
		return t.Elem(), t.Key()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return t, t
	}
	return nil, nil
}

// pipeline walks one pipeline and binds its declarations to the last command's
// static type. A variable declared more than once or assigned anywhere in the
// body stays unknown.
func (c *checker) pipeline(pipe *parse.PipeNode, s scope) (expr, error) {
	if pipe == nil {
		return expr{}, nil
	}
	var last expr
	var errs []error
	for i, cmd := range pipe.Cmds {
		value, err := c.command(cmd, s, i > 0)
		if err != nil {
			errs = append(errs, err)
			last = expr{}
			continue
		}
		last = value
	}
	for _, decl := range pipe.Decl {
		name := decl.Ident[0]
		if pipe.IsAssign || !c.certainVars[name] {
			s.vars[name] = binding{}
			continue
		}
		s.vars[name] = binding{typ: last.typ}
	}
	return last, errors.Join(errs...)
}

// command evaluates one command's static result and names. Only names and pick
// parity are judged; arguments are walked for names.
func (c *checker) command(cmd *parse.CommandNode, s scope, piped bool) (expr, error) {
	explicit := make([]expr, 0, len(cmd.Args)-1)
	for _, arg := range cmd.Args[1:] {
		value, err := c.arg(arg, s)
		if err != nil {
			return expr{}, err
		}
		explicit = append(explicit, value)
	}
	if id, ok := cmd.Args[0].(*parse.IdentifierNode); ok {
		switch id.Ident {
		case "pick", "pickdef":
			_, err := c.pick(id.Ident, cmd, explicit, piped)
			return expr{}, err
		}
		return c.callResult(id.Ident, explicit)
	}
	value, err := c.arg(cmd.Args[0], s)
	if err != nil {
		return expr{}, err
	}
	return value, nil
}

// callResult answers a function call's static type: a configured function's
// declared first result; builtins and anything else are unknown.
func (c *checker) callResult(name string, args []expr) (expr, error) {
	if fn, ok := c.funcs[name]; ok {
		if t := reflect.TypeOf(fn); t.Kind() == reflect.Func && t.NumOut() > 0 {
			return expr{typ: staticType(t.Out(0))}, nil
		}
	}
	return expr{}, nil
}

// staticType is a declared result's static type; an empty interface holds any
// value and stays unknown.
func staticType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Interface && t.NumMethod() == 0 {
		return nil
	}
	return t
}

// pick checks (c1) parity and (P2) enum keys when the first explicit argument
// is a selector chain ending in a registry field.
func (c *checker) pick(name string, cmd *parse.CommandNode, explicit []expr, piped bool) (expr, error) {
	total := len(cmd.Args) - 1
	if piped {
		total++
	}
	class := classOf(c.currentDead)
	if name == "pick" {
		pairs := total - 1
		if pairs < 0 || pairs == 0 || pairs%2 != 0 {
			return expr{}, classError(class, fmt.Errorf("pick takes a value and key/text pairs; %d arguments given", total))
		}
	} else {
		pairs := total - 2
		if pairs < 0 || pairs%2 != 0 {
			return expr{}, classError(class, fmt.Errorf("pickdef takes a value, a default and key/text pairs; %d arguments given", total))
		}
	}
	field, ok := pickField(explicit)
	if !ok {
		return expr{}, nil
	}
	firstKey := 2 // args: name, value, key, text, ...
	if name == "pickdef" {
		firstKey = 3 // args: name, value, default, key, text, ...
	}
	for i := firstKey; i < len(cmd.Args); i += 2 {
		key, ok := cmd.Args[i].(*parse.StringNode)
		if !ok || key.Text == "" {
			continue
		}
		if !listedValue(field.values, key.Text) {
			return expr{}, classError(classP2, fmt.Errorf("pick key %q is not a value of %s", key.Text, field.name))
		}
	}
	return expr{}, nil
}

// pickField answers the registry field one pick's first explicit argument
// directly names, if any.
func pickField(explicit []expr) (enumField, bool) {
	if len(explicit) == 0 || explicit[0].field == nil {
		return enumField{}, false
	}
	return *explicit[0].field, true
}

// classOf is the rule class a defect in one path belongs to.
func classOf(dead bool) string {
	if dead {
		return classP1
	}
	return classCertain
}

// classError names one refusal's rule class so tests can split CERTAIN from
// POLICY.
func classError(class string, err error) error {
	return fmt.Errorf("%s: %w", class, err)
}

// listedValue reports whether one value list holds v.
func listedValue(values []model.EnumValue, v string) bool {
	for _, value := range values {
		if value.Value == v {
			return true
		}
	}
	return false
}

// arg resolves one argument expression and judges names inside it.
func (c *checker) arg(node parse.Node, s scope) (expr, error) {
	switch n := node.(type) {
	case *parse.DotNode:
		return expr{typ: s.dot}, nil
	case *parse.NilNode, *parse.BoolNode, *parse.NumberNode, *parse.StringNode:
		return expr{}, nil
	case *parse.FieldNode:
		return c.selector(s, n.Ident, s.dot)
	case *parse.VariableNode:
		return c.variable(s, n.Ident)
	case *parse.ChainNode:
		base, err := c.arg(n.Node, s)
		if err != nil {
			return expr{}, err
		}
		return c.selector(s, n.Field, base.typ)
	case *parse.PipeNode:
		return c.pipeline(n, s.child())
	case *parse.IdentifierNode:
		return c.callResult(n.Ident, nil)
	}
	return expr{}, nil
}

// variable resolves one variable chain; a variable whose certainty was not
// established is unknown, and names below it are not judged.
func (c *checker) variable(s scope, ident []string) (expr, error) {
	base := ident[0]
	value := binding{typ: s.root}
	if base == "$" {
		if assigned, ok := s.vars["$"]; ok {
			value = assigned
		}
	} else {
		value = s.vars[base]
	}
	if len(ident) == 1 {
		return expr{typ: value.typ}, nil
	}
	return c.selector(s, ident[1:], value.typ)
}

// selector resolves one field or method chain. It refuses a name only when the
// static type is certain and non-interface, and neither the value type nor its
// pointer has the method, no exported field has the name, and it is not a map.
func (c *checker) selector(s scope, names []string, base reflect.Type) (expr, error) {
	if c.noData {
		return expr{}, classError(classCertain, fmt.Errorf("endpoint templates render with no data: .%s names nothing", strings.Join(names, ".")))
	}
	cur := base
	for i, name := range names {
		next, field, err := resolveName(cur, name)
		where := "." + strings.Join(names[:i+1], ".")
		if err != nil {
			return expr{}, classError(classOf(s.dead), fmt.Errorf("%s: %w", where, err))
		}
		cur = next
		value := expr{typ: cur}
		if field != nil {
			value.field = field
		}
		if i == len(names)-1 {
			return value, nil
		}
	}
	return expr{typ: cur}, nil
}

// resolveName applies the executor's field lookup to one static type. An
// unknown or interface type answers unknown (no refusal); a map takes the name
// as a key; a struct answers its exported field or a method; anything else is
// certain to fail.
func resolveName(t reflect.Type, name string) (reflect.Type, *enumField, error) {
	if t == nil {
		return nil, nil, nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Interface {
		return nil, nil, nil
	}
	// The executor looks for a method on the value and, when addressable, on
	// its pointer; either makes the name valid.
	if m, ok := t.MethodByName(name); ok {
		return methodResult(m.Type), nil, nil
	}
	if t.Kind() != reflect.Pointer {
		if m, ok := reflect.PointerTo(t).MethodByName(name); ok {
			return methodResult(m.Type), nil, nil
		}
	}
	if t.Kind() == reflect.Map {
		// A name on a map is a key: the lookup itself never fails here, and the
		// key's element type is not one of the certain sources.
		return nil, nil, nil
	}
	if t.Kind() == reflect.Struct {
		if f, ok := t.FieldByName(name); ok {
			if !f.IsExported() {
				return nil, nil, fmt.Errorf("field %q is not exported", name)
			}
			if field, ok := registeredField(t, name); ok {
				return f.Type, &field, nil
			}
			return f.Type, nil, nil
		}
	}
	return nil, nil, fmt.Errorf("%s has no field or method %q", t, name)
}

// methodResult is a method's declared first result; a method returning nothing
// stays unknown.
func methodResult(t reflect.Type) reflect.Type {
	if t.NumOut() == 0 {
		return nil
	}
	return staticType(t.Out(0))
}
