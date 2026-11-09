package pluginmeta

import (
	"errors"
	"go/ast"
	"go/token"
	"strconv"
)

// unquote is the one unprovokable failure seam: the parser has already
// validated every string literal in the tree, so strconv.Unquote cannot fail on
// an AST node. A test overrides it to prove the error paths.
var unquote = strconv.Unquote

// StringLiteral unquotes one Go string literal.
func StringLiteral(expr ast.Expr) (string, bool) {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := unquote(literal.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

// BoolLiteral reads true or false.
func BoolLiteral(expr ast.Expr) (bool, bool) {
	ident, ok := expr.(*ast.Ident)
	if !ok || (ident.Name != "true" && ident.Name != "false") {
		return false, false
	}
	return ident.Name == "true", true
}

// StringList reads a []string composite literal of string literals.
func StringList(expr ast.Expr) ([]string, bool) {
	values, err := stringList(expr)
	return values, err == nil
}

// stringList is the one []string reader; consumers that need the refusal reason
// use it directly.
func stringList(expr ast.Expr) ([]string, error) {
	literal, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("must be a []string literal")
	}
	out := make([]string, 0, len(literal.Elts))
	for _, element := range literal.Elts {
		value, ok := StringLiteral(element)
		if !ok {
			return nil, errors.New("must be a string literal")
		}
		out = append(out, value)
	}
	return out, nil
}

// stringValue is the reader's internal form for import paths: the parser has
// validated the literal, so a failure is impossible and the path is empty.
func stringValue(expr ast.Expr) string {
	value, _ := StringLiteral(expr)
	return value
}
