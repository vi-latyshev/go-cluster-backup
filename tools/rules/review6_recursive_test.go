package main

import (
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
)

func TestR6_RecursiveContainer(t *testing.T) {
	if os.Getenv("GO_REVIEW6_RECURSION") == "1" {
		debug.SetMaxStack(128 << 10)
		r := mustScan(t, map[string]string{
			"internal/plugins/library/node.go": "package library\ntype Node []Node\n",
		})
		_ = ruleRestrictedSymbols(r, nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestR6_RecursiveContainer$")
	cmd.Env = append(os.Environ(), "GO_REVIEW6_RECURSION=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "fatal error: stack overflow") {
			t.Fatal("valid recursive container crashed rules: fatal error: stack overflow")
		}
		t.Fatalf("rules subprocess: %v: %s", err, output)
	}
}
