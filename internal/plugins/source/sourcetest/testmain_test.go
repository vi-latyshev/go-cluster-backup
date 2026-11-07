package sourcetest

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// childCoverPath is the profile a re-executed copy of this test binary writes.
// Run reports a broken source through t.Fatal, which fails the test, so the only
// way to observe that path is from a child process; this is where its coverage
// is handed back. The child never sets it, so its own TestMain is a no-op.
var childCoverPath string

// TestMain folds the child run's coverage into this run's profile. m.Run writes
// the profile at its own exit, so the merge happens after it returns.
func TestMain(m *testing.M) {
	code := m.Run()
	mergeChildCoverage()
	os.Exit(code)
}

func mergeChildCoverage() {
	f := flag.Lookup("test.coverprofile")
	if f == nil {
		return
	}
	parentPath := f.Value.String()
	childPath := childCoverPath
	if parentPath == "" || childPath == "" || parentPath == childPath {
		return
	}
	parent, err := os.ReadFile(parentPath)
	if err != nil {
		return
	}
	child, err := os.ReadFile(childPath)
	_ = os.RemoveAll(filepath.Dir(childPath))
	if err != nil {
		return
	}
	if merged, ok := mergeProfiles(parent, child); ok {
		_ = os.WriteFile(parentPath, merged, 0o644)
	}
}

// mergeProfiles unions two text coverage profiles block by block, keeping the
// higher count for a block both runs recorded.
func mergeProfiles(parent, child []byte) ([]byte, bool) {
	mode := ""
	nums := map[string]string{}
	counts := map[string]string{}
	var order []string
	for _, data := range [][]byte{parent, child} {
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if strings.HasPrefix(line, "mode:") {
				mode = line
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 3 {
				continue
			}
			block, num, count := fields[0], fields[1], fields[2]
			if _, seen := nums[block]; !seen {
				order = append(order, block)
				nums[block] = num
			}
			if count == "0" {
				if _, seen := counts[block]; !seen {
					counts[block] = count
				}
				continue
			}
			counts[block] = count
		}
	}
	if mode == "" {
		return nil, false
	}
	var b strings.Builder
	b.WriteString(mode)
	b.WriteByte('\n')
	for _, block := range order {
		b.WriteString(block)
		b.WriteByte(' ')
		b.WriteString(nums[block])
		b.WriteByte(' ')
		b.WriteString(counts[block])
		b.WriteByte('\n')
	}
	return []byte(b.String()), true
}
