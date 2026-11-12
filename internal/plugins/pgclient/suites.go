package pgclient

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
)

// suiteBinaries is the complete client suite one directory must hold: the
// whole cluster globals need pg_dumpall, a probe needs pg_restore, an
// enumeration needs psql. A directory is a suite only when all four are there,
// of one major.
var suiteBinaries = []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"}

// Suite is one complete set of PostgreSQL client programs of one major. The
// zero value is not usable; only resolution returns one.
type Suite struct {
	Major int
	dir   string
	paths map[string]string
}

// Path is the program one suite command runs.
func (s Suite) Path(bin string) string { return s.paths[bin] }

// CheckBinaries proves the local client suites without a server: every
// configured directory that exists must hold the whole suite of one major, its
// binaries must report the major its directory name states, and at least one
// suite — from a directory or from PATH — must be usable. A directory that
// does not exist is skipped: a deployment may name a mount point of an optional
// installation, and PATH remains the last candidate. This is the optional
// instances.CheckBinaries capability; it never touches the network.
func (c *Conn) CheckBinaries(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := c.suites(ctx)
	return err
}

// Select resolves the suite of the server's exact major: it asks the server
// its server_version_num through the newest available psql, then answers the
// suite that matches. A server whose major no local suite holds is refused by
// name — a dump from a client of the wrong major is refused by the server, and
// it must be refused here, not at 04:20.
func (c *Conn) Select(ctx context.Context, pass, database string) (Suite, error) {
	suites, err := c.suites(ctx)
	if err != nil {
		return Suite{}, err
	}
	server, err := c.ServerMajor(ctx, pass, database)
	if err != nil {
		return Suite{}, err
	}
	for _, s := range suites {
		if s.Major == server {
			return s, nil
		}
	}
	return Suite{}, pluginerrors.Permanent(fmt.Errorf(
		"server major %d has no PostgreSQL client suite: client_dirs %s and PATH hold major(s) %s, and the image's client must match the server's major",
		server, quotedDirs(c.cfg.ClientDirs), majorList(suites)))
}

// Newest is the newest locally usable suite, without asking a server: a probe
// reads an archive and needs no source server, and any newer pg_restore reads
// an older archive.
func (c *Conn) Newest(ctx context.Context) (Suite, error) {
	suites, err := c.suites(ctx)
	if err != nil {
		return Suite{}, err
	}
	return newest(suites), nil
}

// ServerMajor asks the server itself; server_version_num packs the major into
// its leading digits (180006 is 18.6). This read-only query is the one call
// that runs before exact-major selection: it is how the exact suite is chosen,
// so it may use the newest psql that is available — a version number is all it
// reads, and nothing of the artifact flows through that client.
func (c *Conn) ServerMajor(ctx context.Context, pass, database string) (int, error) {
	suites, err := c.suites(ctx)
	if err != nil {
		return 0, err
	}
	cmd := c.Command(ctx, newest(suites), "psql", pass, DBNameArg(database), "-At",
		"-c", "select current_setting('server_version_num')")
	out, err := c.Output(ctx, cmd)
	if err != nil {
		return 0, err
	}
	num, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, pluginerrors.Permanent(fmt.Errorf("server version %q is not a number: %w", strings.TrimSpace(out), err))
	}
	return num / 10000, nil
}

// suites resolves and memoizes the local suites once per instance. Only a
// successful resolution is memoized: a directory that was not there yet is
// looked for again by the next call, and a broken one keeps answering.
func (c *Conn) suites(ctx context.Context) ([]Suite, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cache != nil {
		return c.cache, nil
	}
	suites, err := c.resolveSuites(ctx)
	if err != nil {
		return nil, err
	}
	c.cache = suites
	return suites, nil
}

func (c *Conn) resolveSuites(ctx context.Context) ([]Suite, error) {
	var errs []error
	var suites []Suite
	for _, dir := range c.cfg.ClientDirs {
		info, err := os.Stat(dir)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			errs = append(errs, pluginerrors.Permanent(fmt.Errorf("client_dirs %s: %w", dir, err)))
			continue
		case !info.IsDir():
			errs = append(errs, pluginerrors.Permanent(fmt.Errorf("client_dirs %s is not a directory", dir)))
			continue
		}
		suite, err := suiteInDir(ctx, dir)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		suites = append(suites, suite)
	}
	if suite, ok, err := suiteOnPath(ctx); err != nil {
		errs = append(errs, err)
	} else if ok {
		suites = append(suites, suite)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(suites) == 0 {
		return nil, pluginerrors.Permanent(fmt.Errorf(
			"no PostgreSQL client suite is usable: client_dirs %s hold no complete suite and PATH holds no complete %s of one major",
			quotedDirs(c.cfg.ClientDirs), strings.Join(suiteBinaries, ", ")))
	}
	return suites, nil
}

// suiteInDir validates one existing directory as a suite: all four programs
// present, all four of one major, and that major equal to the one its own name
// ends in when the name states one. The name check is what catches a directory
// whose contents a version bump replaced: /usr/libexec/postgresql15 holding 16
// is a broken image, not a usable 16 suite.
func suiteInDir(ctx context.Context, dir string) (Suite, error) {
	paths := make(map[string]string, len(suiteBinaries))
	majors := make([]int, 0, len(suiteBinaries))
	for _, bin := range suiteBinaries {
		path := filepath.Join(dir, bin)
		info, err := os.Stat(path)
		if err != nil {
			return Suite{}, pluginerrors.Permanent(fmt.Errorf("client suite %s is incomplete: %s: %w", dir, bin, err))
		}
		if !info.Mode().IsRegular() {
			return Suite{}, pluginerrors.Permanent(fmt.Errorf("client suite %s: %s is %s, not a program", dir, bin, info.Mode()))
		}
		major, err := binaryMajor(ctx, path)
		if err != nil {
			return Suite{}, err
		}
		paths[bin] = path
		majors = append(majors, major)
	}
	for i := 1; i < len(majors); i++ {
		if majors[i] != majors[0] {
			return Suite{}, pluginerrors.Permanent(fmt.Errorf(
				"client suite %s mixes majors: %s is %d and %s is %d; a suite is four programs of one major",
				dir, suiteBinaries[0], majors[0], suiteBinaries[i], majors[i]))
		}
	}
	if want, ok := dirMajor(dir); ok && want != majors[0] {
		return Suite{}, pluginerrors.Permanent(fmt.Errorf(
			"client suite %s names major %d but its programs report %d: the directory's contents do not match its version",
			dir, want, majors[0]))
	}
	return Suite{Major: majors[0], dir: dir, paths: paths}, nil
}

// suiteOnPath treats a PATH holding all four programs as one more candidate —
// the deployment that installs a suite the ordinary way, with no configured
// directory. PATH is shared with unrelated programs, so a partial match is not
// a candidate at all: one stray psql on a developer's PATH must not fail an
// image whose configured suites are complete. A complete PATH suite is
// validated exactly like a directory one.
func suiteOnPath(ctx context.Context) (Suite, bool, error) {
	paths := make(map[string]string, len(suiteBinaries))
	for _, bin := range suiteBinaries {
		path, err := exec.LookPath(bin)
		if err != nil {
			return Suite{}, false, nil
		}
		paths[bin] = path
	}
	majors := make([]int, 0, len(suiteBinaries))
	for _, bin := range suiteBinaries {
		major, err := binaryMajor(ctx, paths[bin])
		if err != nil {
			return Suite{}, false, err
		}
		majors = append(majors, major)
	}
	for i := 1; i < len(majors); i++ {
		if majors[i] != majors[0] {
			return Suite{}, false, pluginerrors.Permanent(fmt.Errorf(
				"PATH completes a PostgreSQL client suite but mixes majors: %s is %d and %s is %d; install one major or name the suite's directory in client_dirs",
				suiteBinaries[0], majors[0], suiteBinaries[i], majors[i]))
		}
	}
	return Suite{Major: majors[0], paths: paths}, true, nil
}

// binaryMajor reads a client program's major from its own --version output.
func binaryMajor(ctx context.Context, path string) (int, error) {
	cmd := exec.CommandContext(ctx, path, "--version")
	stderr := extcmd.NewBoundedBuffer()
	cmd.Stderr = stderr
	out, err := cmd.Output()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, pluginerrors.Permanent(fmt.Errorf("%s --version: %w: %s", path, err, stderr.String()))
	}
	m := versionRe().FindStringSubmatch(string(out))
	if m == nil {
		return 0, pluginerrors.Permanent(fmt.Errorf("cannot read a version from %s --version output %q", path, strings.TrimSpace(string(out))))
	}
	major, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, pluginerrors.Permanent(fmt.Errorf("%s --version version %q is not a number: %w", path, m[1], err))
	}
	return major, nil
}

// dirMajor reads the major a suite directory's own name states: a trailing
// number, like postgresql18. A custom directory may state none, and then only
// the programs' mutual agreement is checked.
func dirMajor(dir string) (int, bool) {
	m := dirMajorRe().FindStringSubmatch(filepath.Base(dir))
	if m == nil {
		return 0, false
	}
	major, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return major, true
}

func newest(suites []Suite) Suite {
	out := suites[0]
	for _, s := range suites[1:] {
		if s.Major > out.Major {
			out = s
		}
	}
	return out
}

func majorList(suites []Suite) string {
	seen := map[int]bool{}
	var majors []string
	for _, s := range suites {
		if seen[s.Major] {
			continue
		}
		seen[s.Major] = true
		majors = append(majors, strconv.Itoa(s.Major))
	}
	return strings.Join(majors, ", ")
}

func quotedDirs(dirs []string) string {
	out := make([]string, len(dirs))
	for i, dir := range dirs {
		out[i] = strconv.Quote(dir)
	}
	return strings.Join(out, ", ")
}

var (
	versionRe  = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`(\d+)\.`) })
	dirMajorRe = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`([0-9]+)$`) })
)
