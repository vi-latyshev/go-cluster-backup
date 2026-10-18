#!/bin/sh
# Coverage report and gate for one Go coverage profile, or for a union of two.
#
#   coverage-report.sh PROFILE [EXTRA]
#
# With one profile the block set is that profile's own: blocks are merged by key
# (file:start,end) first — a Go profile lists a block once per test binary that
# reported it — and each block's statements are counted once, covered when any
# copy of it has a positive run count.
#
# With two profiles the FIRST is the reference: it defines the block set the
# percentage is measured over, and the second only marks blocks of that set as
# covered. This is what makes the union meaningful — the unit profile describes
# the platform the unit suite ran on, and a block the other suite brings from a
# file this platform does not build (a `_linux.go` in a macOS run) must not
# enter the denominator, or the union would report less than the unit run it
# includes. Such blocks are counted and reported in one line, never summed.
#
# Prints a per-package table and the total; appends the same table to
# $GITHUB_STEP_SUMMARY as markdown when that is set; lists the uncovered blocks
# on stdout and exits 1 when the total is below $COVERAGE_THRESHOLD (default
# 100). A profile with no statements is a failure, not a pass.

set -eu

profile="${1:?usage: coverage-report.sh PROFILE [EXTRA]}"
extra="${2:-}"
threshold="${COVERAGE_THRESHOLD:-100}"
summary="${GITHUB_STEP_SUMMARY:-}"
module="github.com/vi-latyshev/go-cluster-backup"

for file in "$profile" ${extra:+"$extra"}; do
    if [ ! -f "$file" ]; then
        echo "coverage-report: $file does not exist" >&2
        exit 1
    fi
done

# Pass 1 merges the reference profile's blocks and aggregates per package into
# sortable records; the sort makes the table stable; pass 2 prints, writes the
# summary and gates.
awk -v module="$module" -v extra="$extra" '
    /^mode:/ { next }
    NF >= 3 && $(NF - 1) ~ /^[0-9]+$/ {
        key = $1
        if (!(key in stmts)) {
            stmts[key] = $(NF - 1)
            split(key, parts, ":")
            path[key] = parts[1]
            pos[key] = parts[2]
        }
        if ($NF > 0) covered[key] = 1
    }
    END {
        # The extra profile only marks the reference set as covered. A block of
        # it outside that set is another platform or another build, not a
        # statement this run failed to reach.
        ignored = 0
        if (extra != "") {
            while ((getline line < extra) > 0) {
                if (line ~ /^mode:/) continue
                if (split(line, fields, " ") != 3) continue
                if (fields[1] in stmts) {
                    if (fields[3] + 0 > 0) covered[fields[1]] = 1
                } else {
                    ignored++
                }
            }
            close(extra)
        }
        if (ignored > 0) {
            printf "coverage: %d blocks of the extra profile are outside the reference block set (another platform, or a file this run does not build); not counted\n", ignored > "/dev/stderr"
        }

        for (key in stmts) {
            pkg = path[key]
            if (index(pkg, module "/") == 1) pkg = substr(pkg, length(module) + 2)
            sub("/[^/]*$", "", pkg)
            total[pkg] += stmts[key]
            if (covered[key]) hit[pkg] += stmts[key]
            N += stmts[key]
            if (covered[key]) C += stmts[key]
            else print "uncovered " path[key] ":" pos[key]
        }
        for (pkg in total) printf "package %s %d %d\n", pkg, hit[pkg], total[pkg]
        printf "total %d %d\n", C, N
    }
' "$profile" | sort | awk -v threshold="$threshold" -v summary="$summary" '
    $1 == "package"   { n++; name[n] = $2; hit[n] = $3; total[n] = $4; next }
    $1 == "total"     { C = $2; N = $3; next }
    $1 == "uncovered" { missed++; block[missed] = $2; next }
    END {
        failed = (N == 0) || (C * 100 < threshold * N)
        pct = N ? int(C * 10000 / N) / 100 : 0

        printf "coverage per package:\n"
        for (i = 1; i <= n; i++) {
            printf "  %-46s %6d/%-6d %6.2f%%\n", name[i], hit[i], total[i],
                total[i] ? 100 * hit[i] / total[i] : 0
        }
        printf "coverage: %d/%d statements (%.2f%%) (threshold: %s%%)\n", C, N, pct, threshold

        if (failed) {
            printf "coverage: below the %s%% threshold; %d uncovered blocks:\n", threshold, missed
            for (i = 1; i <= missed; i++) print "  " block[i]
        }

        if (summary != "") {
            print "## Coverage" >> summary
            print "" >> summary
            print "| Package | Covered | Total | % |" >> summary
            print "|---|---:|---:|---:|" >> summary
            for (i = 1; i <= n; i++) {
                printf "| `%s` | %d | %d | %.2f%% |\n", name[i], hit[i], total[i],
                    total[i] ? 100 * hit[i] / total[i] : 0 >> summary
            }
            printf "| **total** | **%d** | **%d** | **%.2f%%** |\n", C, N, pct >> summary
            if (failed) {
                print "" >> summary
                printf "<details><summary>%d uncovered blocks</summary>\n\n", missed >> summary
                limit = missed < 100 ? missed : 100
                for (i = 1; i <= limit; i++) print "- `" block[i] "`" >> summary
                if (missed > limit) printf "- … and %d more (see the job log)\n", missed - limit >> summary
                print "" >> summary
                print "</details>" >> summary
            }
            close(summary)
        }

        exit failed ? 1 : 0
    }
'
