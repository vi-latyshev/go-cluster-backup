# The project's entry point. Every target name here is a contract: CLAUDE.md
# documents them, .github/workflows/ci.yml calls them, and a contributor runs
# `make check` before handing work over.
#
# Every tool but k3d, kubectl and docker is a tool dependency of tools/go.mod,
# so its version is pinned by that module and no target needs an install step.
# helm is one of them. k3d, kubectl and docker are host tools of the e2e suite.

SHELL := /bin/sh

# Every target runs the toolchain the go.mod `go` directive pins, whatever Go is
# installed: the first run downloads it when the local Go is older. There is no
# `+auto`, so a tools/go.mod newer than go.mod fails loudly instead of silently
# selecting another toolchain. `make GOTOOLCHAIN=local ...` is the escape hatch.
export GOTOOLCHAIN := go$(shell sed -n 's/^go //p' go.mod)

GO ?= go
TOOLS := -modfile=tools/go.mod

# The tooling module is checked exactly like the application: it is a separate
# module, so its targets enter the module from its own directory.
TOOLS_MODULE := tools

CHART := charts/go-cluster-backup
HELM_RELEASE ?= check
CHART_EXAMPLES := minimal k3s-single-node
KUBERNETES_VERSIONS := 1.31.0 1.36.0 1.37.0

# kubeconform downloads a schema per kind and version. The schemas are pinned to
# one commit of yannh/kubernetes-json-schema, so a tag moving under the upstream
# `master` branch cannot change what CI validated against, and a local cache
# makes a warm run work offline. CI caches the same directory.
KUBECONFORM_CACHE ?= $(HOME)/.cache/kubeconform
KUBECONFORM_SCHEMAS := e0c72a1bc5f409d1685c0e1323460c8a98cd7459
KUBECONFORM_SCHEMA_LOCATION := https://raw.githubusercontent.com/yannh/kubernetes-json-schema/$(KUBECONFORM_SCHEMAS)/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json

# The chart tests validate with the same pinned schemas and cache as the chart
# target; exporting the pair keeps the pin in one place for every recipe that
# runs `go test`, `test/chart` included.
export KUBECONFORM_SCHEMA_LOCATION KUBECONFORM_CACHE

# Coverage: `make test` gates the unit profile, `make cover` gates the union of
# the unit and e2e profiles. Both use the same reporter and the same threshold.
COVERAGE_THRESHOLD ?= 100
COVERAGE_PROFILE ?= coverage.out
TOOLS_COVERAGE_PROFILE ?= coverage-tools.out
COMBINED_COVERAGE_PROFILE ?= coverage-combined.out
E2E_COVERAGE_PROFILE ?= coverage-e2e.out
COVERAGE_REPORT := scripts/coverage-report.sh

# e2e runs the end-to-end suite (test/e2e), which owns the cluster: it creates
# the k3d cluster itself — with the cover directory bind-mounted into the node —
# imports the image under test and the dependency images, and deletes the
# cluster again. This target builds that image and converts the counters the
# suite collects; it never creates a cluster, so a local run and a CI run pass
# the suite the same flags.
#
#   E2E_IMAGE   an image already in the local docker daemon; unset builds
#               go-cluster-backup:e2e with COVER=1 so coverage can be collected
#   E2E_DOCKER  the build command, so CI can load the image through buildx:
#               `E2E_DOCKER="docker buildx build --load"`; the BuildKit cache
#               mounts travel separately, through buildkit-cache-dance
#
# The cover directory must exist when the cluster is created and must not be
# replaced while the cluster lives — the node's bind mount names the inode — so
# it is cleared here, before the suite creates anything.
E2E_IMAGE ?=
E2E_DOCKER ?= docker build
E2E_CLUSTER ?= gcb-e2e
E2E_COVERDIR ?= coverage-e2e
E2E_IMAGE_NAME = $(if $(E2E_IMAGE),$(E2E_IMAGE),go-cluster-backup:e2e)

# The repository bans `//nolint` outright. golangci-lint v2.15.0's nolintlint
# has no option to forbid a directive (its settings are require-explanation,
# require-specific, allow-no-explanation and allow-unused), so the ban is this
# grep; .golangci.yml keeps nolintlint at its strictest as a second line.
NOLINT_PATTERN := //[[:space:]]*nolint([[:space:]:,]|$$)

IMAGE ?= go-cluster-backup

.PHONY: check vet test unit-cover tools-cover test-gate cover lint rules vuln chart docs e2e image hooks generate

# check is the whole gate, in the order a failure is cheapest to act on.
check: vet test lint vuln chart

vet:
	$(GO) vet ./...
	cd $(TOOLS_MODULE) && $(GO) vet ./...

# test runs both suites and enforces the coverage gate on their union.
# `make cover` is the full gate — that union plus the e2e coverage.
test: unit-cover tools-cover test-gate

# unit-cover runs the unit suite and writes the unit profile without gating it.
# GORACE=atexit_sleep_ms=0 drops the race runtime's one-second sleep at every
# test binary's exit — one per package, and the suite is dozens of packages.
# -trimpath keeps profile keys at import paths and lets the build cache be
# shared between a worktree and its main checkout.
unit-cover:
	@$(GO) tool -n $(TOOLS) helm > /dev/null
	GORACE=atexit_sleep_ms=0 $(GO) test -race -trimpath -timeout=5m -coverprofile=$(COVERAGE_PROFILE) ./...

# tools-cover runs the tooling module's suite. Its own profile is separate
# because the module is: test-gate concatenates the two disjoint block sets.
tools-cover:
	cd $(TOOLS_MODULE) && GORACE=atexit_sleep_ms=0 $(GO) test -race -trimpath -timeout=5m -coverprofile=../$(TOOLS_COVERAGE_PROFILE) ./...

# test-gate enforces the threshold on the existing profiles, without running a
# suite again — the same reporter `make test` uses. The application and tooling
# modules hold disjoint packages, so their profiles concatenate into one block
# set; the reporter merges blocks by key as usual.
test-gate:
	@test -f $(TOOLS_COVERAGE_PROFILE) || { echo "test-gate: $(TOOLS_COVERAGE_PROFILE) is missing: run 'make test'"; exit 1; }
	@{ echo "mode: set"; tail -n +2 $(COVERAGE_PROFILE); tail -n +2 $(TOOLS_COVERAGE_PROFILE); } > $(COMBINED_COVERAGE_PROFILE)
	@COVERAGE_THRESHOLD=$(COVERAGE_THRESHOLD) $(COVERAGE_REPORT) $(COMBINED_COVERAGE_PROFILE)

# cover merges the unit profile and the e2e profile and enforces the threshold
# on the union. The unit profile owns the denominator — the blocks of the
# platform the unit suite ran on — and the e2e profile only marks those blocks
# covered, so a block the e2e side brings from a file this platform does not
# build cannot enter the set. Run `make test` and `make e2e` first.
cover:
	@test -f $(COVERAGE_PROFILE) || { echo "cover: $(COVERAGE_PROFILE) is missing: run 'make test' first"; exit 1; }
	@test -f $(E2E_COVERAGE_PROFILE) || { \
		if [ -d $(E2E_COVERDIR) ]; then \
			echo "cover: the last 'make e2e' run did not finish: $(E2E_COVERDIR)/ holds counters no run converted, and no $(E2E_COVERAGE_PROFILE) was written. Run 'make e2e' again."; \
		else \
			echo "cover: $(E2E_COVERAGE_PROFILE) is missing: run 'make e2e' first"; \
		fi; \
		exit 1; \
	}
	@test -f $(TOOLS_COVERAGE_PROFILE) || { echo "cover: $(TOOLS_COVERAGE_PROFILE) is missing: run 'make test' first"; exit 1; }
	@{ echo "mode: set"; tail -n +2 $(COVERAGE_PROFILE); tail -n +2 $(TOOLS_COVERAGE_PROFILE); } > $(COMBINED_COVERAGE_PROFILE)
	@COVERAGE_THRESHOLD=$(COVERAGE_THRESHOLD) $(COVERAGE_REPORT) $(COMBINED_COVERAGE_PROFILE) $(E2E_COVERAGE_PROFILE)

# rules proves the repository's structural invariants and the freshness of the
# generated composition root. Both are tracked-file checks no compiler runs:
# the generated copies must match their sources, and a plugin must obey the
# protocol and the Go internal-boundary rule.
rules:
	cd $(TOOLS_MODULE) && $(GO) run ./rules -root ..
	cd $(TOOLS_MODULE) && $(GO) run ./gen -root .. -check

lint: rules
	$(GO) tool $(TOOLS) golangci-lint run
	cd $(TOOLS_MODULE) && $(GO) tool golangci-lint run ./...
	@if git grep -nE '$(NOLINT_PATTERN)' -- '*.go'; then \
		echo "lint: //nolint directives are banned in this repository (see CONTRIBUTING.md)"; \
		exit 1; \
	fi

vuln:
	$(GO) tool $(TOOLS) govulncheck ./...
	cd $(TOOLS_MODULE) && $(GO) tool govulncheck ./...

# generate rewrites the tracked composition root and Floor manifest from the
# plugin packages. The output is committed, so a build never needs it.
generate:
	cd $(TOOLS_MODULE) && $(GO) run ./gen -root ..

# chart lints both examples, renders each of them once, validates every rendered
# object against the Kubernetes API schema of each version the chart claims
# (kubeVersion >=1.31.0-0), and proves the generated README is not stale. A kind
# no pinned schema carries is an error: -ignore-missing-schemas is not passed.
chart:
	@set -e; \
	helm="$$($(GO) tool $(TOOLS) -n helm)"; \
	tmp="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp"' EXIT; \
	mkdir -p $(KUBECONFORM_CACHE); \
	for example in $(CHART_EXAMPLES); do \
		echo "helm lint --strict ($(CHART), $$example)"; \
		"$$helm" lint --strict $(CHART) \
			-f examples/values/$$example.yaml \
			--set-file config=examples/config/$$example.yaml || exit 1; \
		"$$helm" template $(HELM_RELEASE) $(CHART) \
			-f examples/values/$$example.yaml \
			--set-file config=examples/config/$$example.yaml \
			> "$$tmp/$$example.yaml" || exit 1; \
	done; \
	for version in $(KUBERNETES_VERSIONS); do \
		echo "kubeconform (both examples, kubernetes $$version)"; \
		$(GO) tool $(TOOLS) kubeconform -strict -summary \
			-cache $(KUBECONFORM_CACHE) \
			-schema-location '$(KUBECONFORM_SCHEMA_LOCATION)' \
			-kubernetes-version $$version "$$tmp" || exit 1; \
	done
	@$(GO) tool $(TOOLS) helm-docs --chart-search-root charts --sort-values-order file --dry-run \
		| diff -u $(CHART)/README.md - > /dev/null \
		|| { echo "the chart README is stale: run 'make docs' and commit the result"; exit 1; }

# docs regenerates the chart README from values.yaml and README.md.gotmpl.
docs:
	$(GO) tool $(TOOLS) helm-docs --chart-search-root charts --sort-values-order file

# e2e builds the image under test (unless E2E_IMAGE names one already in the
# docker daemon) and runs the end-to-end suite, which creates and deletes its
# own cluster and loads the image into it. The suite reads:
#
#   GCB_E2E=1                enables it; without it the package skips
#   GCB_E2E_IMAGE            the image reference loaded into the cluster
#   GCB_E2E_CLUSTER          the k3d cluster name (default gcb-e2e)
#   GCB_E2E_COVERDIR         a host directory the suite copies the container's
#                            GOCOVERDIR into; this target converts it afterwards
e2e:
	@if [ -z "$(E2E_IMAGE)" ]; then \
		echo "$(E2E_DOCKER) -t $(E2E_IMAGE_NAME) --build-arg COVER=1 ."; \
		$(E2E_DOCKER) -t $(E2E_IMAGE_NAME) --build-arg COVER=1 . || exit 1; \
	fi
	@# The node bind-mounts the cover directory by inode, so a cluster left over
	@# from an earlier run must go before the directory is cleared: a fresh
	@# directory of the same name leaves every pod failing to mount.
	@k3d cluster delete $(E2E_CLUSTER) > /dev/null 2>&1 || true
	@rm -rf $(E2E_COVERDIR) $(E2E_COVERAGE_PROFILE)
	@mkdir -p $(E2E_COVERDIR)
	@# Warm the pinned helm before the suite: its first `go tool` call compiles
	@# the tool, and the suite runs helm under a three-minute exec limit.
	@$(GO) tool $(TOOLS) helm version > /dev/null
	GCB_E2E=1 \
	GCB_E2E_IMAGE=$(E2E_IMAGE_NAME) \
	GCB_E2E_CLUSTER=$(E2E_CLUSTER) \
	GCB_E2E_COVERDIR=$(abspath $(E2E_COVERDIR)) \
		$(GO) test -count=1 -timeout=20m -v ./test/e2e/...
	@if [ -n "$$(ls -A $(E2E_COVERDIR) 2>/dev/null)" ]; then \
		$(GO) tool covdata textfmt -i=$(E2E_COVERDIR) -o=$(E2E_COVERAGE_PROFILE); \
	else \
		echo "e2e: $(E2E_COVERDIR) is empty; no e2e coverage profile written"; \
	fi

# image builds the production image. Coverage instrumentation is opt-in through
# the COVER build arg, used by the e2e job only.
image:
	docker build -t $(IMAGE) .

# hooks opts this checkout into the committed git hooks. It is opt-in and
# idempotent: git never installs hooks on clone, and CI is the real gate.
hooks:
	git config core.hooksPath .githooks
