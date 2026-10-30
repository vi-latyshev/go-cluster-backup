# The project's entry point. Every target name here is a contract: CLAUDE.md
# documents them, .github/workflows/ci.yml calls them, and a contributor runs
# `make check` before handing work over.
#
# Every tool but helm and k3d is a tool dependency of tools/go.mod, so its
# version is pinned by that module and no target needs an install step.

SHELL := /bin/sh

GO ?= go
TOOLS := -modfile=tools/go.mod

CHART := charts/go-cluster-backup
HELM_RELEASE ?= check
CHART_EXAMPLES := minimal k3s-single-node
KUBERNETES_VERSIONS := 1.31.0 1.36.0

# Coverage: `make test` gates the unit profile, `make cover` gates the union of
# the unit and e2e profiles. Both use the same reporter and the same threshold.
COVERAGE_THRESHOLD ?= 100
COVERAGE_PROFILE ?= coverage.out
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
#   E2E_DOCKER  the build command, so CI can pass buildx through with a layer
#               cache: `E2E_DOCKER="docker buildx build --load --cache-from …"`
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

.PHONY: check vet test unit-cover test-gate cover lint vuln chart docs e2e image

# check is the whole gate, in the order a failure is cheapest to act on.
check: vet test lint vuln chart

vet:
	$(GO) vet ./...

# test runs the unit suite and enforces the coverage gate on the unit profile.
# `make cover` is the full gate — the union of unit and e2e coverage.
test: unit-cover test-gate

# unit-cover runs the unit suite and writes the unit profile without gating it.
# GORACE=atexit_sleep_ms=0 drops the race runtime's one-second sleep at every
# test binary's exit — one per package, and the suite is dozens of packages.
unit-cover:
	GORACE=atexit_sleep_ms=0 $(GO) test -race -timeout=5m -coverprofile=$(COVERAGE_PROFILE) ./...

# test-gate enforces the threshold on an existing unit profile, without running
# the suite again — the same reporter `make test` uses.
test-gate:
	@COVERAGE_THRESHOLD=$(COVERAGE_THRESHOLD) $(COVERAGE_REPORT) $(COVERAGE_PROFILE)

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
	@COVERAGE_THRESHOLD=$(COVERAGE_THRESHOLD) $(COVERAGE_REPORT) $(COVERAGE_PROFILE) $(E2E_COVERAGE_PROFILE)

lint:
	$(GO) tool $(TOOLS) golangci-lint run
	@if git grep -nE '$(NOLINT_PATTERN)' -- '*.go'; then \
		echo "lint: //nolint directives are banned in this repository (see CONTRIBUTING.md)"; \
		exit 1; \
	fi

vuln:
	$(GO) tool $(TOOLS) govulncheck ./...

# chart lints and renders both examples, validates every rendered object
# against the Kubernetes API schema of each version the chart claims
# (kubeVersion >=1.31.0-0), and proves the generated README is not stale.
chart:
	@for example in $(CHART_EXAMPLES); do \
		echo "helm lint --strict ($(CHART), $$example)"; \
		helm lint --strict $(CHART) \
			-f examples/values/$$example.yaml \
			--set-file config=examples/config/$$example.yaml || exit 1; \
	done
	@for version in $(KUBERNETES_VERSIONS); do \
		for example in $(CHART_EXAMPLES); do \
			echo "kubeconform ($$example, kubernetes $$version)"; \
			helm template $(HELM_RELEASE) $(CHART) \
				-f examples/values/$$example.yaml \
				--set-file config=examples/config/$$example.yaml \
			| $(GO) tool $(TOOLS) kubeconform -strict -summary -kubernetes-version $$version - || exit 1; \
		done; \
	done
	@$(GO) tool $(TOOLS) helm-docs --chart-search-root charts --sort-values-order file --dry-run \
		| diff -u $(CHART)/README.md - > /dev/null \
		|| { echo "the chart README is stale: run 'make docs' and commit the result"; exit 1; }

# docs regenerates the chart README from values.yaml and README.md.gotmpl.
docs:
	$(GO) tool $(TOOLS) helm-docs --chart-search-root charts --sort-values-order file

# e2e runs the end-to-end suite against a cluster the CI job (or a developer)
# created and loaded E2E_IMAGE into. The suite reads:
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
	GCB_E2E=1 \
	GCB_E2E_IMAGE=$(E2E_IMAGE_NAME) \
	GCB_E2E_CLUSTER=$(E2E_CLUSTER) \
	GCB_E2E_COVERDIR=$(abspath $(E2E_COVERDIR)) \
		$(GO) test -count=1 -timeout=20m ./test/e2e/...
	@if [ -n "$$(ls -A $(E2E_COVERDIR) 2>/dev/null)" ]; then \
		$(GO) tool covdata textfmt -i=$(E2E_COVERDIR) -o=$(E2E_COVERAGE_PROFILE); \
	else \
		echo "e2e: $(E2E_COVERDIR) is empty; no e2e coverage profile written"; \
	fi

# image builds the production image. Coverage instrumentation is opt-in through
# the COVER build arg, used by the e2e job only.
image:
	docker build -t $(IMAGE) .
