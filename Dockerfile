# The image ships the generic distribution: every plugin under internal/plugins
# plus the k3s_state source. The build is multi-platform: the builder runs on the
# build host's platform and cross-compiles the target.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder
ARG TARGETOS
ARG TARGETARCH
# COVER=1 instruments the binary for coverage collection (it writes counters to
# $GOCOVERDIR). It is empty for every production build: the flag expands to
# nothing, so the compiled binary — and therefore every layer — is identical to
# a build that never heard of this argument.
ARG COVER=""
WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,id=go-mod-backup \
    go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build,id=go-build-backup \
    --mount=type=cache,target=/go/pkg/mod,id=go-mod-backup \
    cover=""; \
    if [ "$COVER" = "1" ]; then cover="-cover -covermode=atomic"; fi; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build $cover -trimpath -ldflags="-s -w" -o /out/go-cluster-backup ./cmd/go-cluster-backup

FROM docker.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# The external clients the source plugins shell out to (postgresql18-client,
# redis-cli, sqlite3), CA certificates for every HTTPS storage, and tzdata so a
# retention policy's IANA timezone resolves beyond UTC. age and zstd are
# libraries of the binary, not binaries of the image.
RUN --mount=type=cache,target=/var/cache/apk,id=apk-backup,sharing=locked \
    apk add --no-cache \
        postgresql18-client \
        redis \
        sqlite \
        tzdata \
        ca-certificates

# uid/gid 1000 — unclaimed; gid 999 is taken by ping.
RUN addgroup -g 1000 backup && \
    adduser -D -u 1000 -G backup -h /home/backup -s /bin/sh backup

COPY --from=builder /out/go-cluster-backup /usr/local/bin/go-cluster-backup

USER 1000:1000

# An image missing a client any registered plugin requires cannot be built.
RUN go-cluster-backup doctor

ENTRYPOINT ["/usr/local/bin/go-cluster-backup"]
