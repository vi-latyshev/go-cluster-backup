# The image ships the generic distribution: every plugin under internal/plugins
# plus the k3s_state source. The build is multi-platform: the builder runs on the
# build host's platform and cross-compiles the target.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.4-alpine@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS builder
ARG TARGETOS
ARG TARGETARCH
# COVER=1 instruments the binary for coverage collection (it writes counters to
# $GOCOVERDIR). It is empty for every production build: the flag expands to
# nothing, so the compiled binary — and therefore every layer — is identical to
# a build that never heard of this argument.
ARG COVER=""
WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,id=go-mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build,id=go-build \
    --mount=type=cache,target=/go/pkg/mod,id=go-mod \
    cover=""; \
    if [ "$COVER" = "1" ]; then cover="-cover -covermode=atomic"; fi; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build $cover -trimpath -ldflags="-s -w" -o /out/go-cluster-backup ./cmd/go-cluster-backup

# PostgreSQL 15 is not in Alpine 3.24's package index (16 through 18 are), so
# the 15 client comes from the official image, digest-pinned by tag *and*
# index. Renovate's allowedVersions keeps this stage on 15: a directory a
# newer major is copied into is a broken client suite the image check below
# refuses, because a client's major must match its own directory's name.
FROM docker.io/library/postgres:15.19-alpine3.24@sha256:eab009ce9447a891d9482702d370bbf132c18563bb87cfdcc2b815c6d3b0372f AS postgres15

# The 15 client's binaries are linked against their own libpq, which the target
# image does not carry and the Alpine libpq of another major does not replace.
# The lib moves beside the binaries and the build-host's patchelf writes an
# $ORIGIN-relative rpath into each one — a header edit of the target ELF, so it
# needs no emulation of the target platform.
FROM --platform=$BUILDPLATFORM docker.io/library/alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS postgres15client
RUN apk add --no-cache patchelf
COPY --from=postgres15 /usr/local/bin/pg_dump /usr/local/bin/pg_dumpall /usr/local/bin/pg_restore /usr/local/bin/psql /pg15/bin/
COPY --from=postgres15 /usr/local/lib/libpq.so.5.15 /pg15/lib/
RUN ln -s libpq.so.5.15 /pg15/lib/libpq.so.5 && \
    chmod 755 /pg15/bin/* && \
    for bin in /pg15/bin/*; do patchelf --set-rpath '$ORIGIN/lib' "$bin"; done && \
    for bin in /pg15/bin/*; do echo "$bin: $(patchelf --print-rpath "$bin")"; done

FROM docker.io/library/alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

# The external clients the source plugins shell out to — PostgreSQL 16, 17 and
# 18 from the Alpine index — redis-cli, sqlite3, CA certificates for every
# HTTPS storage, and tzdata so a retention policy's IANA timezone resolves
# beyond UTC. krb5-libs, libldap, libedit and their dependencies are the
# runtime libraries the copied PostgreSQL 15 client loads: its libpq was built
# with GSSAPI and LDAP, and its psql with libedit. age and zstd are libraries
# of the binary, not binaries of the image.
RUN apk add --no-cache \
        postgresql16-client \
        postgresql17-client \
        postgresql18-client \
        redis \
        sqlite \
        tzdata \
        ca-certificates \
        krb5-libs \
        libldap \
        libedit

# The copied 15 client lives where the client_dirs defaults name it, and every
# suite the image carries is proved runnable: all four programs print their
# version, and ldd must resolve the copied libpq for the 15 client rather than
# silently borrowing another major's. This is the image's own proof; doctor
# proves the same directories from the merged configuration at the end of the
# build.
COPY --from=postgres15client /pg15/bin/ /usr/libexec/postgresql15/
COPY --from=postgres15client /pg15/lib/ /usr/libexec/postgresql15/lib/
RUN for dir in /usr/libexec/postgresql15 /usr/libexec/postgresql16 /usr/libexec/postgresql17 /usr/libexec/postgresql18; do \
        for bin in pg_dump pg_dumpall pg_restore psql; do "$dir/$bin" --version; done; \
        ldd "$dir/pg_dump"; \
    done

# uid/gid 1000 — unclaimed; gid 999 is taken by ping.
RUN addgroup -g 1000 backup && \
    adduser -D -u 1000 -G backup -h /home/backup -s /sbin/nologin backup

COPY --from=builder /out/go-cluster-backup /usr/local/bin/go-cluster-backup

USER 1000:1000

# An image missing a client any registered plugin requires cannot be built.
RUN go-cluster-backup doctor

ENTRYPOINT ["/usr/local/bin/go-cluster-backup"]
