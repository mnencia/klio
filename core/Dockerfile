# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

ARG BASE_IMAGE=debian:13.7-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f

# Build Kopia on the host platform for the target platform
FROM --platform=${BUILDPLATFORM} golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Build the Kopia CLI from the fork, pinned to the same commit as the library
# replace directive in kopia-pgbasebackup/go.mod. Pinning to a commit (rather
# than a branch tip, which can move) keeps the CLI and the library at identical
# fork state and makes the image reproducible.
ARG KOPIA_FORK_COMMIT=5d68ae99563555aaf95ab162cd659e82aff844f7
RUN --mount=type=cache,target=/go/pkg/mod,id=kopia-mod-${TARGETARCH} \
    --mount=type=cache,target=/root/.cache/go-build,id=kopia-build-${TARGETARCH} \
    mkdir kopia && cd kopia && \
    git init -q && \
    git remote add origin https://github.com/cloudnative-pg/kopia && \
    git fetch -q --depth=1 origin "${KOPIA_FORK_COMMIT}" && \
    git checkout -q FETCH_HEAD && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -o kopia main.go

# Build the BASE_BACKUP ingestion helper from this repository. It consumes the
# Kopia library via a replace directive in its go.mod (pointing at the fork
# until the patches are merged upstream), so the Kopia Go library never enters
# the main klio binary.
COPY kopia-pgbasebackup/ /workspace/kopia-pgbasebackup/
RUN --mount=type=cache,target=/go/pkg/mod,id=kopia-mod-${TARGETARCH} \
    --mount=type=cache,target=/root/.cache/go-build,id=kopia-build-${TARGETARCH} \
    cd /workspace/kopia-pgbasebackup && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -o kopia-pgbasebackup .

FROM ${BASE_IMAGE} AS base
ARG TARGETARCH
ARG TARGETOS
# Copy CA certificates from builder image to support TLS connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/kopia/kopia /usr/bin/kopia
COPY --from=builder /workspace/kopia-pgbasebackup/kopia-pgbasebackup /usr/bin/kopia-pgbasebackup
COPY dist/klio_${TARGETOS}_${TARGETARCH}*/klio /usr/bin/klio
COPY licenses /licenses
ENTRYPOINT ["/usr/bin/klio"]
USER 26
