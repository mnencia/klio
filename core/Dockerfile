# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

ARG BASE_IMAGE=debian:13.6-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

# Build Kopia on the host platform for the target platform
FROM --platform=${BUILDPLATFORM} golang:1.27.1@sha256:512690a5660563b57d37ecc31129e7f136e831db2aed24a1dbeb8ad7380dc0fa AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Build the Kopia CLI from the fork, pinned to the same commit as the library
# replace directive in kopia-pgbasebackup/go.mod. Pinning to a commit (rather
# than a branch tip, which can move) keeps the CLI and the library at identical
# fork state and makes the image reproducible.
ARG KOPIA_FORK_COMMIT=98edd76bc877345e8a38171631e43ec998831c29
RUN --mount=type=cache,target=/go/pkg/mod,id=kopia-mod-${TARGETARCH} \
    --mount=type=cache,target=/root/.cache/go-build,id=kopia-build-${TARGETARCH} \
    mkdir kopia && cd kopia && \
    git init -q && \
    git remote add origin https://github.com/leonardoce/kopia && \
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
