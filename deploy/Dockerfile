# syntax=docker/dockerfile:1.7

# ---- builder ----------------------------------------------------------------
# Pin Go to match go.mod's `go 1.25`. Distroless static at runtime → no glibc.
FROM golang:1.25-bookworm AS builder

WORKDIR /src

# Cache deps separately so source-only edits don't redownload modules.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy sources. config/config.yaml ships with the binary as the default
# (embedded via runtime.Caller in config.Load) but we also bake it under
# /app/config/config.yaml so containers without MASTER_CONFIG still work.
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ENV CGO_ENABLED=0 GOFLAGS=-trimpath

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build \
        -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
        -o /out/master ./cmd/server

# ---- runtime ----------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/master /app/master
COPY --from=builder /src/config/config.yaml /app/config/config.yaml

# Non-root user from distroless. /dev/shm/bridge needs to be writable for FIFOs;
# in Docker mount it as tmpfs in compose (see deploy/docker-compose.yml).
USER nonroot:nonroot

EXPOSE 8083
ENTRYPOINT ["/app/master"]
