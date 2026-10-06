# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.23

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

# Docker CLI dibutuhkan oleh Dagger CLI/container workflow
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

# Cache dependency layer
COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Source
COPY . .

# Build Dagger binary
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/dagger \
    .

# ---------- Runtime ----------

FROM alpine:3.22 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    docker-cli

COPY --from=builder /out/dagger /usr/local/bin/dagger

RUN addgroup -S dagger \
    && adduser -S -G dagger dagger

USER dagger

ENTRYPOINT ["dagger"]