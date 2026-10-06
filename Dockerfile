# syntax=docker/dockerfile:1.7

# Samakan dengan versi di go.mod.
ARG GO_VERSION=1.23

# ---------- Base ----------
# Build di arsitektur host lalu cross-compile ke target,
# jadi cepat di Mac Apple Silicon maupun di CI.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS base

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

# Layer dependency di-cache terpisah dari source code.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# ---------- Build ----------
# Build setiap binary yang ada di ./cmd/* (server, worker, publisher, ...),
# jadi binary baru otomatis ikut ter-build tanpa mengubah Dockerfile.
FROM base AS build

ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for dir in ./cmd/*/; do \
    app=$(basename "$dir"); \
    go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/${app} ./cmd/${app} || exit 1; \
    done

# ---------- Debug build ----------
# Tanpa optimasi & inlining agar breakpoint dan variabel terbaca di Delve.
FROM base AS build-debug

ARG DELVE_VERSION=v1.24.2
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install github.com/go-delve/delve/cmd/dlv@${DELVE_VERSION} && \
    # Saat cross-compile, go install menaruh binary di bin/${GOOS}_${GOARCH}.
    mkdir -p /out && cp $(find /go/bin -type f -name dlv | head -n1) /out/dlv

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for dir in ./cmd/*/; do \
    app=$(basename "$dir"); \
    go build -gcflags="all=-N -l" \
    -o /out/${app} ./cmd/${app} || exit 1; \
    done

# ---------- Debugger ----------
# Dipakai oleh `make debug` (PROFILE_DOCKERFILE_TARGET=debugger).
# Command compose (mis. /app/server) diteruskan ke `dlv exec`.
FROM alpine:3.20 AS debugger

WORKDIR /app
COPY --from=build-debug /out/ /app/

EXPOSE 8080 2345

ENTRYPOINT ["/app/dlv", "--listen=:2345", "--headless=true", "--api-version=2", "--accept-multiclient", "--continue", "exec"]
CMD ["/app/server"]

# ---------- Runtime ----------
# distroless/static sudah berisi ca-certificates dan tzdata,
# tanpa shell, dan berjalan sebagai non-root.
# Stage terakhir = target default bila `docker build` tanpa --target.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app
COPY --from=build /out/ /app/

USER nonroot:nonroot
EXPOSE 8080

# Default menjalankan server; worker & publisher memakai override command.
ENTRYPOINT []
CMD ["/app/server"]
