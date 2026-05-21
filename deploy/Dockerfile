# Multi-stage build: compile the static binary in a Go toolchain image, then
# copy into a minimal distroless runtime. Final image is ~25 MB and contains
# no shell, no package manager, no userland — just the binary and its config
# directories.

FROM golang:1.24-alpine AS build

WORKDIR /src

# Cache module downloads in a dedicated layer so source edits don't bust
# the dependency cache.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
ARG COMMIT=unknown

# CGO_ENABLED=0 produces a fully static binary so the distroless static image
# can run it without glibc. Strip the symbol table to trim a few MB.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.GitCommit=${COMMIT}" \
    -o /out/l7rp \
    ./cmd/l7rp

# distroless/static is the minimum surface needed: ca-certificates, /etc/passwd,
# /tmp. No shell. No package manager. No interactive debugging — operators
# attach a debug sidecar in production.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/l7rp /usr/local/bin/l7rp

USER nonroot:nonroot

# Expose the conventional data and metrics ports. Operators override via the
# config file; these declarations are informational for image consumers.
EXPOSE 8080 8443 9090

ENTRYPOINT ["/usr/local/bin/l7rp"]
CMD ["--config", "/etc/l7rp/config.yaml"]
