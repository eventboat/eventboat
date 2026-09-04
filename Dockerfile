# syntax=docker/dockerfile:1

# Build stage: static binary. CGO is off — every driver in go.mod is pure Go
# (SQLite via modernc.org/sqlite, Postgres via pgx, Kafka via kafka-go), so
# the result runs on distroless/static.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/eventboat ./cmd/eventboat \
    && mkdir -p /out/data /out/pipelines \
    && touch /out/data/.keep /out/pipelines/.keep

# Runtime stage: distroless static, non-root. /pipelines is the config-dir
# mount point and /data the SQLite volume (examples/k8s/deployment.yaml
# mounts exactly these two paths; the .keep placeholders carry the
# nonroot-owned directories through COPY and keep unmounted smoke runs
# working).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/eventboat /usr/local/bin/eventboat
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/pipelines /pipelines
USER nonroot
ENTRYPOINT ["/usr/local/bin/eventboat"]
# No CMD: a bare `docker run ghcr.io/eventboat/eventboat` prints the help
# screen and exits 0 (the CLI's bare-invocation behavior); pass verbs
# explicitly, e.g.  run --config-dir /pipelines
