# syntax=docker/dockerfile:1

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/leadgraph ./cmd/leadgraph

# Distroless, static, non-root. It has no browser: run fetch workers that need
# the headless fallback from an image that ships Chrome and set
# LEADGRAPH_FETCH_HEADLESS_EXEC_PATH.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/leadgraph /usr/local/bin/leadgraph
USER nonroot:nonroot
EXPOSE 9108
ENTRYPOINT ["/usr/local/bin/leadgraph"]
CMD ["run-all"]
