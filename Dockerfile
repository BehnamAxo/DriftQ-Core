# syntax=docker/dockerfile:1.7

# Builder
ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
      -ldflags "-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
      -o /out/driftqd ./cmd/driftqd

RUN mkdir -p /out/data && chown 65532:65532 /out/data && chmod 0755 /out/data

FROM gcr.io/distroless/static:nonroot

COPY --from=build --chown=65532:65532 /out/driftqd /driftqd
COPY --from=build --chown=65532:65532 /out/data /data

EXPOSE 8080

ENTRYPOINT ["/driftqd"]
CMD ["-addr", ":8080", "-wal", "/data/driftq.wal"]
