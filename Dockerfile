# syntax=docker/dockerfile:1.7

ARG NODE_VERSION=20-alpine
ARG GO_VERSION=1.25-alpine

# UI builder
FROM node:${NODE_VERSION} AS ui-build

WORKDIR /ui

COPY ui/package.json ui/package-lock.json ./
RUN npm ci

COPY ui ./
RUN npm run build

# Builder
FROM golang:${GO_VERSION} AS build

WORKDIR /src

ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
COPY --from=ui-build /ui/dist /src/ui/dist

ARG VERSION=dev
ARG COMMIT=unknown

RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
      -ldflags "-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
      -o /out/driftqd ./cmd/driftqd

RUN mkdir -p /out/data && chown 65532:65532 /out/data && chmod 0755 /out/data

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build --chown=65532:65532 /out/driftqd /driftqd
COPY --from=build --chown=65532:65532 /out/data /data

EXPOSE 8080

ENTRYPOINT ["/driftqd"]
CMD ["-addr", ":8080", "-wal", "/data/driftq.wal"]
