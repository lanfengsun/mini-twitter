# syntax=docker/dockerfile:1
# One Dockerfile for every Go binary: docker build --build-arg SERVICE=api|worker|seed .
FROM golang:1.24-alpine AS build
ARG SERVICE
WORKDIR /src
COPY . .
# `go mod tidy` resolves indirect deps and writes go.sum inside the build, so the
# repo builds even before a go.sum has been committed (`make tidy` creates it).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/svc ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/svc /app/svc
ENTRYPOINT ["/app/svc"]
