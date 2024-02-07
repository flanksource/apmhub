FROM golang:1.22@sha256:ef61a20960397f4d44b0e729298bf02327ca94f1519239ddc6d91689615b1367 as builder
WORKDIR /app
ARG VERSION
COPY go.mod /app/go.mod
COPY go.sum /app/go.sum
RUN go mod download
COPY ./ ./
WORKDIR /app
RUN go version
RUN make build

FROM ubuntu:jammy@sha256:ec050c32e4a6085b423d36ecd025c0d3ff00c38ab93a3d71a460ff1c44fa6d77
WORKDIR /app

# install CA certificates
RUN apt-get update && \
  apt-get install -y ca-certificates && \
  rm -Rf /var/lib/apt/lists/*  && \
  rm -Rf /usr/share/doc && rm -Rf /usr/share/man  && \
  apt-get clean

COPY --from=builder /app/.bin/apm-hub /app
ENV ASSUME_NO_MOVING_GC_UNSAFE_RISK_IT_WITH=go1.20
ENTRYPOINT ["/app/apm-hub"]
