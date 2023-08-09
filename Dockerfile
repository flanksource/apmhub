FROM golang:1.21@sha256:a0e3e6859220ee48340c5926794ce87a891a1abb51530573c694317bf8f72543 as builder
WORKDIR /app
ARG VERSION
COPY go.mod /app/go.mod
COPY go.sum /app/go.sum
RUN go mod download
COPY ./ ./
WORKDIR /app
RUN go version
RUN make build

FROM ubuntu:jammy@sha256:0bced47fffa3361afa981854fcabcd4577cd43cebbb808cea2b1f33a3dd7f508
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
