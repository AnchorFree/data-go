# Build stage
FROM golang:1.12.7 AS builder

# Enable support of go modules by default
ENV GO111MODULE on
ENV BASE_DIR /go/src/data-go

# Install gosec
RUN wget -O - -q https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b /usr/bin v2.3.0

# Warming modules cache with project dependencies
WORKDIR ${BASE_DIR}
COPY go.mod go.sum ./
RUN go mod download

# Copy project source code to WORKDIR
COPY . .

# Run gosec
RUN gosec ./...

# Run tests and build on success
RUN go test -v ./...

# Final container stage
FROM alpine
RUN touch /test.OK
