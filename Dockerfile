# Build stage
FROM golang:1.12.7 AS builder

# Enable support of go modules by default
ENV GO111MODULE on
ENV BASE_DIR /go/src/data-go

# Warming modules cache with project dependencies
WORKDIR ${BASE_DIR}
COPY go.mod go.sum ./
RUN go mod download

# Copy project source code to WORKDIR
COPY . .

# Run tests and build on success
RUN go test -v ./...

# Final container stage
FROM alpine
RUN touch /test.OK
