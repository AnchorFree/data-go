package grpc_client

import (
	"github.com/grpc-ecosystem/go-grpc-prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()
)

func (c *ClientGRPC) RegisterMetrics() {
	// Register standard metrics and customized metrics to registry.
	c.Prom.MustRegister(grpcMetrics)
}
