package consul

import (
	"github.com/hashicorp/consul/api"
)

func NewClient(address string) (*api.Client, error) {
	config := api.DefaultConfig()
	config.Address = address
	return api.NewClient(config)
}
