package event_selector

type Config struct {
	ConsulAddress string `yaml:"consul_address"`
	ConsulKeyPath string `yaml:"consul_key_path"`
}
