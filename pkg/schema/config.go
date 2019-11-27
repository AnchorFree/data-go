package schema

type Config struct {
	ConsulAddress        string   `yaml:"consul_address"`
	ConsulKeyPath        string   `yaml:"consul_key_path"`
	InvalidMessagesTopic string   `yaml:"invalid_messages_topic"`
	ValidateTopics       []string `yaml:"validate_topics"`
	PropertyName         string   `yaml:"property_name"`
}
