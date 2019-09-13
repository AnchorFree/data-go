package event_selector

type Selectors struct {
	Selectors []Selector `yaml:"selectors"`
}

type Selector struct {
	TargetTopic string            `yaml:"target_topic"`
	Matching    map[string]string `yaml:"matching"`
}
