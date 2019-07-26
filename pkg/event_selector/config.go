package event_selector

type Config struct {
	EventSelectors []SelectorConfig `json:"event_selectors"`
}

type SelectorConfig struct {
	TargetTopic string            `json:"target_topic"`
	Selectors   map[string]string `json:"selectors"`
}
