package event_selector

type Selectors struct {
	Selectors []Selector `json:"selectors"`
}

type Selector struct {
	TargetTopic string            `json:"target_topic"`
	Matching    map[string]string `json:"matching"`
}
