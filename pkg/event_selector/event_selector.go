package event_selector

import (
	"sync"
)

type EventSelector struct {
	sync.RWMutex
	config *Config
}

func NewEventSelector() *EventSelector {
	return &EventSelector{
		config: new(Config),
	}
}

func (es *EventSelector) ApplyConfig(config *Config) {
	es.Lock()
	defer es.Unlock()
	es.config = config
}
