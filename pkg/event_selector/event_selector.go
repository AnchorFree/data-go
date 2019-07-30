package event_selector

import (
	"sync"

	"github.com/valyala/fastjson"
)

type EventSelector struct {
	sync.RWMutex
	pPool  *fastjson.ParserPool
	aPool  *fastjson.ArenaPool
	config *Config
}

func NewEventSelector() *EventSelector {
	return &EventSelector{
		pPool:  new(fastjson.ParserPool),
		aPool:  new(fastjson.ArenaPool),
		config: new(Config),
	}
}

func (es *EventSelector) ApplyConfig(config *Config) {
	es.Lock()
	defer es.Unlock()
	es.config = config
}
