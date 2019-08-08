package event_selector

import (
	"encoding/json"
	"sync"

	"github.com/anchorfree/data-go/pkg/consul"
	"github.com/anchorfree/data-go/pkg/logger"
)

type EventSelector struct {
	sync.RWMutex
	selectors *Selectors
	config *Config
}

func NewEventSelector(config Config) (*EventSelector) {
	es := &EventSelector{
		selectors: new(Selectors),
		config: &config,
	}
	return es
}

func (es *EventSelector) ApplySelectors(selectors *Selectors) {
	es.Lock()
	defer es.Unlock()
	es.selectors = selectors
}

func (es *EventSelector) RunConfigWatcher() error {
	client, err := consul.NewClient(es.config.ConsulAddress)
	if err != nil {
		return err
	}
	watcher := consul.NewWatcher(client, nil)
	watcher.Watch(es.config.ConsulKeyPath, es.updateConfig)
	return nil
}

func (es *EventSelector) updateConfig(rawConfig []byte) error {
	selectors := &Selectors{}
	err := json.Unmarshal(rawConfig, selectors)
	if err != nil {
		return err
	}
	es.ApplySelectors(selectors)
	logger.Get().Info("Event selector selectors has been successfully updated")
	return nil
}
