package event_selector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventSelector_ApplyConfig(t *testing.T) {
	config := Config{
		ConsulAddress: "",
		ConsulKeyPath: "",
	}
	es := NewEventSelector(config)
	selectors := &Selectors{
		Selectors: []Selector{
			{
				TargetTopic: "test",
				Matching: map[string]string{
					"test": "test",
				},
			},
		},
	}
	es.ApplySelectors(selectors)
	assert.Equal(t, *selectors, *es.selectors)
}
