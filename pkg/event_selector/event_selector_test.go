package event_selector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventSelector_ApplyConfig(t *testing.T) {
	es := NewEventSelector()
	config := &Config{
		EventSelectors: []SelectorConfig{
			{
				TargetTopic: "test",
				Selectors: map[string]string{
					"test": "test",
				},
			},
		},
	}
	es.ApplyConfig(config)
	assert.Equal(t, *config, *es.config)
}
