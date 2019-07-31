package event_selector

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/anchorfree/data-go/pkg/line_offset_reader"
)

type ER struct {
	name     string
	raw      []byte
	count    int
	topic    string
	esConfig *Config
}

var eventSelectorTest = []ER{
	{
		name:     "empty_config",
		raw:      []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count:    3,
		topic:    "test",
		esConfig: &Config{},
	},
	{
		name:  "single_event",
		raw:   []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count: 4,
		topic: "test",
		esConfig: &Config{
			EventSelectors: []SelectorConfig{
				{
					TargetTopic: "jtest",
					Selectors: map[string]string{
						"payload": "test1",
					},
				},
			},
		},
	},
	{
		name:  "all_events",
		raw:   []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count: 6,
		topic: "test",
		esConfig: &Config{
			EventSelectors: []SelectorConfig{
				{
					TargetTopic: "jtest",
					Selectors: map[string]string{
						"event": "test",
					},
				},
			},
		},
	},
	{
		name:  "equal_topics",
		raw:   []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count: 3,
		topic: "test",
		esConfig: &Config{
			EventSelectors: []SelectorConfig{
				{
					TargetTopic: "test",
					Selectors: map[string]string{
						"event": "test",
					},
				},
			},
		},
	},
	{
		name:  "several_selectors_per_target",
		raw:   []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count: 4,
		topic: "test",
		esConfig: &Config{
			EventSelectors: []SelectorConfig{
				{
					TargetTopic: "jtest",
					Selectors: map[string]string{
						"event": "test",
						"payload": "test2",
					},
				},
			},
		},
	},
	{
		name:  "several_event_selectors",
		raw:   []byte("{\"event\":\"test\",\"payload\":\"test1\"}\n{\"event\":\"test\",\"payload\":\"test2\"}\n{\"event\":\"test\",\"payload\":\"test\"}"),
		count: 5,
		topic: "test",
		esConfig: &Config{
			EventSelectors: []SelectorConfig{
				{
					TargetTopic: "jtest",
					Selectors: map[string]string{
						"event": "test",
						"payload": "test2",
					},
				},
				{
					TargetTopic: "atest",
					Selectors: map[string]string{
						"event": "test",
						"payload": "test1",
					},
				},
			},
		},
	},
}

func TestEventReader_ReadEvent(t *testing.T) {
	for testIdx, test := range eventSelectorTest {
		lor := line_offset_reader.NewEventReader(bytes.NewReader(test.raw), test.topic)
		es := NewEventSelector()
		es.ApplyConfig(test.esConfig)
		er := es.NewEventReader(lor)
		count := 0
		for {
			eventEntry := er.ReadEvent()
			count++
			if eventEntry.Error != nil {
				break
			}
		}
		assert.Equal(t, test.count, count, "Got more events that expected in test %d \"%s\" (%d vs %d)", testIdx, test.name, test.count, count)
	}
}
