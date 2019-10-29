package client

import (
	"bytes"
	"encoding/json"

	"github.com/anchorfree/data-go/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

type I interface {
	SendEvents(iterator types.EventIterator) (uint64, uint64, uint64, error)
	FilterTopicMessage(string, []byte) (string, []byte, bool)
	SetValidateJsonTopics(map[string]bool)
	GetValidateJsonTopics() map[string]bool
	ListTopics() ([]string, error)
}

type Props struct {
	InvalidMessagesTopic string `yaml:"invalid_messages_topic"`
}

type T struct {
	Prom               *prometheus.Registry
	Config             Props
	ValidateJsonTopics map[string]bool
}

func (c *T) GetInvalidMessagesTopic() string {
	if len(c.Config.InvalidMessagesTopic) > 0 {
		return c.Config.InvalidMessagesTopic
	}
	return "malformed"
}

func (c *T) SetValidateJsonTopics(topics map[string]bool) {
	c.ValidateJsonTopics = topics
}

func (c *T) FilterTopicMessage(topic string, message []byte) (string, []byte, bool) {
	doValidate, present := c.ValidateJsonTopics[topic]
	if present && doValidate {
		if !json.Valid(message) {
			return c.GetInvalidMessagesTopic(), bytes.Join([][]byte{[]byte(topic), message}, []byte("\t")), true
		}
	}
	return topic, message, false
}

func (c *T) FilterTopicEvent(event *types.Event) (*types.Event, bool) {
	doValidate, present := c.ValidateJsonTopics[event.Topic]
	if present && doValidate {
		if !json.Valid(event.Message) {
			event.Message = bytes.Join([][]byte{[]byte(event.Topic), event.Message}, []byte("\t"))
			event.Topic = c.GetInvalidMessagesTopic()
			return event, true
		}
	}
	return event, false
}

func (c *T) GetValidateJsonTopics() map[string]bool {
	return c.ValidateJsonTopics
}
