package client

import (
	"bytes"
	"encoding/json"

	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/prometheus/client_golang/prometheus"
)

type I interface {
	SendEvents(event.Reader) (uint64, uint64, uint64, error)
	// Deprecated: use SendEvents instead
	SendMessages(string, line_reader.I) (uint64, uint64, uint64, error)
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

func (c *T) FilterTopicEvent(eventEntry *event.Event) (*event.Event, bool) {
	doValidate, present := c.ValidateJsonTopics[eventEntry.Topic]
	if present && doValidate {
		if !json.Valid(eventEntry.Message) {
			eventEntry.Topic = c.GetInvalidMessagesTopic()
			eventEntry.Message = bytes.Join([][]byte{[]byte(eventEntry.Topic), eventEntry.Message}, []byte("\t"))
			return eventEntry, true
		}
	}
	return eventEntry, false
}

func (c *T) GetValidateJsonTopics() map[string]bool {
	return c.ValidateJsonTopics
}
