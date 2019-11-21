package types

import "encoding/json"

// Control type of message could be either Json or Raw
type eventType int

const (
	// Non Json Message
	EventRaw eventType = iota

	// Valid Json Message
	EventJson
)

// Event type, contains all required fields
type Event struct {
	Topic   string
	Message []byte
	Json    map[string]interface{}
	Offset  uint64
	Type    eventType
}

func NewEvent(topic string, message []byte) *Event {
	event := &Event{Topic: topic}
	if err := json.Unmarshal(message, event.Json) ; err != nil {
		return &Event{Topic: topic, Message: message, Type: EventRaw}
	}
	return &Event{Topic: topic, Type: EventJson}
}

func (e *Event) MessageString() string {
	return string(e.Message)
}
