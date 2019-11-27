package types

import "fmt"

// Event type, contains all required fields
type Event struct {
	Topic   string
	Message []byte
	Offset  uint64
	Type    eventType
}

// Control type of message could be either Json or Raw
// NOTE: Placeholder for future features
// TODO: Add implementations using that field
type eventType int

const (
	// Unknown Message Type
	TypeUnknown eventType = iota

	// Valid Json Message
	TypeJson

	// Non Json Message
	TypeRaw
)

func (e *Event) MessageString() string {
	return string(e.Message)
}

func (e *Event) String() string {
	return fmt.Sprintf("{Topic: `%s`, Message: `%s` }", e.Topic, string(e.Message))
}
