package event

// Event type, contains all required fields
type Event struct {
	Topic   string
	Message []byte
	Offset  uint64
	Error   error
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

// Event Reader interface
type Reader interface {
	// return Event pointer
	ReadEvent() *Event
}

func (e *Event) MessageString() string {
	return string(e.Message)
}
