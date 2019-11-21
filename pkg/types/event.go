package types

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
	// Non Json Message
	EventRaw eventType = iota

	// Valid Json Message
	TypeJson
)

func (e *Event) MessageString() string {
	return string(e.Message)
}
