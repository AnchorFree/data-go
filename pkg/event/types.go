package event

// Event type, contains all required fields
type Event struct {
	Topic   string
	Message []byte
	Offset  uint64
	Error   error
	Type    eventType
}

// Control type of message
// could be either Json or Raw
type eventType uint

const (
	// Unknown Message Type
	TypeUnknown eventType = 0

	// Valid Json Message
	TypeJson eventType = 1

	// Non Json Message
	TypeRaw eventType = 2
)

type Reader interface {
	//return Event
	ReadEvent() *Event
}

func (e *Event) MessageString() string  {
	return string(e.Message)
}