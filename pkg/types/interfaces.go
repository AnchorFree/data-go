package types

// EventReader interface
type EventReader interface {
	// return Event pointer
	ReadEvent() *Event
}

// EventIterator interface
type EventIterator interface {
	Next() bool
	At() *Event
	Err() error
}
