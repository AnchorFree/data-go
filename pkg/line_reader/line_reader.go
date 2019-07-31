// Deprecated: use event_reader instead
package line_reader

// Common interface to communicate
// between event line handlers
//
// Deprecated: use event_reader.Reader instead
type I interface {
	// return message, offset, error
	ReadLine() ([]byte, uint64, error)
}
