package metricbuilder

import (
	"github.com/anchorfree/data-go/pkg/line_reader"
)

// Deprecated: use EventReader instead
type Reader struct {
	reader line_reader.I
	topic  string
}

var _ line_reader.I = (*Reader)(nil)

// Deprecated: use NewEventReader instead
func NewReader(lr line_reader.I, topic string) *Reader {
	return &Reader{
		reader: lr,
		topic:  topic,
	}
}

// Deprecated: use EventReader.ReadLine() instead
func (r *Reader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	const maxReplacements = 1
	updateMetric(
		appendTopicToMessage(line, r.topic),
		r.topic,
	)
	return line, offset, err
}
