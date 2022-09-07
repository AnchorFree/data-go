package error_at_offset_reader

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorAtOffsetReader(t *testing.T) {
	var errorAtOffset int = 10
	testData := []byte("This is a test string that is supposed to help")
	br := bytes.NewReader(testData)
	r := NewErrorAtOffsetReader(br, errorAtOffset)
	data, err := io.ReadAll(r)
	assert.Equal(t, errorAtOffset, len(data), "Error offset is not what expected")
	assert.Equal(t, io.ErrUnexpectedEOF, err, "Returned error is not what expected")
}
