package error_at_offset_reader

import (
	"io"
)

type ErrorAtOffsetReader struct {
	offset      int
	errorOffset int
	srcReader   io.Reader
}

func NewErrorAtOffsetReader(inp io.Reader, errorOffset int) (r *ErrorAtOffsetReader) {
	return &ErrorAtOffsetReader{
		srcReader:   inp,
		errorOffset: errorOffset,
		offset:      0,
	}
}

func (r *ErrorAtOffsetReader) Read(p []byte) (n int, err error) {
	if r.offset+len(p) > r.errorOffset {
		p = p[:r.errorOffset-r.offset]
	}
	n, err = r.srcReader.Read(p)
	r.offset += n
	if r.offset == r.errorOffset {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
