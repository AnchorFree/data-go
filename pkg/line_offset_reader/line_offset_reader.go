package line_offset_reader

import (
	"bufio"
	"bytes"
	"github.com/anchorfree/data-go/pkg/logger"
	"io"
)

type LineOffsetReader struct {
	io.Reader
	nextOffset            uint64
	bufReader             *bufio.Reader
	bytesRead             int64
	linesRead             int64
	LookForJsonDelimiters bool
	leftoverBytes         []byte
	TrimMessages          bool
}

func NewLineOffsetReader(inp io.Reader) (r *LineOffsetReader) {
	return &LineOffsetReader{
		nextOffset:            0,
		bufReader:             bufio.NewReader(inp),
		bytesRead:             0,
		linesRead:             0,
		LookForJsonDelimiters: false,
		leftoverBytes:         []byte{},
		TrimMessages:          false,
	}
}

func (r *LineOffsetReader) BytesRead() int64 {
	return r.bytesRead
}
func (r *LineOffsetReader) LinesRead() int64 {
	return r.linesRead
}
func IsWhiteSpace(b byte) bool {
	for _, w := range []byte("\n\t \u000A") {
		if b == w {
			return true
		}
	}
	return false
}
func (r *LineOffsetReader) readJsonMessage() []byte {
	var open, closed int
	opened := false
	for i, b := range r.leftoverBytes {
		switch b {
		case byte('{'):
			opened = true
			open++
		case byte('}'):
			if opened {
				closed++
			}
		}
		// if amount of open equals to closed, we have complete JSON message

		if open != 0 && open == closed && (i+1 == len(r.leftoverBytes) || !IsWhiteSpace(r.leftoverBytes[i+1])) {
			var message []byte
			leftover := r.leftoverBytes[i+1:]
			//check whether there is non-whitespace characters left, so that it makes to leave leftovers for extra line
			if len(bytes.TrimSpace(leftover)) > 0 {
				message := r.leftoverBytes[0 : i+1]
				r.leftoverBytes = leftover
				return message
			} else {
				message := r.leftoverBytes
				r.leftoverBytes = []byte{}
				return message
			}
			return message
		}
	}
	message := r.leftoverBytes
	r.leftoverBytes = []byte{}
	return message
}
func (r *LineOffsetReader) readFullLine() []byte {
	ret := r.leftoverBytes
	r.leftoverBytes = []byte{}
	return ret
}

func (r *LineOffsetReader) ReadLine() ([]byte, uint64, error) {
	var (
		err error
		buf []byte
	)
	offset := r.nextOffset
	if len(r.leftoverBytes) == 0 {
		r.leftoverBytes, err = r.bufReader.ReadBytes('\n')
	}
	if r.LookForJsonDelimiters {
		buf = r.readJsonMessage()
	} else {
		buf = r.readFullLine()
	}
	r.bytesRead += int64(len(buf))
	r.nextOffset += uint64(len(buf))
	if err != nil && err != io.EOF {
		logger.Get().Debugf("LineOffsetReader error: %s", err)
	}
	if len(buf) > 0 && buf[len(buf)-1] == '\n' {
		buf = buf[:len(buf)-1]
	}
	if len(buf) > 0 && buf[len(buf)-1] == '\r' {
		buf = buf[:len(buf)-1]
	}
	if len(buf) > 0 {
		r.linesRead++
	}
	peekBytes, peekErr := r.bufReader.Peek(1)
	if len(peekBytes) == 0 && peekErr == io.EOF {
		err = io.EOF
	}
	if len(r.leftoverBytes) > 0 {
		var emptyErr error
		err = emptyErr
	}
	if r.TrimMessages {
		buf = bytes.TrimSpace(buf)
	}

	return buf, offset, err
}
