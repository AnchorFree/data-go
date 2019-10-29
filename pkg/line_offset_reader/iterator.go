package line_offset_reader

import (
	"bufio"
	"bytes"
	"io"

	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	topic                 string
	event                 *types.Event
	err                   error
	next                  bool
	bufReader             *bufio.Reader
	nextOffset            uint64
	bytesRead             int64
	linesRead             int64
	leftoverBytes         []byte
	lookForJsonDelimiters bool
	trimMessages          bool
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(inp io.Reader, topic string) *EventIterator {
	return &EventIterator{
		topic: topic,
		next:  true,

		nextOffset:            0,
		bufReader:             bufio.NewReader(inp),
		bytesRead:             0,
		linesRead:             0,
		lookForJsonDelimiters: false,
		leftoverBytes:         []byte{},
		trimMessages:          false,
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.next {
		return false
	}

	var (
		err error
		buf []byte
	)
	offset := ei.nextOffset
	if len(ei.leftoverBytes) == 0 {
		ei.leftoverBytes, err = ei.bufReader.ReadBytes('\n')
	}
	if ei.lookForJsonDelimiters {
		buf = ei.readJsonMessage()
	} else {
		buf = ei.readFullLine()
	}
	ei.bytesRead += int64(len(buf))
	ei.nextOffset += uint64(len(buf))
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
		ei.linesRead++
	}
	peekBytes, peekErr := ei.bufReader.Peek(1)
	if len(peekBytes) == 0 && peekErr == io.EOF {
		err = io.EOF
	}
	if len(ei.leftoverBytes) > 0 {
		var emptyErr error
		err = emptyErr
	}
	if ei.trimMessages {
		buf = bytes.TrimSpace(buf)
	}

	ei.event = &types.Event{
		Topic:   ei.topic,
		Message: buf,
		Offset:  offset,
		Type:    types.TypeUnknown,
	}

	if err != nil {
		ei.next = false
	}

	if err != io.EOF {
		ei.err = err
	}

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}

func (ei *EventIterator) readJsonMessage() []byte {
	var open, closed int
	opened := false
	for i, b := range ei.leftoverBytes {
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
		if open != 0 && open == closed && (i+1 == len(ei.leftoverBytes) || !IsWhiteSpace(ei.leftoverBytes[i+1])) {
			leftover := ei.leftoverBytes[i+1:]
			//check whether there is non-whitespace characters left, so that it makes to leave leftovers for extra line
			if len(bytes.TrimSpace(leftover)) > 0 {
				message := ei.leftoverBytes[0 : i+1]
				ei.leftoverBytes = leftover
				return message
			} else {
				message := ei.leftoverBytes
				ei.leftoverBytes = []byte{}
				return message
			}
		}
	}
	message := ei.leftoverBytes
	ei.leftoverBytes = []byte{}
	return message
}

func (ei *EventIterator) readFullLine() []byte {
	ret := ei.leftoverBytes
	ei.leftoverBytes = []byte{}
	return ret
}

func (er *EventIterator) LookForJsonDelimiters(flag bool) *EventIterator {
	er.lookForJsonDelimiters = flag
	return er
}

func (er *EventIterator) TrimMessages(flag bool) *EventIterator {
	er.trimMessages = flag
	return er
}

func (er *EventIterator) BytesRead() int64 {
	return er.bytesRead
}

func (er *EventIterator) LinesRead() int64 {
	return er.linesRead
}
