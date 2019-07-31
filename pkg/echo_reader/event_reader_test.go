package echo_reader

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"testing"
)

func TestEventReader_ReadEvent(t *testing.T) {
	topic := "test"
	raw := []byte("This is the test\nSecond line here\n")
	lor := line_offset_reader.NewEventReader(bytes.NewReader(raw), topic)
	pipeReader, pipeWriter := io.Pipe()
	echoReader := NewEventReader(lor, pipeWriter)
	go func() {
		for {
			eventEntry := echoReader.ReadEvent()
			if eventEntry.Error != nil {
				_ = pipeWriter.Close()
				return
			}
		}
	}()
	rawLor := line_offset_reader.NewEventReader(bytes.NewReader(raw), topic)
	buf := bytes.NewBuffer([]byte(""))
	for {
		//line, _, err := rawLor.ReadLine()
		eventEntry := rawLor.ReadEvent()
		buf.Write([]byte(echoReader.lineReader.Prefix))
		buf.Write(eventEntry.Message)
		buf.Write([]byte(echoReader.lineReader.Suffix))
		if eventEntry.Error != nil {
			break
		}
	}
	echoed, _ := ioutil.ReadAll(pipeReader)
	expected, _ := ioutil.ReadAll(buf)
	assert.Equal(t, expected, echoed)
}

func TestEventReader_SetPrefix(t *testing.T) {
	topic := "test"
	raw := []byte("This is the test\nSecond line here\n")
	prefix := "PrefixString"
	lor := line_offset_reader.NewEventReader(bytes.NewReader(raw), topic)
	_, pipeWriter := io.Pipe()
	defer pipeWriter.Close()
	echoReader := NewEventReader(lor, pipeWriter)
	echoReader.SetPrefix(prefix)
	assert.Equal(t, prefix, echoReader.lineReader.Prefix)
}

func TestEventReader_SetSuffix(t *testing.T) {
	topic := "test"
	raw := []byte("This is the test\nSecond line here\n")
	suffix := "SuffixString"
	lor := line_offset_reader.NewEventReader(bytes.NewReader(raw), topic)
	_, pipeWriter := io.Pipe()
	defer pipeWriter.Close()
	echoReader := NewEventReader(lor, pipeWriter)
	echoReader.SetSuffix(suffix)
	assert.Equal(t, suffix, echoReader.lineReader.Suffix)
}
