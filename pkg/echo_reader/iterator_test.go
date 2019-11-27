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
	lor := line_offset_reader.NewIterator(bytes.NewReader(raw), topic)
	pipeReader, pipeWriter := io.Pipe()
	echoReader := NewIterator(lor, pipeWriter)
	go func() {
		cnt := 0
		for echoReader.Next() {
			_ = echoReader.At()
			cnt++
		}
		_ = pipeWriter.Close()
	}()
	rawLor := line_offset_reader.NewIterator(bytes.NewReader(raw), topic)
	buf := bytes.NewBuffer([]byte(""))
	for rawLor.Next() {
		event := rawLor.At()
		buf.Write([]byte(echoReader.prefix))
		buf.Write(event.Message)
		buf.Write([]byte(echoReader.suffix))
	}
	echoed, _ := ioutil.ReadAll(pipeReader)
	expected, _ := ioutil.ReadAll(buf)
	assert.Equal(t, expected, echoed)
}

func TestEventReader_SetPrefix(t *testing.T) {
	topic := "test"
	raw := []byte("This is the test\nSecond line here\n")
	prefix := "PrefixString"
	lor := line_offset_reader.NewIterator(bytes.NewReader(raw), topic)
	_, pipeWriter := io.Pipe()
	defer pipeWriter.Close()
	echoReader := NewIterator(lor, pipeWriter)
	echoReader.SetPrefix(prefix)
	assert.Equal(t, prefix, echoReader.prefix)
}

func TestEventReader_SetSuffix(t *testing.T) {
	topic := "test"
	raw := []byte("This is the test\nSecond line here\n")
	suffix := "SuffixString"
	lor := line_offset_reader.NewIterator(bytes.NewReader(raw), topic)
	_, pipeWriter := io.Pipe()
	defer pipeWriter.Close()
	echoReader := NewIterator(lor, pipeWriter)
	echoReader.SetSuffix(suffix)
	assert.Equal(t, suffix, echoReader.suffix)
}
