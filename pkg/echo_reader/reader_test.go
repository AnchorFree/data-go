package echo_reader

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"testing"
)

func TestEchoReader(t *testing.T) {
	raw := []byte("This is the test\nSecond line here\n")
	lor := line_offset_reader.NewReader(bytes.NewReader(raw))
	pipeReader, pipeWriter := io.Pipe()
	echoReader := NewEchoReader(lor, pipeWriter)
	go func() {
		for {
			_, _, err := echoReader.ReadLine()
			if err != nil {
				pipeWriter.Close()
				return
			}
		}
	}()
	rawLor := line_offset_reader.NewReader(bytes.NewReader(raw))
	buf := bytes.NewBuffer([]byte(""))
	for {
		line, _, err := rawLor.ReadLine()
		buf.Write([]byte(echoReader.Prefix))
		buf.Write(line)
		buf.Write([]byte(echoReader.Suffix))
		if err != nil {
			break
		}
	}
	echoed, _ := ioutil.ReadAll(pipeReader)
	expected, _ := ioutil.ReadAll(buf)
	assert.Equal(t, expected, echoed)
}
