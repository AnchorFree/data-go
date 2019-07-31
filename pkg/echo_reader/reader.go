package echo_reader

import (
	"fmt"
	"io"

	"github.com/anchorfree/data-go/pkg/line_reader"
)

type EchoReader struct {
	writer io.Writer
	line_reader.I
	reader line_reader.I
	Prefix string
	Suffix string
}

func NewEchoReader(lr line_reader.I, w io.Writer) *EchoReader {
	return &EchoReader{
		reader: lr,
		writer: w,
		Prefix: "sent message: ",
		Suffix: "\n",
	}
}

func (r *EchoReader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	fmt.Fprintf(r.writer, "%s%s%s", r.Prefix, line, r.Suffix)
	return line, offset, err
}
