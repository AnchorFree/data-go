package gzip_hash_reader

import (
	"bytes"
	"crypto/md5"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gzipHashTest struct {
	name string
	raw  string
	gzip []byte
	err  error
}

// use `cat binary.data | xxd -i` to fill the gzip test data
var gzipHashTests = []gzipHashTest{
	{
		"basic",
		"This is a string that might help you to debug your app\nА це будe під огірочки...\n",
		[]byte{
			0x1f, 0x8b, 0x08, 0x08, 0xe6, 0xa6, 0x44, 0x5b, 0x00, 0x03, 0x63, 0x6f,
			0x70, 0x79, 0x2e, 0x6c, 0x6f, 0x67, 0x00, 0x0b, 0xc9, 0xc8, 0x2c, 0x56,
			0x00, 0xa2, 0x44, 0x85, 0xe2, 0x92, 0xa2, 0xcc, 0xbc, 0x74, 0x85, 0x92,
			0x8c, 0xc4, 0x12, 0x85, 0xdc, 0xcc, 0xf4, 0x8c, 0x12, 0x85, 0x8c, 0xd4,
			0x9c, 0x02, 0x85, 0xca, 0xfc, 0x52, 0x85, 0x92, 0x7c, 0x85, 0x94, 0xd4,
			0xa4, 0xd2, 0x74, 0x10, 0xa7, 0x48, 0x21, 0xb1, 0xa0, 0x80, 0xeb, 0xc2,
			0x04, 0x85, 0x8b, 0x6d, 0x17, 0xb6, 0x2a, 0x5c, 0xd8, 0x78, 0xb1, 0xf9,
			0xc2, 0x96, 0x54, 0x85, 0x0b, 0xfb, 0x2f, 0x4e, 0xbb, 0xb0, 0x45, 0xe1,
			0xc2, 0xbe, 0x0b, 0x9b, 0x2f, 0x4e, 0xbb, 0xd8, 0x70, 0x61, 0xdf, 0xc5,
			0xf6, 0x0b, 0xbb, 0x2e, 0xec, 0xd0, 0xd3, 0xd3, 0xe3, 0x02, 0x00, 0xc2,
			0xed, 0x66, 0x68, 0x62, 0x00, 0x00, 0x00,
		},
		nil,
	},
	{
		"empty",
		"",
		[]byte{
			0x1f, 0x8b, 0x08, 0x08, 0xb9, 0x8d, 0x44, 0x5b, 0x00, 0x03, 0x65, 0x6d,
			0x70, 0x74, 0x79, 0x2e, 0x6c, 0x6f, 0x67, 0x00, 0x03, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		},
		nil,
	},
	{
		"unexpected EOF",
		"This is a string that might help you to debug your app\nА це будe під огірочки...\n",
		[]byte{
			0x1f, 0x8b, 0x08, 0x08, 0xe6, 0xa6, 0x44, 0x5b, 0x00, 0x03, 0x63, 0x6f,
			0x70, 0x79, 0x2e, 0x6c, 0x6f, 0x67, 0x00, 0x0b, 0xc9, 0xc8, 0x2c, 0x56,
			0x00, 0xa2, 0x44, 0x85, 0xe2, 0x92, 0xa2, 0xcc, 0xbc, 0x74, 0x85, 0x92,
		},
		io.ErrUnexpectedEOF,
	},
}

func TestGzipHashReader(t *testing.T) {
	for test_index, test := range gzipHashTests {
		inp := bytes.NewReader(test.gzip)
		ghr, createErr := NewGzipHashReader(inp)
		require.NoErrorf(t, createErr, "%s: NewGzipHashReader: %s", test.name, createErr)
		defer ghr.Close()
		var (
			buff bytes.Buffer
			err  error = nil
		)
		// nolint: ineffassign
		for {
			var n int = 0
			var chunkLen = 10
			err = nil
			chunk := make([]byte, chunkLen)
			n, err = ghr.Read(chunk)
			if n > 0 {
				buff.Write(chunk[:n])
			}
			if err == io.EOF || err != nil {
				break
			}
		}
		if err == io.EOF {
			assert.Equalf(t, test.raw, buff.String(), "Raw read string don't match in test #%d \"%s\" (len %d vs %d) '%s'", test_index, test.name, len(test.raw), len(buff.String()), buff.String())
			assert.Equalf(t, md5.Sum(test.gzip), ghr.Sum(), "MD5 checksums don't match in test #%d \"%s\" (%x vs %x)", test_index, test.name, md5.Sum(test.gzip), ghr.Sum())
		} else {
			assert.Equalf(t, test.err, err, "test#d \"%s\"", test_index, test.name)
		}
	}
}
