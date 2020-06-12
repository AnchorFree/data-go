package gzip_hash_reader

import (
	"compress/gzip"
	"crypto/md5" // #nosec
	"github.com/anchorfree/data-go/pkg/logger"
	"hash"
	"io"
	"sync"
)

type GzipHashReader struct {
	bytesRead  int64
	checksum   hash.Hash
	teeReader  io.Reader
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	gzipReader *gzip.Reader
	waitGroup  sync.WaitGroup
}

func NewGzipHashReader(inp io.Reader) (r *GzipHashReader, err error) {
	r = new(GzipHashReader)
	r.bytesRead = 0
	r.checksum = md5.New() // #nosec
	r.pipeReader, r.pipeWriter = io.Pipe()
	r.teeReader = io.TeeReader(inp, r.pipeWriter)
	r.waitGroup.Add(1)
	go func() {
		defer r.waitGroup.Done()
		r.bytesRead, _ = io.Copy(r.checksum, r.pipeReader)
	}()
	r.gzipReader, err = gzip.NewReader(r.teeReader)
	return r, err
}

func (r *GzipHashReader) Read(p []byte) (n int, err error) {
	n, err = r.gzipReader.Read(p)
	if err != nil {
		if err != io.EOF {
			logger.Get().Debugf("GzipHashReader error: %s", err)
		}
		r.Close()
		r.waitGroup.Wait()
	}
	return n, err
}

func (r *GzipHashReader) BytesRead() int64 {
	return r.bytesRead
}

func (r *GzipHashReader) Close() {
	_ = r.pipeWriter.Close()
	_ = r.pipeReader.Close()
	_ = r.gzipReader.Close()
}

func (r *GzipHashReader) Sum() [md5.Size]byte {
	var hash [md5.Size]byte
	ret := r.checksum.Sum(nil)
	copy(hash[:], ret)
	return hash
}
