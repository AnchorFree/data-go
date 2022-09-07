package file_watcher

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGeo(t *testing.T) {
	// Create temp dir and a file inside it
	// Mac OS will try to watch file`s parent directory
	tmpDir, err := os.MkdirTemp("", "test-data-go-*")
	defer os.Remove(tmpDir)
	assert.NoError(t, err)

	tmpFile, err := os.CreateTemp(tmpDir, "test-fole-watch-*")
	defer os.Remove(tmpFile.Name())
	assert.NoError(t, err)

	_, err = tmpFile.Write([]byte("this is test data"))
	assert.NoError(t, err)

	c := make(chan struct{})
	DefaultTimeoutAfterLastEvent = 1 * time.Second
	_, err = New(tmpFile.Name(), func(fn string) {
		c <- struct{}{}
	})
	assert.NoError(t, err)

	err = tmpFile.Truncate(0)
	assert.NoError(t, err)

	_, err = tmpFile.WriteAt([]byte("some dummy data"), 0)
	assert.NoError(t, err)

	select {
	case <-c:
		//success
	case <-time.After(5 * time.Second):
		assert.FailNow(t, "Did not detect file change")
	}
}
