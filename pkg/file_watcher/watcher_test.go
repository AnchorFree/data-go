package file_watcher

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGeo(t *testing.T) {
	tmpFile := TempFileName("file-watcher-test", "tmp")
	testData := []byte("this is test data")
	err := ioutil.WriteFile(tmpFile, testData, 0777)
	assert.NoError(t, err)
	c := make(chan struct{})
	DefaultTimeoutAfterLastEvent = 1 * time.Second
	_, err = New(tmpFile, func(fn string) {
		c <- struct{}{}
	})
	assert.NoError(t, err)
	err = ioutil.WriteFile(tmpFile, []byte("some dummy data"), 0777)
	assert.NoError(t, err)
	select {
	case <-c:
		//success
	case <-time.After(5 * time.Second):
		assert.FailNow(t, "Did not detect file change")
	}
}

func TempFileName(prefix, suffix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return filepath.Join(os.TempDir(), prefix+hex.EncodeToString(randBytes)+suffix)
}
