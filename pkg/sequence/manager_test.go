package sequence

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSequenceManager(t *testing.T) {
	sm := NewSequenceManager()
	assert.Implements(t, (*sync.Locker)(nil), sm, "Failed sync.Locker interface check")
	assert.Equal(t, sm.sequences, map[string]*uint64{}, "Failed data storage type and value test ")
}

func TestSequenceManager_GetForName(t *testing.T) {
	sm := NewSequenceManager()
	inc := sm.GetForName("test")

	assert.Equal(t, inc(), uint64(1), "Failed to test first increment")
	assert.Equal(t, inc(), uint64(2), "Failed to test second increment")
	assert.Equal(t, inc(), uint64(3), "Failed to test third increment")

	assert.Equal(t, *sm.sequences["test"], uint64(3), "Failed to test stored value")
}
