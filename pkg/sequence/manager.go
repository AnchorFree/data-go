package sequence

import (
	"sync"
	"sync/atomic"
)

type sequenceManager struct {
	sync.Mutex
	sequences map[string]*uint64
}

func NewSequenceManager() *sequenceManager {
	return &sequenceManager{
		sequences: make(map[string]*uint64),
	}
}

func (sm *sequenceManager) GetForName(name string) func() uint64 {
	valptr := sm.getValuePrt(name)
	return func() uint64 {
		return atomic.AddUint64(valptr, 1)
	}
}

func (sm *sequenceManager) getValuePrt(name string) (valptr *uint64) {
	sm.Lock()
	defer sm.Unlock()
	valptr, ok := sm.sequences[name]
	if !ok {
		valptr = new(uint64)
		sm.sequences[name] = valptr
	}
	return
}
