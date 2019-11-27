package schema

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	sm *SchemaManager

	iter  types.EventIterator
	event *types.Event
	err   error
}

func (sm *SchemaManager) NewIterator(iterator types.EventIterator) *EventIterator {
	return &EventIterator{
		sm:   sm,
		iter: iterator,
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.iter.Next() {
		logger.Get().Infof("no upstream events")
		ei.err = ei.iter.Err()
		return false
	}

	ei.event = ei.iter.At()
	logger.Get().Infof("go event from upstream: %s", ei.event)

	if ei.sm.schema == nil {
		logger.Get().Infof("empty swagger schema, skip validation")
		return true
	}

	if _, ok := ei.sm.validateTopics[ei.event.Topic]; !ok {
		logger.Get().Infof("topic %s is not selected for validation", ei.event.Topic)
		return true
	}

	if ok, err := ei.sm.Validate(*ei.event); !ok {
		logger.Get().Infof("failed to validate event: %s with error: %#v", ei.event, err)
		ei.event.Message = bytes.Join([][]byte{[]byte(ei.event.Topic), ei.event.Message}, []byte("\t"))
		ei.event.Topic = ei.sm.GetInvalidMessagesTopic()
	}

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}
