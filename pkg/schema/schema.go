package schema

import (
	"github.com/qri-io/jsonschema"
	"gopkg.in/yaml.v2"

	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	iter  types.EventIterator
	rs *jsonschema.RootSchema
	event *types.Event
	err   error
}

func NewIterator(iterator types.EventIterator, schemaData string) (*EventIterator, error) {
	rs := &jsonschema.RootSchema{}
	if err := yaml.Unmarshal([]byte(schemaData), rs); err != nil {
		return nil, err
	}
	return &EventIterator{iter: iterator}, nil
}

func (ei *EventIterator) Next() bool {
	if !ei.iter.Next() {
		ei.err = ei.iter.Err()
		return false
	}

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}
