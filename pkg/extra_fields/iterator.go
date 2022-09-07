package extra_fields

import (
	"encoding/json"
	"net/http"

	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type EventIterator struct {
	iterator       types.EventIterator
	event          *types.Event
	err            error
	request        *http.Request
	extraFields    []byte
	extraFieldFunc map[string]func() interface{}
}

var _ types.EventIterator = (*EventIterator)(nil)

func NewIterator(eventIterator types.EventIterator, req *http.Request) *EventIterator {
	return &EventIterator{
		iterator:       eventIterator,
		request:        req,
		extraFields:    []byte(""),
		extraFieldFunc: make(map[string]func() interface{}),
	}
}

func (ei *EventIterator) Next() bool {
	if !ei.iterator.Next() {
		ei.err = ei.iterator.Err()
		return false
	}

	ei.event = ei.iterator.At()

	fields := new(ExtraFields)
	fields.GeoOrigin(ei.request)
	fields.CloudFront = IsCloudfront(ei.request)
	fields.Host = GetNginxHostname(ei.request)

	extra, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		return false
	}

	ei.event.Message = AppendJsonExtraFields(ei.event.Message, extra)
	if len(ei.extraFields) > 0 {
		ei.event.Message = AppendJsonExtraFields(ei.event.Message, ei.extraFields)
	}
	if len(ei.extraFieldFunc) > 0 {
		ei.event.Message = AppendJsonExtraFields(ei.event.Message, ei.renderExtraFieldsFunc())
	}

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.event
}

func (ei *EventIterator) Err() error {
	return ei.err
}

func (ei *EventIterator) With(extra map[string]interface{}) *EventIterator {
	extraJson, err := json.Marshal(extra)
	if err != nil {
		logger.Get().Errorf("Could not marshal extra fields: %v", extra)
	} else {
		ei.extraFields = AppendJsonExtraFields(ei.extraFields, extraJson)
	}
	return ei
}

func (ei *EventIterator) WithFuncUint64(key string, f func() uint64) *EventIterator {
	ei.WithFunc(key, func() interface{} {
		return interface{}(f())
	})
	return ei
}

func (ei *EventIterator) WithFunc(key string, f func() interface{}) *EventIterator {
	ei.extraFieldFunc[key] = f
	return ei
}

func (ei *EventIterator) renderExtraFieldsFunc() []byte {
	extraFields := make(map[string]interface{})
	for key, f := range ei.extraFieldFunc {
		extraFields[key] = f()
	}
	renderedExtraFields, err := json.Marshal(extraFields)
	if err != nil {
		logger.Get().Errorf("Could not marshal function extra fields: %v", extraFields)
	}
	return renderedExtraFields
}
