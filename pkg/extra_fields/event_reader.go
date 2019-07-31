package extra_fields

import (
	"encoding/json"
	"net/http"

	"github.com/anchorfree/data-go/pkg/event"
)

type EventReader struct {
	lineReader  *ExtraFieldsReader
	eventReader event.Reader
}

var _ event.Reader = (*EventReader)(nil)

func NewEventReader(eventReader event.Reader, req *http.Request) *EventReader {
	return &EventReader{
		eventReader: eventReader,
		lineReader:  NewExtraFieldsReader(nil, req),
	}
}

func (er *EventReader) With(extra map[string]interface{}) *EventReader {
	er.lineReader.With(extra)
	return er
}

func (er *EventReader) WithFuncUint64(key string, f func() uint64) *EventReader {
	er.lineReader.WithFunc(key, func() interface{} {
		return interface{}(f())
	})
	return er
}

func (er *EventReader) WithFunc(key string, f func() interface{}) *EventReader {
	er.lineReader.extraFieldFunc[key] = f
	return er
}

func (er *EventReader) ReadEvent() *event.Event {
	eventEntry := er.eventReader.ReadEvent()

	fields := new(ExtraFields)
	fields.GeoOrigin(er.lineReader.request)
	fields.CloudFront = IsCloudfront(er.lineReader.request)
	fields.Host = GetNginxHostname(er.lineReader.request)

	extra, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		return eventEntry
	}

	eventEntry.Message = AppendJsonExtraFields(eventEntry.Message, extra)
	if len(er.lineReader.extraFields) > 0 {
		eventEntry.Message = AppendJsonExtraFields(eventEntry.Message, er.lineReader.extraFields)
	}
	if len(er.lineReader.extraFieldFunc) > 0 {
		eventEntry.Message = AppendJsonExtraFields(eventEntry.Message, er.lineReader.renderExtraFieldsFunc())
	}

	return eventEntry
}
