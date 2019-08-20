package event_selector

import (
	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/valyala/fastjson"
)

type EventReader struct {
	eventReader    event.Reader
	eventSelector  *EventSelector
	selectedEvents []*event.Event
	returnSelected bool
	upstreamErr    error
}

var _ event.Reader = (*EventReader)(nil)

func (es *EventSelector) NewEventReader(reader event.Reader) *EventReader {
	return &EventReader{
		eventReader:    reader,
		eventSelector:  es,
		selectedEvents: []*event.Event{},
		returnSelected: false,
	}
}

func (er *EventReader) ReadEvent() *event.Event {
	var eventEntry *event.Event
	switch er.returnSelected {
	case true:
		eventEntry = er.getSelectedEvent()
		logger.Get().Debugf("Got event from selected: %s", eventEntry)
	case false:
		eventEntry = er.getUpstreamEvent()
		logger.Get().Debugf("Got event from upstream: %s", eventEntry)
		if eventEntry.Error != nil && len(er.selectedEvents) > 0 {
			er.upstreamErr = eventEntry.Error
			er.returnSelected = true
			eventEntry.Error = nil
			logger.Get().Debugf("Got error from upstream, return from selected: %s", eventEntry)
		}
	}
	return eventEntry
}

func (er *EventReader) getSelectedEvent() *event.Event {
	var eventEntry *event.Event
	switch selectedLength := len(er.selectedEvents); {
	case selectedLength > 1:
		eventEntry, er.selectedEvents = er.selectedEvents[0], er.selectedEvents[1:]
		logger.Get().Debugf("Pop event from selected: %s, left: %d", eventEntry, len(er.selectedEvents))
	case selectedLength == 1:
		eventEntry, er.selectedEvents = er.selectedEvents[0], er.selectedEvents[1:]
		eventEntry.Error = er.upstreamErr
		logger.Get().Debugf("Pop event from selected and fill error with upstream error: %s, left: %d", eventEntry, len(er.selectedEvents))
	default:
		eventEntry = &event.Event{Error: er.upstreamErr}
		logger.Get().Debugf("Return empty event with error: %#v", eventEntry)
	}
	return eventEntry
}

func (er *EventReader) getUpstreamEvent() *event.Event {
	eventEntry := er.eventReader.ReadEvent()

	message, err := fastjson.Parse(eventEntry.MessageString())
	if err != nil {
		logger.Get().Debugf("json parsing error: %#v", err)
		return eventEntry
	}

	for _, es := range er.eventSelector.selectors.Selectors {
		logger.Get().Debugf("Event selector: %#v", es)
		if eventEntry.Topic == es.TargetTopic {
			continue
		}
		if checkEventSelection(message, &es) {
			selectedEvent := &event.Event{}
			*selectedEvent = *eventEntry
			origTopic, err := fastjson.Parse("\"" + eventEntry.Topic + "\"")
			if err != nil {
				logger.Get().Errorf("Topic parsing error: %#v", err)
				continue
			}
			message.Set("__orig_topic__", origTopic)
			selectedEvent.Topic = es.TargetTopic
			selectedEvent.Message = []byte(message.String())
			er.selectedEvents = append(er.selectedEvents, selectedEvent)
			logger.Get().Debugf("Selected event: %s and send to the topic: %s", selectedEvent.MessageString(), selectedEvent.Topic)
		}
	}

	logger.Get().Debugf("%#v", er.selectedEvents)

	return eventEntry
}
