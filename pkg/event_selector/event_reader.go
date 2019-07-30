package event_selector

import (
	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/logger"
)

type EventReader struct {
	eventReader    event.Reader
	eventSelector  *EventSelector
	selectedEvents []*event.Event
	returnSelected bool
	upstreamErr error
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
	if er.returnSelected {
		eventEntry = er.getSelectedEvent()
		logger.Get().Debugf("Got event from selected: %#v", eventEntry)
	} else {
		logger.Get().Debugf("Getting event from upstream: %#v", eventEntry)
		eventEntry = er.getUpstreamEvent()
		if eventEntry.Error != nil {
			er.upstreamErr = eventEntry.Error
			er.returnSelected = true
			eventEntry.Error = nil
			logger.Get().Debugf("Upstream event with error, return from selected: %#v", eventEntry)
		}
	}
	return eventEntry
}

func (er *EventReader) getSelectedEvent() *event.Event {
	var eventEntry *event.Event
	if len(er.selectedEvents) > 0 {
		eventEntry, er.selectedEvents = er.selectedEvents[0], er.selectedEvents[1:]
		logger.Get().Debugf("Pop event from selected: %#v, left: %d", eventEntry, len(er.selectedEvents))
	} else {
		eventEntry = &event.Event{Error: er.upstreamErr}
		logger.Get().Debugf("Return empty event with error: %#v", eventEntry)
	}
	return eventEntry
}

func (er *EventReader) getUpstreamEvent() *event.Event {
	eventEntry := er.eventReader.ReadEvent()

	parser := er.eventSelector.pPool.Get()
	message, err := parser.Parse(eventEntry.MessageString())
	er.eventSelector.pPool.Put(parser)
	if err != nil {
		logger.Get().Errorf("json parsing error: %#v", err)
		return eventEntry
	}

	for _, es := range er.eventSelector.config.EventSelectors {
		logger.Get().Debugf("Event selector: %#v", es)
		if eventEntry.Topic == es.TargetTopic {
			continue
		}
		if checkEventSelection(message, &es) {
			selectedEvent := &event.Event{}
			*selectedEvent = *eventEntry
			arena := er.eventSelector.aPool.Get()
			message.Set("__orig_topic__", arena.NewString(eventEntry.Topic))
			selectedEvent.Topic = es.TargetTopic
			selectedEvent.Message = []byte(message.String())
			er.eventSelector.aPool.Put(arena)
			er.selectedEvents = append(er.selectedEvents, selectedEvent)
			logger.Get().Debugf("Selected event: %s and send to the topic: %s", selectedEvent.MessageString(), selectedEvent.Topic)
		}
	}

	logger.Get().Debugf("%#v", er.selectedEvents)

	return eventEntry
}