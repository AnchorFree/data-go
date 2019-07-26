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
}

func (es *EventSelector) NewEventReader(reader event.Reader) *EventReader {
	return &EventReader{
		eventReader:    reader,
		eventSelector:  *es,
		selectedEvents: new([]*event.Event),
		returnSelected: false,
	}
}

func (esr *EventReader) ReadEvent() *event.Event {
	eventEntry := esr.eventReader.ReadEvent()

	parser := esr.eventSelector.pPool.Get()
	defer esr.eventSelector.pPool.Put(parser)
	arena := esr.eventSelector.aPool.Get()
	defer esr.eventSelector.aPool.Put(arena)

	message, err := parser.Parse(eventEntry.MessageString())
	if err != nil {
		logger.Get().Errorf("json parsing error: %#v", err)
		return eventEntry
	}

	for _, es := range esr.eventSelector.config.EventSelectors {
		logger.Get().Infof("Event selector: %#v", es)
		if checEvent(message, &es) {
			message.Set("__orig_topic__", arena.NewString(esr.topic))
			logger.Get().Infof("Selected event: %s and send to the topic: %s", message.String(), es.TargetTopic)
			esr.selectedEvents[es.TargetTopic] = append(esr.selectedEvents[es.TargetTopic], message.String())
		}
	}

	logger.Get().Infof("%#v", esr.selectedEvents)

	return eventEntry
}
