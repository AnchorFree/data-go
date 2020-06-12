package event_selector

import (
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
	"github.com/valyala/fastjson"
)

type EventIterator struct {
	iterator       types.EventIterator
	eventSelector  *EventSelector
	entry          *types.Event
	selectedEvents []*types.Event
	err            error
}

var _ types.EventIterator = (*EventIterator)(nil)

func (es *EventSelector) NewIterator(eventIterator types.EventIterator) *EventIterator {
	return &EventIterator{
		iterator:       eventIterator,
		eventSelector:  es,
		selectedEvents: []*types.Event{},
	}
}

func (ei *EventIterator) Next() bool {
	if len(ei.selectedEvents) > 0 {
		logger.Get().Debugf("Return from selectedEvents: %#v", ei.selectedEvents)
		ei.entry, ei.selectedEvents = ei.selectedEvents[0], ei.selectedEvents[1:]
		return true
	}

	if !ei.iterator.Next() {
		logger.Get().Debugf("No upstream events")
		ei.err = ei.iterator.Err()
		return false
	}

	ei.entry = ei.iterator.At()

	message, err := fastjson.ParseBytes(ei.entry.Message)
	if err != nil {
		logger.Get().Infof("json parsing error: %#v", err)
		return true
	}

	for _, es := range ei.eventSelector.selectors.Selectors {
		logger.Get().Debugf("Event selector: %#v", es)
		if ei.entry.Topic == es.TargetTopic {
			continue
		}
		/* #nosec */
		if checkEventSelection(message, &es) {
			selectedEvent := &types.Event{}
			*selectedEvent = *ei.entry
			origTopic, err := fastjson.Parse("\"" + ei.entry.Topic + "\"")
			if err != nil {
				logger.Get().Errorf("Topic parsing error: %#v", err)
				continue
			}
			message.Set("__orig_topic__", origTopic)
			selectedEvent.Topic = es.TargetTopic
			selectedEvent.Message = []byte(message.String())
			ei.selectedEvents = append(ei.selectedEvents, selectedEvent)
			logger.Get().Debugf("Selected event: %s and send to the topic: %s", selectedEvent.MessageString(), selectedEvent.Topic)
		}
	}

	return true
}

func (ei *EventIterator) At() *types.Event {
	return ei.entry
}

func (ei *EventIterator) Err() error {
	return ei.err
}
