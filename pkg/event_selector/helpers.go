package event_selector

import (
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/valyala/fastjson"
)

func checEvent(event *fastjson.Value, esc *SelectorConfig) bool {
	for field, pattern := range esc.Selectors {
		logger.Get().Infof("Get field: %s, pattern: %s", field, pattern)
		value := event.GetStringBytes(field)
		logger.Get().Infof("Get value: %s for field: %s", string(value), field)
		if value == nil || string(value) != pattern {
			return false
		}
	}
	return true
}
