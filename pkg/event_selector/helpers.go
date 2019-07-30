package event_selector

import (
	"github.com/valyala/fastjson"

	"github.com/anchorfree/data-go/pkg/logger"
)

func checkEventSelection(message *fastjson.Value, esc *SelectorConfig) bool {
	for field, pattern := range esc.Selectors {
		logger.Get().Debugf("Get field: %s, pattern: %s", field, pattern)
		value := message.GetStringBytes(field)
		logger.Get().Debugf("Get value: %s for field: %s", string(value), field)
		if value == nil || string(value) != pattern {
			return false
		}
	}
	return true
}