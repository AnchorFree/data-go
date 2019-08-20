package event_selector

import (
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anchorfree/data-go/pkg/logger"
)

func checkEventSelection(message *fastjson.Value, esc *Selector) bool {
	for field, pattern := range esc.Matching {
		logger.Get().Debugf("Get field: %s, pattern: %s", field, pattern)
		value := message.GetStringBytes(strings.Split(field, ".")...)
		logger.Get().Debugf("Get value: %s for field: %s", string(value), field)
		if value == nil || string(value) != pattern {
			return false
		}
	}
	return true
}
