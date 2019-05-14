package promutils

import (
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestGetVectorLabels(t *testing.T) {

	counter := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "test_counter",
			Help: "A test counter",
		},
		[]string{"a", "b", "c"},
	)
	gauge := prom.NewGaugeVec(
		prom.GaugeOpts{
			Name: "test_counter",
			Help: "A test counter",
		},
		[]string{"a", "b", "c"},
	)
	histogram := prom.NewHistogramVec(
		prom.HistogramOpts{
			Name: "test_counter",
			Help: "A test counter",
		},
		[]string{"a", "b", "c"},
	)

	labelsArr := []prom.Labels{
		prom.Labels{
			"a": "aval1",
			"b": "bval1",
			"c": "cval1",
		},
		prom.Labels{
			"a": "aval2",
			"b": "bval2",
			"c": "cval2",
		},
		prom.Labels{
			"a": "aval3",
			"b": "bval3",
			"c": "cval3",
		},
	}

	for _, labels := range labelsArr {
		counter.With(labels).Add(1)
		gauge.With(labels).Set(1)
		histogram.With(labels).Observe(1)
	}

	assert.ElementsMatch(t, labelsArr, GetVectorLabels(counter, prom.Labels{}))
	assert.ElementsMatch(t, labelsArr, GetVectorLabels(gauge, prom.Labels{}))
	assert.ElementsMatch(t, labelsArr, GetVectorLabels(histogram, prom.Labels{}))

	assert.ElementsMatch(t, []prom.Labels{labelsArr[0]}, GetVectorLabels(counter, prom.Labels{"a": "aval1"}))
	assert.ElementsMatch(t, []prom.Labels{labelsArr[1]}, GetVectorLabels(gauge, prom.Labels{"b": "bval2"}))
	assert.ElementsMatch(t, []prom.Labels{labelsArr[2]}, GetVectorLabels(histogram, prom.Labels{"c": "cval3"}))

	//filter no match
	assert.ElementsMatch(t, []prom.Labels{}, GetVectorLabels(counter, prom.Labels{"a": "neverfound"}))
	//two filter labels with one non-matching
	assert.ElementsMatch(t, []prom.Labels{}, GetVectorLabels(gauge, prom.Labels{"a": "aval1", "b": "neverfound"}))
	//filter should treat multiple labels with AND logic
	assert.ElementsMatch(t, []prom.Labels{}, GetVectorLabels(histogram, prom.Labels{"a": "aval1", "b": "bval2"}))
	assert.ElementsMatch(t, []prom.Labels{labelsArr[0]}, GetVectorLabels(histogram, prom.Labels{"a": "aval1", "b": "bval1"}))
}
