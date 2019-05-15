package promutils

import (
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestGrpcRequests(t *testing.T) {

	testCounter := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "test_counter",
			Help: "Test Description",
		},
		[]string{"a"},
	)
	testGauge := prom.NewGaugeVec(
		prom.GaugeOpts{
			Name: "test_gauge",
			Help: "Test Description",
		},
		[]string{"a"},
	)
	testHisto := prom.NewHistogramVec(
		prom.HistogramOpts{
			Name: "test_histo",
			Help: "Test Description",
		},
		[]string{"a"},
	)
	testCounter.With(prom.Labels{"a": "123"}).Add(1)
	testCounter.With(prom.Labels{"a": "456"}).Add(1)

	testGauge.With(prom.Labels{"a": "777"}).Set(99)

	testHisto.With(prom.Labels{"a": "777"}).Observe(3)

	r := prom.NewRegistry()
	r.MustRegister(testCounter)
	r.MustRegister(testGauge)
	r.MustRegister(testHisto)

	expect := `# HELP test_counter Test Description
# TYPE test_counter counter
test_counter{a="123"} 1.0
test_counter{a="456"} 1.0
`
	rendered, err := Collect(testCounter)
	assert.Equal(t, expect, rendered)
	assert.NoError(t, err)

	expect = `# HELP test_gauge Test Description
# TYPE test_gauge gauge
test_gauge{a="777"} 99.0
`

	rendered, err = Collect(testGauge)
	assert.Equal(t, expect, rendered)
	assert.NoError(t, err)

	expect = `# HELP test_histo Test Description
# TYPE test_histo histogram
test_histo_bucket{a="777",le="0.005"} 0.0
test_histo_bucket{a="777",le="0.01"} 0.0
test_histo_bucket{a="777",le="0.025"} 0.0
test_histo_bucket{a="777",le="0.05"} 0.0
test_histo_bucket{a="777",le="0.1"} 0.0
test_histo_bucket{a="777",le="0.25"} 0.0
test_histo_bucket{a="777",le="0.5"} 0.0
test_histo_bucket{a="777",le="1.0"} 0.0
test_histo_bucket{a="777",le="2.5"} 0.0
test_histo_bucket{a="777",le="5.0"} 1.0
test_histo_bucket{a="777",le="10.0"} 1.0
test_histo_bucket{a="777",le="+Inf"} 1.0
test_histo_sum{a="777"} 3.0
test_histo_count{a="777"} 1.0
`

	rendered, err = Collect(testHisto)
	assert.Equal(t, expect, rendered)
	assert.NoError(t, err)

	expect = `# HELP test_counter Test Description
# TYPE test_counter counter
test_counter{a="123"} 1.0
test_counter{a="456"} 1.0
# HELP test_gauge Test Description
# TYPE test_gauge gauge
test_gauge{a="777"} 99.0
# HELP test_histo Test Description
# TYPE test_histo histogram
test_histo_bucket{a="777",le="0.005"} 0.0
test_histo_bucket{a="777",le="0.01"} 0.0
test_histo_bucket{a="777",le="0.025"} 0.0
test_histo_bucket{a="777",le="0.05"} 0.0
test_histo_bucket{a="777",le="0.1"} 0.0
test_histo_bucket{a="777",le="0.25"} 0.0
test_histo_bucket{a="777",le="0.5"} 0.0
test_histo_bucket{a="777",le="1.0"} 0.0
test_histo_bucket{a="777",le="2.5"} 0.0
test_histo_bucket{a="777",le="5.0"} 1.0
test_histo_bucket{a="777",le="10.0"} 1.0
test_histo_bucket{a="777",le="+Inf"} 1.0
test_histo_sum{a="777"} 3.0
test_histo_count{a="777"} 1.0
`
	rendered, err = Gather(r)
	assert.Equal(t, expect, rendered)
	assert.NoError(t, err)
}
