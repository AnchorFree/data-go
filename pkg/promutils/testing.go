package promutils

import (
	"bytes"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func Collect(c prom.Collector, metricNames ...string) (string, error) {
	reg := prom.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		return "", err
	}
	return Gather(reg, metricNames...)
}

func Gather(g prom.Gatherer, metricNames ...string) (string, error) {
	ms, err := g.Gather()
	if err != nil {
		return "", err
	}
	if metricNames != nil {
		var filtered []*dto.MetricFamily
		for _, m := range ms {
			for _, name := range metricNames {
				if m.GetName() == name {
					filtered = append(filtered, m)
					break
				}
			}
		}
		ms = filtered
	}
	encoded, err := encodeMetrics(ms)
	if err != nil {
		return "", err
	}
	return encoded, nil
}

func encodeMetrics(ms []*dto.MetricFamily) (string, error) {
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.FmtText)
	for _, m := range ms {
		if err := enc.Encode(m); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}
