package promutils

import (
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func GetVectorLabels(vec prom.Collector, filter prom.Labels) []prom.Labels {
	ch := make(chan prom.Metric)
	go func() {
		//Collect will lock metric map access inside
		vec.Collect(ch)
		close(ch)
	}()
	labelSets := []prom.Labels{}
	for metric := range ch {
		d := &dto.Metric{}
		metric.Write(d)
		ls := func() prom.Labels {
			ret := prom.Labels{}
			for _, labelPair := range d.GetLabel() {
				ret[labelPair.GetName()] = labelPair.GetValue()
				for fk, fv := range filter {
					if labelPair.GetName() == fk && labelPair.GetValue() != fv {
						return nil
					}
				}
			}
			return ret
		}()
		if ls != nil {
			labelSets = append(labelSets, ls)
		}
	}
	return labelSets
}
