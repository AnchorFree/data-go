package metricbuilder

import (
	"bytes"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/buger/jsonparser"
	"github.com/prometheus/client_golang/prometheus"
)

type ExporterProps struct {
	Name   string
	Topics []string
	Metric struct {
		Name string
		Help string
	}
	Aggregations []struct {
		Name         string
		Modify       string
		Path         []string
		UnpackedPath [][]string
		Values       []string
	}
}

var internalTime = time.Now()

type metric struct {
	Name      string
	Tags      map[string]string
	UpdatedAt time.Time
}

type MessagePayload struct {
	Msg   []byte
	Topic string
}

var expConfigs []ExporterProps
var LRUTimeout = 1 * time.Minute

//modify to read from configuration file
var timeBucket float64 = 5.0

var (
	metricsVec = make(map[string]*prometheus.CounterVec)
	metricsLRU = map[string]metric{}
)

var (
	mutexLRU = &sync.Mutex{}
	prom     *prometheus.Registry
)

type PathConfig struct {
	Names         []string
	Paths         [][]string
	DefaultValues map[string]string
}

var pathConfigs = []PathConfig{}

func init() {
	go func() {
		internalTime = time.Now()
		time.Sleep(1 * time.Second)
	}()
}

func Init(expProps []ExporterProps, promRegistry *prometheus.Registry) {
	prom = promRegistry
	expConfigs = expProps
	for i, e := range expConfigs {
		pc := PathConfig{DefaultValues: map[string]string{}}
		for j, a := range e.Aggregations {
			for _, v := range a.Path {
				unpackedPath := strings.Split(v, ".")
				expConfigs[i].Aggregations[j].UnpackedPath = append(expConfigs[i].Aggregations[j].UnpackedPath, unpackedPath)
				if len(v) > 0 {
					pc.Names = append(pc.Names, a.Name)
					pc.DefaultValues[a.Name] = ""
					pc.Paths = append(pc.Paths, unpackedPath)
				}
			}
		}
		pathConfigs = append(pathConfigs, pc)
	}
	for _, v := range expConfigs {
		aggregationsArray := func() []string {
			var tmp []string
			for _, val := range v.Aggregations {
				tmp = append(tmp, val.Name)
			}
			return tmp
		}
		metricsVec[v.Metric.Name] = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: v.Metric.Name,
				Help: v.Metric.Help,
			},
			aggregationsArray(),
		)
		prom.MustRegister(metricsVec[v.Metric.Name])
	}
}

//entry point
func PutIncomeMessage(mp MessagePayload) {
	createMetric(&mp.Msg, &mp.Topic)
}

func RunLRU() {
	go func() {
		for {
			for k, v := range metricsVec {
				purgeOldMetrics(k, v)
			}
			time.Sleep(LRUTimeout)
		}
	}()
}

func isCountableTopic(topic *string, exp *ExporterProps) bool {
	if len(exp.Topics) > 0 {
		for i := range exp.Topics {
			if *topic == exp.Topics[i] {
				return true
			}
		}
		return false
	}
	return false
}

func createMetric(message *[]byte, topic *string) {
	var m metric
	for k, v := range expConfigs {
		if !isCountableTopic(topic, &v) {
			continue
		}
		skip := false

		tags := fetchMessageTags(message, pathConfigs[k])
		for _, av := range v.Aggregations {
			match := false
			if len(av.Values) > 0 {
				for _, v := range av.Values {
					if v == tags[av.Name] {
						match = true
					}
				}
				if !match {
					skip = true
					break
				}
			}
		}
		if !skip && len(tags) > 0 {
			m.Name = v.Metric.Name
			m.Tags = tags
			m.UpdatedAt = internalTime

			metricsVec[m.Name].With(m.Tags).Inc()
			addMetricToLRU(&m)
		}
	}
}

func fetchMessageTags(message *[]byte, pc PathConfig) map[string]string {
	tags := pc.DefaultValues
	jsonparser.EachKey(*message, func(idx int, value []byte, vt jsonparser.ValueType, err error) {
		tags[pc.Names[idx]] = string(value)
	}, pc.Paths...)
	return tags
}

func modifyValue(modify *string, value string) string {
	switch *modify {
	case "tolower":
		value = strings.ToLower(value)
	case "toupper":
		value = strings.ToUpper(value)
	}
	return value
}

func filterMessage(message *[]byte, fieldName string, paths [][]string, values []string) (string, string) {
	var (
		val []byte
		err error
	)
	for _, path := range paths {
		val, _, _, err = jsonparser.Get(*message, path...)
		if err != nil && err.Error() == "Key path not found" {
			continue
		} else if err != nil {
			logger.Get().Warnf("error: %v", err)
		}

		if len(val) > 0 {
			break
		}
	}

	if len(values) == 0 {
		if len(val) == 0 {
			return fieldName, ""
		}
		return fieldName, string(val)
	}

	for _, v := range values {
		if string(val) == v {
			return fieldName, v
		}
	}
	return "", ""
}

func addMetricToLRU(m *metric) {
	tagsInline := string("")
	// map is not sorted in Go
	// but we need to keep order
	var keys []string
	for k := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, v := range keys {
		tagsInline += " " + m.Tags[v]
	}

	mutexLRU.Lock()
	metricsLRU[m.Name+" "+tagsInline[1:]] = *m
	mutexLRU.Unlock()
}

func purgeOldMetrics(metricName string, vec *prometheus.CounterVec) {
	mutexLRU.Lock()
	for k, v := range metricsLRU {
		if metricName == v.Name {
			if internalTime.Sub(v.UpdatedAt).Minutes() > timeBucket {
				vec.Delete(v.Tags)
				delete(metricsLRU, k)
			}
		}
	}
	mutexLRU.Unlock()
}

type Reader struct {
	line_reader.I
	reader line_reader.I
	topic  string
}

func NewReader(lr line_reader.I, topic string) *Reader {
	return &Reader{
		reader: lr,
		topic:  topic,
	}
}

func (r *Reader) ReadLine() (line []byte, offset uint64, err error) {
	const maxReplacements = 1
	line, offset, err = r.reader.ReadLine()
	PutIncomeMessage(MessagePayload{
		Msg:   bytes.Replace(line, []byte("{"), []byte(`{"topic":"`+r.topic+`",`), maxReplacements),
		Topic: r.topic,
	})
	return line, offset, err
}
