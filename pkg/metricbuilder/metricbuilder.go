package metricbuilder

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anchorfree/gpr-edge/pkg/confreader"
	"github.com/anchorfree/ula-edge/pkg/line_reader"
	"github.com/anchorfree/ula-edge/pkg/logger"
	"github.com/buger/jsonparser"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsChannelBufferLength = uint8(50)
	incomeChannelBufferLength  = uint8(50)
)

type metric struct {
	Name      string
	Tags      map[string]string
	UpdatedAt time.Time
}

type MessagePayload struct {
	Msg   []byte
	Topic string
}

var conf *confreader.Configuration

//modify to read from configuration file
var timeBucket float64 = 5.0
var numParsers int = 5
var numReaders int = 2

var (
	incomeMessageChannel = make(chan MessagePayload, incomeChannelBufferLength)
	metricsChannel       = make(chan metric, metricsChannelBufferLength)
	metricsVec           = make(map[string]*prometheus.CounterVec)
	metricsLRU           = map[string]metric{}
)
var (
	incomeChannelCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpr_exporter_income_message_queue",
		Help: "Current capacity of the incoming message queue.",
	})
	metricsChannelCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpr_exporter_parsed_metrics_queue",
		Help: "Current capacity of the parsed metrics queue.",
	})
)

var (
	mutex = &sync.Mutex{}
	prom  *prometheus.Registry
)

func Init(cfg *confreader.Configuration, promRegistry *prometheus.Registry) {
	prom = promRegistry
	conf = cfg
	for _, v := range cfg.Exporters {
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
	prom.MustRegister(incomeChannelCapacity)
	prom.MustRegister(metricsChannelCapacity)

}

func PutIncomeMessage(mp MessagePayload) {
	incomeMessageChannel <- mp
}

func ParseIncomeMessageBody() {
	for m := range incomeMessageChannel {
		createMetric(&m.Msg, &m.Topic)
	}
}

func Run() {
	for i := 0; i < numReaders; i++ {
		go func() {
			for {
				cm := <-metricsChannel
				metricsVec[cm.Name].With(cm.Tags).Inc()
				addMetricToLRU(&cm)
			}
		}()
	}
	for i := 0; i < numParsers; i++ {
		go func() {
			ParseIncomeMessageBody()
		}()
	}
	go func() {
		for {
			for k, v := range metricsVec {
				purgeOldMetrics(k, v)
			}
			time.Sleep(time.Duration(5000 * time.Millisecond))
		}
	}()
	go func() {
		for {
			incomeChannelCapacity.Set(float64(len(incomeMessageChannel)))
			metricsChannelCapacity.Set(float64(len(metricsChannel)))
			time.Sleep(time.Duration(50 * time.Millisecond))
		}
	}()
}

func isCountableTopic(topic *string, exp *confreader.Exporter) bool {
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

	for _, v := range conf.Exporters {

		if !isCountableTopic(topic, &v) {
			continue
		}

		skip := false

		tags := make(map[string]string)

		for _, av := range v.Aggregations {

			fieldName, fieldValue := filterMessage(message, av.Name, av.UnpackedPath, av.Values)

			if fieldName == "" {
				skip = true
			}
			fieldValue = modifyValue(&av.Modify, fieldValue)
			tags[fieldName] = fieldValue
		}

		if skip {
			continue
		}

		if len(tags) > 0 {
			m.Name = v.Metric.Name
			m.Tags = tags
			m.UpdatedAt = time.Now()

			metricsChannel <- m
		}
	}
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

	var val []byte
	var err error

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
		tagsInline += fmt.Sprintf(" %s", m.Tags[v])
	}

	mutex.Lock()
	metricsLRU[m.Name+" "+tagsInline[1:]] = *m
	mutex.Unlock()
}

func purgeOldMetrics(metricName string, vec *prometheus.CounterVec) {

	timeNow := time.Now()
	mutex.Lock()
	for k, v := range metricsLRU {
		if metricName == v.Name {
			if timeNow.Sub(v.UpdatedAt).Minutes() > timeBucket {
				vec.Delete(v.Tags)
				delete(metricsLRU, k)
			}
		}
	}
	mutex.Unlock()
}

type MetricBuilderReader struct {
	line_reader.I
	reader line_reader.I
	topic  string
}

func NewMetricBuilderReader(lr line_reader.I, topic string) *MetricBuilderReader {
	return &MetricBuilderReader{
		reader: lr,
		topic:  topic,
	}
}

func (r *MetricBuilderReader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	PutIncomeMessage(MessagePayload{
		Msg:   bytes.Replace(line, []byte("}"), []byte(", \"topic\":\""+r.topic+"\"}"), bytes.LastIndex(line, []byte("}"))),
		Topic: r.topic,
	})
	return line, offset, err
}
