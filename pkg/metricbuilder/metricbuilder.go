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

type Props struct {
	Metrics map[string]MetricProps
}

type MetricProps struct {
	Topics []string
	Help   string
	Labels map[string]struct {
		Modify string
		Paths  []string
		Values []string
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

var metricConfigs map[string]MetricProps
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

var pathConfigs = map[string]PathConfig{}

func init() {
	go func() {
		internalTime = time.Now()
		time.Sleep(1 * time.Second)
	}()
}

func Init(config Props, promRegistry *prometheus.Registry) {
	prom = promRegistry
	metricConfigs = config.Metrics
	//for i, e := range metricConfigs {
	for metricName, metricConfig := range metricConfigs {
		pc := PathConfig{DefaultValues: map[string]string{}}
		//for j, a := range e.Labels {
		for labelName, labelConfig := range metricConfig.Labels {
			for _, path := range labelConfig.Paths {
				splitPath := strings.Split(path, ".")
				if len(path) > 0 {
					pc.Names = append(pc.Names, labelName)
					pc.DefaultValues[labelName] = ""
					pc.Paths = append(pc.Paths, splitPath)
				}
			}
		}
		if len(pc.Names) != len(pc.Paths) {
			logger.Get().Fatal("Error initializing merged paths config")
		}
		pathConfigs[metricName] = pc
	}
	for metricName, metricConfig := range metricConfigs {
		labelsArray := func() []string {
			var tmp []string
			for labelName, _ := range metricConfig.Labels {
				tmp = append(tmp, labelName)
			}
			return tmp
		}
		metricsVec[metricName] = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: metricName,
				Help: metricConfig.Help,
			},
			labelsArray(),
		)
		prom.MustRegister(metricsVec[metricName])
	}
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

func isCountableTopic(topic string, mConfig *MetricProps) bool {
	if len(mConfig.Topics) > 0 {
		for i := range mConfig.Topics {
			if topic == mConfig.Topics[i] {
				return true
			}
		}
		return false
	}
	return false
}

func updateMetric(message []byte, topic string) {
	var m metric
	for metricName, metricConf := range metricConfigs {
		if !isCountableTopic(topic, &metricConf) {
			continue
		}
		skip := false

		tags := fetchMessageTags(message, pathConfigs[metricName])
		for labelName, labelConfig := range metricConf.Labels {
			match := false
			if len(labelConfig.Values) > 0 {
				for _, v := range labelConfig.Values {
					if v == tags[labelName] {
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
			m.Name = metricName
			m.Tags = tags
			m.UpdatedAt = internalTime

			metricsVec[m.Name].With(m.Tags).Inc()
			addMetricToLRU(&m)
		}
	}
}

func fetchMessageTags(message []byte, pc PathConfig) map[string]string {
	tags := pc.DefaultValues
	jsonparser.EachKey(message, func(idx int, value []byte, vt jsonparser.ValueType, err error) {
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
	updateMetric(
		bytes.Replace(line, []byte("{"), []byte(`{"topic":"`+r.topic+`",`), maxReplacements),
		r.topic,
	)
	return line, offset, err
}
