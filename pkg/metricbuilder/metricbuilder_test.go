package metricbuilder

import (
	"bytes"
	"fmt"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
	"reflect"
	"testing"
	"time"
)

var testConfig = []byte(`
gpr_first:
  help: "gpr_first help"
  topics:
   - "test"
  labels:
    topic:
      paths:
      - "topic"
      values: []
    platform:
      modify: "tolower"
      paths:
      - "payload.af_platform"
      - "properties.af_platform"
      values: []
    app_version:
      modify: "tolower"
      paths:
      - "payload.app_version"
      - "properties.app_version"
      values: []
    from_country:
      paths:
      - "from_country"
      values: []
    error_code:
      paths:
      - "payload.error_code"
      - "properties.error_code"
      values: []
    reason:
      modify: "tolower"
      paths:
      - "payload.reason"
      - "properties.reason"
      values: []
    first:
      paths:
      - "payload.first"
      - "properties.first"
      values: []
    event:
      paths:
      - "event"
      values:
      - app_start
      - connection_start
      - connection_end
`)

var testString = []byte(`{
		"event":"app_start",
		"ts":1521800858842,
		"payload":{
		"local_time":"2018-03-23T14:57:38-04:30",
		"seq_no":593,
		"platform":"Android",
		"af_platform":"android",
		"os":27,
		"os_version":"8.1.0",
		"manufacturer":"Google",
		"brand":"google",
		"model":"Pixel",
		"device_language":"en",
		"screen_dpi":420,
		"screen_height":1794,
		"screen_width":1080,
		"app_version":"5.9.2",
		"app_release":59200,
		"carrier":"IR-TCI",
		"wifi":false,
		"has_nfc":true,
		"has_telephone":true,
		"sim_country":"IR",
		"af_hash":"G91210CA1AC0330138BFDC8AEBC9CA24",
		"app_build":"1b74fd4082fa449f",
		"epoch":1521665364740,
		"app_name":"hssb.android.free.app",
		"dist_channel":"basic",
		"sample_chance":49,
		"ucr_experiments":"[\"AND_723_B, AND_681_C\"]",
		"google_play_services":"available",
		"distinct_id":"59c62e1c-29bf-4e2b-9396-a1a4a7c2e825",
		"advertiser":"admob",
		"ad_type":"ad_interstitial",
		"flags":"{\"android_fg_permission\":false,\"android_vpn_permission\":true,\"unknown_sources_enabled\":true}",
		"session_id":"",
		"advertiser_id":26,
		"placement_id":"ca-app-pub-4751437627903161\/3696802127",
		"ucr_sd_source":"embedded",
		"ad_id":13,
		"user_type":"free",
		"aaid":"Q50B47AB0D1C55CB79F8CDCFD59C5B35A",
		"catime":"",
		"caid":"",
		"ucr_hydra_mode":"sticky",
		"time":1521800858842,
		"sampled":false,
		"af_token":"3d416b03616ad3a80000000272677331"
		},
		"host":"favoriteshoes.us",
		"from_country_source":"ngx.var.geoip_country_code",
		"cloudfront":0,
		"ngx_var_remote_addr":"113.203.84.0",
		"via":"74.115.4.69",
		"from_country":"AE",
		"from_ip":"113.203.84.0",
		"server_ts":1521800927956,
		"client_ts":1521800918976
		}{
		"event":"app_start",
		"ts":1521800858842,
		"payload":{
		"local_time":"2018-03-23T14:57:38-04:30",
		"seq_no":593,
		"platform":"Android",
		"af_platform":"android",
		"os":27,
		"os_version":"8.1.0",
		"manufacturer":"Google",
		"brand":"google",
		"model":"Pixel",
		"device_language":"en",
		"screen_dpi":420,
		"screen_height":1794,
		"screen_width":1080,
		"app_version":"5.9.2",
		"app_release":59200,
		"carrier":"IR-TCI",
		"wifi":false,
		"has_nfc":true,
		"has_telephone":true,
		"sim_country":"IR",
		"af_hash":"G91210CA1AC0330138BFDC8AEBC9CA24",
		"app_build":"1b74fd4082fa449f",
		"epoch":1521665364740,
		"app_name":"hssb.android.free.app",
		"dist_channel":"basic",
		"sample_chance":49,
		"ucr_experiments":"[\"AND_723_B, AND_681_C\"]",
		"google_play_services":"available",
		"distinct_id":"59c62e1c-29bf-4e2b-9396-a1a4a7c2e825",
		"advertiser":"admob",
		"ad_type":"ad_interstitial",
		"flags":"{\"android_fg_permission\":false,\"android_vpn_permission\":true,\"unknown_sources_enabled\":true}",
		"session_id":"",
		"advertiser_id":26,
		"placement_id":"ca-app-pub-4751437627903161\/3696802127",
		"ucr_sd_source":"embedded",
		"ad_id":13,
		"user_type":"free",
		"aaid":"Q50B47AB0D1C55CB79F8CDCFD59C5B35A",
		"catime":"",
		"caid":"",
		"ucr_hydra_mode":"sticky",
		"time":1521800858842,
		"sampled":false,
		"af_token":"3d416b03616ad3a80000000272677331"
		},
		"host":"favoriteshoes.us",
		"from_country_source":"ngx.var.geoip_country_code",
		"cloudfront":0,
		"ngx_var_remote_addr":"113.203.84.0",
		"via":"74.115.4.69",
		"from_country":"AE",
		"from_ip":"113.203.84.0",
		"server_ts":1521800927956,
		"client_ts":1521800918976
		}
`)

func TestModifyValue(t *testing.T) {
	table := []struct {
		Modify   string
		Value    string
		Expected string
	}{
		{"tolower", "Application", "application"},
		{"tolower", "", ""},
		{"", "Application", "Application"},
		{"toupper", "Application", "APPLICATION"},
	}
	for _, v := range table {
		result := modifyValue(&v.Modify, v.Value)
		assert.Equalf(t, v.Expected, result, "Modify value doesn't work")
	}

}

func TestFetchMessageTags(t *testing.T) {
	testTable := []struct {
		Name           string
		Message        []byte
		Config         []byte
		LabelsOverride []byte
		Field          string
		Expected       string
	}{
		{
			"simple properties match",
			[]byte(`{"event":"app_start","properties":{"platform":"Windows"}}`),
			testConfig,
			//map[string]Label{"platform": Label{Paths: []string{"properties.platform", "payload.platform"}}},
			[]byte(`{platform: {paths: ["properties.platform", "payload.platform"]}}`),
			"platform",
			"Windows",
		},
		{
			"simple payload match",
			[]byte(`{ "event":"app_start","payload":{"platform":"Android"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["properties.platform", "payload.platform"]}}`),
			"platform",
			"Android",
		},
		{
			"platform match with two possible variants",
			[]byte(`{"event":"app_start","payload":{"platform":"Android"},"properties":{"platform":"Windows"}}`),
			testConfig,
			//map[string]Label{},
			[]byte(`{platform: {paths: ["properties.platform", "payload.platform"]}}`),
			"platform",
			"Windows", //gets overwritten by the latest match
		},
		{
			"missing af_platform field",
			[]byte(`{"event":"app_start"}`),
			testConfig,
			[]byte(`{platform: {paths: ["properties.platform", "payload.platform"]}}`),
			"platform",
			"",
		},
		{
			"no path",
			[]byte(`{"event":"app_start","payload":{"platform":"P"}}`),
			testConfig,
			[]byte(`{platform: {paths: []}}`),
			"platform",
			"",
		},
		{
			"empty path",
			[]byte(`{"event":"app_start","payload":{"platform":"P"}}`),
			testConfig,
			[]byte(`{platform: {paths: [""]}}`),
			"platform",
			"",
		},
		{
			"duplicate path",
			[]byte(`{"event":"app_start","payload":{"platform":"P"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform", "payload.platform"]}}`),
			"platform",
			"P",
		},
		{
			"non-empty and empty paths",
			[]byte(`{"event":"app_start","payload":{"platform":"P"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform", ""]}}`),
			"platform",
			"P",
		},
		{
			"empty path",
			[]byte(`{"event":"app_start","payload":{"platform":""}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"",
		},
		{
			"broken json",
			[]byte(`{"event":"app_start","payl`),
			testConfig,
			[]byte(`{platform: {paths: []}}`),
			"platform",
			"",
		},
		{
			"duplicates",
			[]byte(`{"event":"app_start","payload":{"platform":"A","platform":"B"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"A",
		},
		{
			"empty field",
			[]byte(`{"event":"app_start","payload":{"platform":""}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"",
		},
		{
			"null value",
			[]byte(`{"event":"app_start","payload":{"platform": null}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"null",
		},
		{
			"numeric value",
			[]byte(`{"event":"app_start","payload":{"platform": 747}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"747",
		},
		{
			"value is not in while list",
			[]byte(`{"event":"app_start","payload":{"platform": "Z"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"], values: ["P", "R", "S"]}}`),
			"platform",
			"Z",
		},
		{
			"Unicode symbols",
			[]byte(`{"event":"app_start","payload":{"platform": "Железяка` + "\u1234" + `"}}`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"Железяка\u1234",
		},
		{
			"malformed json with negative level",
			[]byte(`}}}[`),
			testConfig,
			[]byte(`{platform: {paths: ["payload.platform"]}}`),
			"platform",
			"",
		},
	}

	for testIndex, test := range testTable {
		//topic := "test"
		metricName := "gpr_first"
		promReg := prometheus.NewRegistry()
		mConfigs := HelperMetricsConfigFromBytes(t, test.Config)
		if len(test.LabelsOverride) > 0 {
			for labelName, labelOverride := range HelperLabelsConfigFromBytes(t, test.LabelsOverride) {
				mConfigs[metricName].Labels[labelName] = labelOverride
			}
		}
		Init(Props{Metrics: mConfigs}, promReg)
		//pathConfigs is a global var that gets filled in Init()
		tags := fetchMessageTags(test.Message, pathConfigs[metricName])
		assert.Equalf(t, test.Expected, tags[test.Field], `test #%d: %s`, testIndex, test.Name)
	}
}

func TestUpdateMetric(t *testing.T) {
	topic := "test"
	var config = []byte(fmt.Sprintf(`
gpr_first:
  help: "gpr_first help"
  topics:
   - "%s"
  labels:
    topic:
      paths:
      - "topic"
      values: []
    platform:
      paths:
      - "payload.platform"
      values: []
    event:
      paths:
      - "event"
      values:
      - A
      - B
      - C
`, topic))

	testTable := []struct {
		Name               string
		Message            []byte
		Config             []byte
		LabelsConfOverride []byte
		Labels             map[string]string
		Value              float64
	}{
		{
			"topic + event + platform",
			[]byte(`{"event":"A","payload":{"platform": "P"}}`),
			config,
			[]byte(``),
			map[string]string{"topic": topic, "event": "A", "platform": "P"},
			float64(1),
		},
		{
			"event out of listed value set",
			[]byte(`{"event":"Z","payload":{"platform": "P"}}`),
			config,
			[]byte(``),
			map[string]string{},
			float64(-1),
		},
		{
			"missing field",
			[]byte(`{"event":"A","payload":{"somefield": "somevalue"}}`),
			config,
			[]byte(``),
			map[string]string{"topic": topic, "event": "A", "platform": ""},
			float64(1),
		},
		{
			"empty field",
			[]byte(`{"event":"A","payload":{"platform": ""}}`),
			config,
			[]byte(``),
			map[string]string{"topic": topic, "event": "A", "platform": ""},
			float64(1),
		},
	}

	for testIndex, test := range testTable {
		metricName := "gpr_first"
		promReg := prometheus.NewRegistry()
		mConfigs := HelperMetricsConfigFromBytes(t, test.Config)
		if len(test.LabelsConfOverride) > 0 {
			for labelName, labelOverride := range HelperLabelsConfigFromBytes(t, test.LabelsConfOverride) {
				mConfigs[metricName].Labels[labelName] = labelOverride
			}
		}
		Init(Props{Metrics: mConfigs}, promReg)
		//pathConfigs is a global var that gets filled in Init()
		updateMetric(appendTopicToMessage(test.Message, topic), topic)
		foundMetrics := HelperFetchPromCounters(t, promReg)
		if len(test.Labels) == 0 && test.Value == -1 {
			assert.Equalf(t, 0, len(foundMetrics), "Should not find any metrics")
		} else {
			assert.Equal(t, 1, len(foundMetrics), "Should find single metric")
			metric := foundMetrics[0]
			assert.Equalf(t, test.Labels, metric.Labels, "test #%d (%s): label set does not match", testIndex, test.Name)
			assert.Equalf(t, test.Value, metric.Value, "test #%d (%s): label value does not match", testIndex, test.Name)
		}

	}
}

func TestReader(t *testing.T) {
	topic := "test"
	config := []byte(fmt.Sprintf(`
gpr_first:
  help: "gpr_first help"
  topics:
   - "%s"
  labels:
    topic:
      paths:
      - "topic"
      values: []
    platform:
      paths:
      - "payload.platform"
      values: []
    event:
      paths:
      - "event"
      values:
      - A
      - B
      - C
`, topic))
	data := []byte(`{"event":"A","payload":{"platform": "P1"}}
{"event":"B","payload":{"platform": "P2"}}
{"event":"B","payload":{"platform": "P2"}}
{"event":"B","payload":{"platform": "P2"}}
{"event":"C","payload":{"platform": "P3"}}
{"event":"C","payload":{"platform": "P3"}}
`)

	expectedMetrics := []struct {
		Value  float64
		Labels map[string]string
	}{
		{float64(1), map[string]string{"topic": topic, "event": "A", "platform": "P1"}},
		{float64(3), map[string]string{"topic": topic, "event": "B", "platform": "P2"}},
		{float64(2), map[string]string{"topic": topic, "event": "C", "platform": "P3"}},
	}
	metricName := "gpr_first"
	promReg := prometheus.NewRegistry()
	mConfigs := HelperMetricsConfigFromBytes(t, config)
	Init(Props{Metrics: mConfigs}, promReg)
	lor := line_offset_reader.NewIterator(bytes.NewReader(data), topic)
	mb := NewIterator(lor)
	for_cnt := 0
	for mb.Next() {
		_ = mb.At()
		for_cnt++
	}
	foundMetrics := HelperFetchPromCounters(t, promReg)
	for _, em := range expectedMetrics {
		found := false
		for _, fm := range foundMetrics {
			if reflect.DeepEqual(em.Labels, fm.Labels) {
				found = true
				assert.Equal(t, metricName, fm.Name, "metric name does not match")
				assert.Equal(t, em.Labels, fm.Labels, "label set does not match")
				assert.Equal(t, em.Value, fm.Value, "metric value does not match")
				break
			}
		}
		assert.Truef(t, found, "Did not find expected metric: %+v", em)
	}
}

func TestPurgeOldMetrics(t *testing.T) {
	ResetMetricsLRU()
	topic := "test"
	config := []byte(fmt.Sprintf(`
gpr_first:
  help: "gpr_first help"
  topics:
   - "%s"
  labels:
    topic:
      paths:
      - "topic"
      values: []
    platform:
      paths:
      - "payload.platform"
      values: []
    event:
      paths:
      - "event"
      values:
      - A
      - B
      - C
`, topic))
	message1 := []byte(`{"event":"A","payload":{"platform": "P"}}`)
	message2 := []byte(`{"event":"B","payload":{"platform": "P"}}`)
	labels1 := map[string]string{"topic": topic, "event": "A", "platform": "P"}
	labels2 := map[string]string{"topic": topic, "event": "B", "platform": "P"}
	metricName := "gpr_first"
	promReg := prometheus.NewRegistry()
	mConfigs := HelperMetricsConfigFromBytes(t, config)
	Init(Props{Metrics: mConfigs}, promReg)
	LRUTimeBucket = 1 //seconds
	batchSize := 10
	for i := 1; i <= batchSize; i++ {
		updateMetric(appendTopicToMessage(message1, topic), topic)
	}
	time.Sleep(2 * time.Second)
	for i := 1; i <= batchSize; i++ {
		updateMetric(appendTopicToMessage(message2, topic), topic)
	}
	foundMetrics := HelperFetchPromCounters(t, promReg)
	assert.Equal(t, 2, len(foundMetrics), "Should find a metric with 2 label sets")
	for _, metric := range foundMetrics {
		if reflect.DeepEqual(metric.Labels, labels1) {
			assert.Equal(t, metricName, metric.Name, "metric name does not match")
			assert.Equal(t, labels1, metric.Labels, "label set does not match")
			assert.Equal(t, float64(batchSize), metric.Value, "metric value does not match")
		} else {
			assert.Equal(t, metricName, metric.Name, "metric name does not match")
			assert.Equal(t, labels2, metric.Labels, "label set does not match")
			assert.Equal(t, float64(batchSize), metric.Value, "metric value does not match")
		}
	}
	// has to clean up all the metrics with last update time older than LRUTimeBucket
	purgeOldMetrics()
	foundMetrics = HelperFetchPromCounters(t, promReg)
	assert.Equal(t, 1, len(foundMetrics), "Should find a metric with 1 label sets")
	metric := foundMetrics[0]
	assert.Equalf(t, labels2, metric.Labels, "label set does not match")
	assert.Equalf(t, float64(batchSize), metric.Value, "metric value does not match")
	time.Sleep(2 * time.Second)
	purgeOldMetrics()
	foundMetrics = HelperFetchPromCounters(t, promReg)
	assert.Equal(t, 0, len(foundMetrics), "Should not find test metric")
}

func TestIsCountableTopic(t *testing.T) {
	metricName := "gpr_first"
	promReg := prometheus.NewRegistry()
	mConfigs := HelperMetricsConfigFromBytes(t, testConfig)
	mConfig := mConfigs[metricName]
	Init(Props{Metrics: mConfigs}, promReg)
	assert.False(t, isCountableTopic("somedummytopic", &mConfig), "dummy topic should not be allowed")
	assert.False(t, isCountableTopic("", &mConfig), "empty topic should not be allowed")
	assert.True(t, isCountableTopic("test", &mConfig), "test topic should be allowed")
}

func BenchmarkUpdateMetric(b *testing.B) {
	promReg := prometheus.NewRegistry()
	mConfigs := HelperMetricsConfigFromBytes(b, testConfig)
	topic := "test"
	Init(Props{Metrics: mConfigs}, promReg)

	b.ResetTimer()
	msg := HelperFlattenMessage(b, testString)
	for i := 0; i < b.N; i++ {
		updateMetric(msg, topic)
	}
	b.StopTimer()
}

type PromMetric struct {
	Name   string
	Labels map[string]string
	Value  float64
}

func HelperFetchPromCounters(t *testing.T, reg *prometheus.Registry) []PromMetric {
	metricFamily, err := reg.Gather()
	assert.Nil(t, err, "Could not gather metrics: %v", err)
	ret := []PromMetric{}
	for _, mf := range metricFamily {
		for _, m := range mf.Metric {
			metric := PromMetric{Labels: map[string]string{}}
			metric.Name = *mf.Name
			metric.Value = *m.Counter.Value
			for _, l := range m.Label {
				metric.Labels[*l.Name] = *l.Value
			}
			ret = append(ret, metric)
		}
	}
	return ret
}

func HelperMetricsConfigFromBytes(t testing.TB, data []byte) map[string]MetricProps {
	var metricConfigs map[string]MetricProps
	if err := yaml.Unmarshal(data, &metricConfigs); err != nil {
		t.Fatalf("error: %v", err)
	}
	return metricConfigs
}

func HelperLabelsConfigFromBytes(t testing.TB, data []byte) map[string]Label {
	var labelsConfig map[string]Label
	if err := yaml.Unmarshal(data, &labelsConfig); err != nil {
		t.Fatalf("error: %v", err)
	}
	return labelsConfig
}

func HelperFlattenMessage(t testing.TB, m []byte) []byte {
	return bytes.Replace(
		bytes.Replace(testString, []byte("\n"), []byte(""), -1),
		[]byte("\t"),
		[]byte(""), -1,
	)
}
