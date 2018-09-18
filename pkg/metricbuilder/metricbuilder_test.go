package metricbuilder

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestParsers_parse(t *testing.T) {
	testMsgBody := []byte(`{
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

	testTypes := []string{"gzip", "plain"}

	tables := []struct {
		fieldName  string
		fieldValue string
	}{
		{"error_code", ""},
		{"reason", ""},
		{"event", "app_start"},
	}

	cfp := "test.yaml"
	confFilePath = &cfp

	for _, v := range testTypes {
		var req *http.Request
		var err error
		if v == "gzip" {
			var buf bytes.Buffer
			g := gzip.NewWriter(&buf)
			g.Write(testMsgBody)
			// we need to close gzip writer before using it
			g.Close()
			req, err = http.NewRequest("POST", "/api/report/test", bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)

			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("Content-Encoding", "gzip")
		} else {
			req, err = http.NewRequest("POST", "/api/report/test", bytes.NewReader(testMsgBody))
			assert.Nil(t, err)
		}

		config = getConfig()
		config.Global.Topics = []string{"test"}

		H := &edgeHandler{
			cityDB: nil,
			ispDB:  nil,
		}

		router := mux.NewRouter()
		router.HandleFunc("/api/report/{topic}", H.gatewayHandler)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		assert.Equalf(t, http.StatusOK, rr.Code, "Failed http status code for context %s. Expected: [%v] Got: [%v]", v, http.StatusOK, rr.Code)
		//assert.Equal(t, 1, len(incomeMessageChannel), "Message didn't get into income channel in context %s", v)

		go parseIncomeMessageBody()

		testMetric := <-metricsChannel

		for k, v := range testMetric.Tags {
			for _, table := range tables {
				if k == table.fieldName {
					assert.Equalf(t, table.fieldValue, v, "Failed getting [%v] field. Expected: [%v] Got: [%v]", table.fieldName, table.fieldValue, v)
				}
			}
		}
	}
}

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

func TestMultiPath(t *testing.T) {
	config1 := []byte(`
exporters:
- name: "GPR breakdown"
  metric:
    name: "gpr_breakdown"
    help: "gpr_breakdown help"
  aggregations:
  - name: "platform"
    path:
    - "payload.platform"
    - "properties.platform"
    values: []
`)
	message1 := []byte(`{
		"event":"app_start",
		"payload":{
			"platform":"Android",
		},
		"properties": {
			"platform": "Windows",
		}
		}
	`)
	message2 := []byte(`{
		"event":"app_start",
		"properties": {
			"platform": "Windows",
		}
		}
	`)
	message3 := []byte(`{
		"event":"app_start",
		}`)
	message4 := []byte(`{
		"event":"app_start",
		"payload":{
			"platform":"Android",
		}
		}
	`)
	table := []struct {
		Message  *[]byte
		Config   *[]byte
		Fields   string
		Expected string
	}{
		{&message1, &config1, "platform", "Android"},
		{&message2, &config1, "platform", "Windows"},
		{&message3, &config1, "platform", ""},
		{&message4, &config1, "platform", "Android"},
	}

	for _, v := range table {
		config = parseConfig(v.Config)
		av := config.Exporters[0].Aggregations[0]
		_, value := filterMessage(v.Message, av.Name, av.UnpackedPath, av.Values)
		assert.Equal(t, v.Expected, value, "Expected different result")
	}
}
