package extra_fields

import (
	"bytes"
	"encoding/json"
	"fmt"
	lor "github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"
)

type EF struct {
	ClientTs    int64  `json:"client_ts"`
	CloudFront  int    `json:"cloudfront"`
	FromAsDesc  string `json:"from_as_desc"`
	FromAsn     string `json:"from_asn"`
	FromCity    string `json:"from_city"`
	FromCountry string `json:"from_country"`
	FromIsp     string `json:"from_isp"`
	FromOrgName string `json:"from_org_name"`
	Host        string `json:"host"`
	ServerTs    int64  `json:"server_ts"`
}

var raw []byte = []byte(`{"event":"test","payload":{"field": "hi"},"tail":"latest"}`)

func TestExtraFieldsFromHeaders(t *testing.T) {
	/*
	   "host":"favoriteshoes.us"
	   "from_country_source":"ngx.var.geoip_country_code"
	   "cloudfront":0
	   "ngx_var_remote_addr":"113.203.84.0"
	   "via":"74.115.4.69"
	   "from_country":"AE"
	   "from_ip":"113.203.84.0"
	   "server_ts":1521800927956
	   "client_ts":1521800918976
	*/
	topic := "test"
	host := "favoriteshoes.com"
	testIP := "1.128.0.0"
	fromCountry := "UA"
	fromCity := "Kiev"
	fromAsn := "54500"
	fromAsDesc := "AS54500"
	fromIsp := "AnchorFree"
	cloudFront := 1
	fromOrgName := "EGIHosting"
	serverTs := int64(1521800927956)
	clientTs := int64(1521800918976)

	path := fmt.Sprintf("/ula?report_type=%s", topic)

	// test with geo data passed through headers
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	req.RemoteAddr = testIP
	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Cf-Id", fmt.Sprintf("%d", cloudFront))
	req.Header.Set("x_af_c_country", fromCountry)
	req.Header.Set("x_af_c_city", fromCity)
	req.Header.Set("x_af_asn", fromAsn)
	req.Header.Set("x_af_asdescription", fromAsDesc)
	req.Header.Set("x_af_ispname", fromIsp)
	req.Header.Set("x_af_orgname", fromOrgName)
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", "test-data/geo.conf")
	extraFields := map[string]interface{}{
		"server_ts": serverTs,
		"client_ts": clientTs,
	}
	lineReader := lor.NewLineOffsetReader(bytes.NewReader(raw))
	efr := NewExtraFieldsReader(lineReader, req).With(extraFields)

	for {
		line, _, readerErr := efr.ReadLine()
		var rec EF
		err := json.Unmarshal(line, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, clientTs, rec.ClientTs, "client_ts field is not correct")
		assert.Equal(t, cloudFront, rec.CloudFront, "cloudfront field is not correct")
		assert.Equal(t, fromAsDesc, rec.FromAsDesc, "from_as_desc field is not correct")
		assert.Equal(t, fromAsn, rec.FromAsn, "from_asn field is not correct")
		assert.Equal(t, fromCity, rec.FromCity, "from_city field is not correct")
		assert.Equal(t, fromCountry, rec.FromCountry, "from_country field is not correct")
		assert.Equal(t, fromIsp, rec.FromIsp, "from_isp field is not correct")
		assert.Equal(t, fromOrgName, rec.FromOrgName, "from_org_name field is not correct")
		assert.Equal(t, host, rec.Host, "host field is not correct")
		assert.Equal(t, serverTs, rec.ServerTs, "server_ts field is not correct")

		if readerErr != nil {
			break
		}
	}
}

func TestExtraFieldsFromIspDb(t *testing.T) {
	// test with geo data from ISP mmdb
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", "test-data/geo.conf")
	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	testIP := "1.128.0.0"
	req.RemoteAddr = testIP
	lineReader := lor.NewLineOffsetReader(bytes.NewReader(raw))
	efr := NewExtraFieldsReader(lineReader, req)
	for {
		line, _, readerErr := efr.ReadLine()
		var rec EF
		err := json.Unmarshal(line, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, "Telstra Pty Ltd", rec.FromAsDesc, "from_as_desc field is not correct")
		assert.Equal(t, "1221", rec.FromAsn, "from_asn field is not correct")
		assert.Equal(t, "Telstra Internet", rec.FromIsp, "from_isp field is not correct")
		assert.Equal(t, "Telstra Internet", rec.FromOrgName, "from_org_name field is not correct")

		if readerErr != nil {
			break
		}
	}

}

func TestExtraFieldsFromCityDb(t *testing.T) {
	// test with geo data from ISP mmdb
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", "test-data/geo.conf")
	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	testIP := "81.2.69.160"
	req.RemoteAddr = testIP
	lineReader := lor.NewLineOffsetReader(bytes.NewReader(raw))
	efr := NewExtraFieldsReader(lineReader, req)
	for {
		line, _, readerErr := efr.ReadLine()
		var rec EF
		err := json.Unmarshal(line, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, "London", rec.FromCity, "from_city field is not correct")
		assert.Equal(t, "GB", rec.FromCountry, "from_country field is not correct")

		if readerErr != nil {
			break
		}
	}

}
