package extra_fields

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/anchorfree/data-go/pkg/geo"
	lor "github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"
)

type EF struct {
	ClientTs      int64   `json:"client_ts"`
	CloudFront    int     `json:"cloudfront"`
	FromAsDesc    string  `json:"from_as_desc"`
	FromAsn       string  `json:"from_asn"`
	FromCity      string  `json:"from_city"`
	FromCountry   string  `json:"from_country"`
	FromRegion    string  `json:"from_region"`
	FromIsp       string  `json:"from_isp"`
	FromOrgName   string  `json:"from_org_name"`
	FromLatitude  float64 `json:"from_latitude"`
	FromLongitude float64 `json:"from_longitude"`
	Host          string  `json:"host"`
	ServerTs      int64   `json:"server_ts"`
}

var raw []byte = []byte(`{"event":"test","payload":{"field": "hi"},"tail":"latest"}`)

func TestExtraFieldsFromHeaders(t *testing.T) {
	topic := "test"
	host := "favoriteshoes.com"
	testIP := "1.128.0.0"
	fromCountry := "UA"
	fromCity := "Kiev"
	fromRegion := "Berdichevsky"
	fromAsn := "54500"
	fromAsDesc := "AS54500"
	fromIsp := "AnchorFree"
	fromLatitude := float64(50.433300)
	fromLongitude := float64(30.516700)
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
	req.Header.Set("x_af_c_region", fromRegion)
	req.Header.Set("x_af_c_ll", fmt.Sprintf("%f,%f", fromLatitude, fromLongitude))
	req.Header.Set("x_af_asn", fromAsn)
	req.Header.Set("x_af_asdescription", fromAsDesc)
	req.Header.Set("x_af_ispname", fromIsp)
	req.Header.Set("x_af_orgname", fromOrgName)
	geoSet = geo.NewGeo().FromFile("geo-test.conf")
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", geoSet)
	extraFields := map[string]interface{}{
		"server_ts": serverTs,
		"client_ts": clientTs,
	}
	lineReader := lor.NewIterator(bytes.NewReader(raw), topic)
	efi := NewIterator(lineReader, req).With(extraFields)

	for efi.Next() {
		event := efi.At()
		var rec EF
		err := json.Unmarshal(event.Message, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, clientTs, rec.ClientTs, "client_ts field is not correct")
		assert.Equal(t, cloudFront, rec.CloudFront, "cloudfront field is not correct")
		assert.Equal(t, fromAsDesc, rec.FromAsDesc, "from_as_desc field is not correct")
		assert.Equal(t, fromAsn, rec.FromAsn, "from_asn field is not correct")
		assert.Equal(t, fromCity, rec.FromCity, "from_city field is not correct")
		assert.Equal(t, fromRegion, rec.FromRegion, "from_region field is not correct")
		assert.Equal(t, fromCountry, rec.FromCountry, "from_country field is not correct")
		assert.Equal(t, fromLatitude, rec.FromLatitude, "from_latitude field is not correct")
		assert.Equal(t, fromLongitude, rec.FromLongitude, "from_longitude field is not correct")
		assert.Equal(t, fromIsp, rec.FromIsp, "from_isp field is not correct")
		assert.Equal(t, fromOrgName, rec.FromOrgName, "from_org_name field is not correct")
		assert.Equal(t, host, rec.Host, "host field is not correct")
		assert.Equal(t, serverTs, rec.ServerTs, "server_ts field is not correct")
	}
}

func TestExtraFieldsFromIspDb(t *testing.T) {
	// test with geo data from ISP mmdb
	geoSet = geo.NewGeo().FromFile("geo-test.conf")
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", geoSet)
	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	testIP := "1.128.0.0"
	req.RemoteAddr = testIP
	lineReader := lor.NewIterator(bytes.NewReader(raw), topic)
	efi := NewIterator(lineReader, req)
	for efi.Next() {
		event := efi.At()
		var rec EF
		err := json.Unmarshal(event.Message, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, "Telstra Pty Ltd", rec.FromAsDesc, "from_as_desc field is not correct")
		assert.Equal(t, "1221", rec.FromAsn, "from_asn field is not correct")
		assert.Equal(t, "Telstra Internet", rec.FromIsp, "from_isp field is not correct")
		assert.Equal(t, "Telstra Internet", rec.FromOrgName, "from_org_name field is not correct")
	}
}

func TestExtraFieldsFromCityDb(t *testing.T) {
	// test with geo data from ISP mmdb
	geoSet = geo.NewGeo().FromFile("geo-test.conf")
	Init("test-data/test-data/GeoIP2-City-Test.mmdb", "test-data/test-data/GeoIP2-ISP-Test.mmdb", geoSet)
	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	testIP := "81.2.69.160"
	req.RemoteAddr = testIP
	lineIter := lor.NewIterator(bytes.NewReader(raw), topic)
	efi := NewIterator(lineIter, req)
	for efi.Next() {
		event := efi.At()
		var rec EF
		err := json.Unmarshal(event.Message, &rec)
		assert.Nilf(t, err, "Could not Unmarshal json: %v", err)
		assert.Equal(t, "London", rec.FromCity, "from_city field is not correct")
		assert.Equal(t, "ENG", rec.FromRegion, "from_region field is not correct")
		assert.Equal(t, "GB", rec.FromCountry, "from_country field is not correct")
		assert.Equal(t, 51.5142, rec.FromLatitude, "from_latitude field is not correct")
		assert.Equal(t, -0.0931, rec.FromLongitude, "from_longitude field is not correct")
	}
}

func TestExtraFieldsReader_With(t *testing.T) {
	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	lineIter := lor.NewIterator(bytes.NewReader(raw), topic)

	efi := NewIterator(lineIter, req)
	efi.With(map[string]interface{}{"override": "1"}).
		With(map[string]interface{}{"override": 5}).
		With(map[string]interface{}{"int": 2}).
		With(map[string]interface{}{"string": "str"})

	for efi.Next() {
		event := efi.At()
		t.Logf("%s\n", event.Message)

		var rec map[string]interface{}
		err := json.Unmarshal(event.Message, &rec)
		assert.Equal(t, err, nil, "failed to unmarshal json")

		t.Logf("%#v\n", rec)
		assert.Equal(t, rec["override"], float64(5), "field override is not correct")
		assert.Equal(t, rec["int"], float64(2), "field int is not correct")
		assert.Equal(t, rec["string"], "str", "field string is not correct")
	}
}

func TestExtraFieldsReader_WithFuncUint64(t *testing.T) {
	initSeq := uint64(0)
	fuint64 := func() uint64 {
		initSeq++
		return initSeq
	}

	var raw = []byte(`{"event":"test","payload":{"field": "hi"},"tail":"latest"}
 					  {"event":"test","payload":{"field": "hi"},"tail":"latest"}
					  {"event":"test","payload":{"field": "hi"},"tail":"latest"}`)

	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	lineIter := lor.NewIterator(bytes.NewReader(raw), topic)

	efi := NewIterator(lineIter, req)
	efi.WithFuncUint64("uint64", fuint64)

	i := float64(0)
	for efi.Next() {
		event := efi.At()
		t.Logf("%s\n", event.Message)

		var rec map[string]interface{}
		err := json.Unmarshal(event.Message, &rec)
		assert.Equal(t, err, nil, "failed to unmarshal json")

		t.Logf("%#v\n", rec)
		i++
		assert.Equal(t, rec["uint64"], i, "field uint64 is not correct")
	}
}

func TestExtraFieldsReader_WithFunc(t *testing.T) {
	initSeq := uint64(0)
	f := func() interface{} {
		initSeq++
		return interface{}(initSeq)
	}

	var raw = []byte(`{"event":"test","payload":{"field": "hi"},"tail":"latest"}
 					  {"event":"test","payload":{"field": "hi"},"tail":"latest"}
					  {"event":"test","payload":{"field": "hi"},"tail":"latest"}`)

	topic := "test"
	path := fmt.Sprintf("/ula?report_type=%s", topic)
	req := httptest.NewRequest("POST", path, bytes.NewReader([]byte("")))
	lineIter := lor.NewIterator(bytes.NewReader(raw), topic)

	efi := NewIterator(lineIter, req)
	efi.WithFunc("uint64", f)

	i := float64(0)
	for efi.Next() {
		event := efi.At()
		t.Logf("%s\n", event.Message)

		var rec map[string]interface{}
		err := json.Unmarshal(event.Message, &rec)
		assert.Equal(t, err, nil, "failed to unmarshal json")

		t.Logf("%#v\n", rec)
		i++
		assert.Equal(t, rec["uint64"], i, "field uint64 is not correct")
	}
}

func TestAppendJsonExtraFields(t *testing.T) {
	assert.Equal(t, AppendJsonExtraFields([]byte{}, []byte{}), []byte{}, "Failed to append empty []byte{}")
	assert.Equal(t, AppendJsonExtraFields([]byte{}, []byte(`{}`)), []byte(`{}`), "Failed to append empty json objects to []byte{}")
	assert.Equal(t, AppendJsonExtraFields([]byte(`{}`), []byte(`{}`)), []byte(`{}`), "Failed to append empty json objects")
	assert.Equal(t, AppendJsonExtraFields([]byte(`{}`), []byte{}), []byte(`{}`), "Failed to append empty json objects")
}
