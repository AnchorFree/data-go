package gdpr

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_IPv4Filtering(t *testing.T) {
	ip := string("193.43.210.30")
	want := string("0.0.0.0")
	got := ipGDPR(ip)
	if want != got {
		t.Fatalf("Want: %s, got %s", want, got)
	}
}

func Test_IPv6Filtering(t *testing.T) {
	ip := string("2001:db8:a0b:12f0::1")
	want := string("::")
	got := ipGDPR(ip)
	if want != got {
		t.Fatalf("Want: %s, got %s", want, got)
	}
}

func Test_FindIPv4(t *testing.T) {
	msg := []byte(`{
			"payload": {
				"ucr_hydra_mode": "sticky",
				"time": 1521800858842,
				"sampled": false,
				"af_token": "3d416b03616ad3a80000000272677331"
			    "via": "74.115.4.69",
			},
			"host": "favoriteshoes.us",
			"from_country_source": "ngx.var.geoip_country_code",
			"cloudfront": 0,
			"ngx_var_remote_addr": "113.203.84.5",
			"from_country": "AE",
			"from_ip": "113.203.84.0",
			"server_ts": 1521800927956,
			"client_ts": 1521800918976
		}`)
	expected := bytes.Split([]byte("74.115.4.69,113.203.84.5,113.203.84.0"), []byte(","))
	result := findIPs(msg)
	if result == nil {
		t.Fatalf("Did not find any IPs expected")
	}
	for i := range expected {
		assert.Equal(t, bytes.Compare(expected[i], result[i]), 0, "Did not match expected %v, got: %v", expected[i], result[i])
	}
}

func Test_MaskIPs(t *testing.T) {
	msg := []byte(`{
			"payload": {
				"ucr_hydra_mode": "sticky",
				"time": 1521800858842,
				"sampled": false,
				"af_token": "3d416b03616ad3a80000000272677331"
			    "via": "74.115.4.69",
			},
			"host": "favoriteshoes.us",
			"from_country_source": "ngx.var.geoip_country_code",
			"cloudfront": 0,
			"ngx_var_remote_addr": "113.203.84.5",
			"invalid_addr": "113.203.12.333",
			"from_country": "AE",
			"from_ip": "113.203.84.0",
			"server_ts": 1521800927956,
			"client_ts": 1521800918976
		}`)
	expected := []byte(`{
			"payload": {
				"ucr_hydra_mode": "sticky",
				"time": 1521800858842,
				"sampled": false,
				"af_token": "3d416b03616ad3a80000000272677331"
			    "via": "74.115.4.69",
			},
			"host": "favoriteshoes.us",
			"from_country_source": "ngx.var.geoip_country_code",
			"cloudfront": 0,
			"ngx_var_remote_addr": "0.0.0.0",
			"invalid_addr": "113.203.12.333",
			"from_country": "AE",
			"from_ip": "0.0.0.0",
			"server_ts": 1521800927956,
			"client_ts": 1521800918976
		}`)
	geoSet := geo.NewGeo()
	geoSet.LoadFromBytes([]byte("74.115.4.69 af;"))
	reader := &Reader{
		geoSet: geoSet,
	}
	result := reader.ApplyGDPR(msg)

	//result := maskIPs(msg, findIPs(msg))
	assert.Equal(t, bytes.Compare(expected, result), 0, "Did not match expected %v, got: %v", string(expected), string(result))
}
