package gdpr

import (
	"bytes"
	"fmt"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_IPv4Filtering(t *testing.T) {
	ip := string("193.43.210.30")
	want := string("0.0.0.0")
	got := ipGDPR(ip)
	assert.Equal(t, want, got)
}

func Test_IPv6Filtering(t *testing.T) {
	ips := []string{
		"2001:db8:a0b:12f0::1",
		"::12f0:0:1",
		"23:A9::",
		"::1",
		"3281:DF:1::12",
	}
	for _, ip := range ips {
		want := "::"
		got := ipGDPR(ip)
		assert.Equalf(t, want, got, "Incorectly filtered %s got: %v, want: %v", ip, got, want)
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

func Test_FindIPv6(t *testing.T) {
	msg := []byte(`{
			"payload": {
				"ucr_hydra_mode": "sticky",
				"time": 1521800858842,
				"sampled": false,
				"af_token": "3d416b03616ad3a80000000272677331"
				"via": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
			},
			"host": "favoriteshoes.us",
			"from_country_source": "ngx.var.geoip_country_code",
			"cloudfront": 0,
			"ngx_var_remote_addr": "3281:DF:1::12",
			"from_country": "AE",
			"from_ip": "A::B",
			"server_ts": 1521800927956,
			"client_ts": 1521800918976
		}`)
	expected := [][]byte{
		[]byte("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
		[]byte("3281:DF:1::12"),
		[]byte("A::B"),
	}
	result := findIPs(msg)
	assert.Equal(t, expected, result)
	stringMsg := `{"event": "this is a test address %s to be found"}`
	ips := []string{
		"1:2:3:4:5:6:7:8",
		"1:2:3:4:5:6:7::",
		"1:2:3:4:5:6::8",
		"1:2:3:4:5:6::8",
		"1:2:3:4:5::7:8",
		"1:2:3:4:5::8",
		"1:2:3:4::6:7:8",
		"1:2:3:4::8",
		"1:2:3::5:6:7:8",
		"1:2:3::8",
		"1:2::4:5:6:7:8",
		"1:2::8",
		"1::",
		"1::3:4:5:6:7:8",
		"1::3:4:5:6:7:8",
		"1::4:5:6:7:8",
		"1::5:6:7:8",
		"1::6:7:8",
		"1::7:8",
		"1::8",
		"1::8",
		"::2:3:4:5:6:7:8",
		"::2:3:4:5:6:7:8",
		"::8",
		"FE80:0000:0000:0000:0202:B3FF:FE1E:8329",
	}

	for _, ip := range ips {
		found := findIPs([]byte(fmt.Sprintf(stringMsg, ip)))
		if len(found) > 0 {
			assert.Equal(t, ip, string(found[0]))
		} else {
			assert.FailNowf(t, "Test failed", "could not find ip: %s", ip)
		}
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
	geoSet.FromBytes([]byte("74.115.4.69 af;"))
	reader := &Reader{
		geoSet: geoSet,
	}
	result := reader.ApplyGDPR(msg)

	//result := maskIPs(msg, findIPs(msg))
	assert.Equal(t, bytes.Compare(expected, result), 0, "Did not match expected %v, got: %v", string(expected), string(result))
}

var benchMsg = []byte(`{
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
		"invalid_addr": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		"from_country": "AE",
		"other_ip": "113.0.84.0/35",
		"server_ts": 1521800927956,
		"client_ts": 1521800918976
	}`)

func BenchmarkIPv4Regex(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ipv4Regex.FindAll(benchMsg, -1)
	}
}

func BenchmarkIPv6Regex(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ipv6Regex.FindAll(benchMsg, -1)
	}
}
