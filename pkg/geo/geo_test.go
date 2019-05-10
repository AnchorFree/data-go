package geo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGeo(t *testing.T) {
	data := []byte(`
		107.181.165.223 af;
		184.170.253.178 af;
		107.152.103.226 af;
		107.152.104.0/24 af;
		108.62.39.90 af;
		199.115.116.166 af;
		198.8.84.226 af;
		198.8.84.226 af;
		54.182.0.0/16 aws;
		54.182.0.0/16 amz;
		2400::/14 v6;
		::1 lo;
		`)
	g := NewGeo().FromBytes(data)
	assert.Equalf(t, 9, g.Len(), "Amount of IPs does not match")
	assert.Equalf(t, "af", g.Get("184.170.253.178"), "Did not find af record")
	assert.Equalf(t, "af", g.Get("107.152.104.4"), "Did not find af record")
	assert.Truef(t, g.Match("198.8.84.226", "af"), "Did not match af record")
	assert.Truef(t, g.Match("107.152.104.4", "af"), "Did not match af record")
	assert.Truef(t, g.Match("54.182.1.2", "aws"), "Did not match af record")
	assert.Truef(t, g.Match("54.182.1.1", "amz"), "Did not match af record")
	assert.Falsef(t, g.Match("123.123.111.222", "af"), "Should not match record")
	assert.Equalf(t, DefaultValue, g.Get("8.8.8.8"), "Should be default value")
	assert.Truef(t, g.Match("2400::1:2:3", "v6"), "Did not match v6 record")
}

func BenchmarkGeo(b *testing.B) {
	g := NewGeo().FromFile("test-data/aws.conf")
	b.ResetTimer()
	ip := "123.123.123.123"
	for i := 0; i < b.N; i++ {
		g.Match(ip, "aws")
	}
}
