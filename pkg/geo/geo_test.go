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
		108.62.39.90 af;
		199.115.116.166 af;
		198.8.84.226 af;
		`)
	g := NewGeo()
	g.LoadFromBytes(data)
	assert.Equalf(t, 6, g.Len(), "Amount of IPs does not match")
	assert.Equalf(t, "af", g.Get("184.170.253.178"), "Did not find af record")
	assert.Equalf(t, DefaultValue, g.Get("8.8.8.8"), "Should be default value")
}
