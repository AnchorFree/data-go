package geo

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		`)
	g := NewGeo()
	g.LoadFromBytes(data)
	assert.Equalf(t, 7, g.Len(), "Amount of IPs does not match")
	assert.Equalf(t, "af", g.Get("184.170.253.178"), "Did not find af record")
	assert.Equalf(t, "af", g.Get("107.152.104.4"), "Did not find af record")
	assert.Equalf(t, DefaultValue, g.Get("8.8.8.8"), "Should be default value")
}
