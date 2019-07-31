package gdpr

import (
	"bytes"
	"net"

	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/line_reader"
)

// Deprecated: use EventReader instead
type Reader struct {
	line_reader.I
	reader line_reader.I
	geoSet *geo.Geo
}

// Deprecated: use NewEventReader instead
func NewReader(lr line_reader.I, geoSet *geo.Geo) (gr *Reader) {
	return &Reader{
		reader: lr,
		geoSet: geoSet,
	}
}

// Deprecated: use EventReader.ApplyGDPR() instead
func (r *Reader) ApplyGDPR(msg []byte) []byte {
	for _, ip := range findIPs(msg) {
		if net.ParseIP(string(ip)) != nil {
			if r.geoSet.Get(string(ip)) != "af" {
				msg = bytes.Replace(msg, ip, []byte(ipGDPR(string(ip))), -1)
			}
		}
	}
	return msg
}

// Deprecated: use EventReader.ReadEvent() instead
func (r *Reader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	return r.ApplyGDPR(line), offset, err
}
