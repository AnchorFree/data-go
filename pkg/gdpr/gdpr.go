package gdpr

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"net"
	"regexp"
)

var ipv4Regex = regexp.MustCompile(`([0-9]{1,3}\.){3}[0-9]{1,3}`)

//no support for IPv4-IPv6 embedded notation
var ipv6Regex = regexp.MustCompile(`(?i)(?:[^0-9a-f:])(([0-9a-f]{1,4}:){1,7}:|:(:[0-9a-f]{1,4}){1,7}|([0-9a-f]{1,4}:){1,7}[0-9a-f]{0,4}(:[0-9a-f]{1,4}){1,7})(?:[^0-9a-f:])`)

type Reader struct {
	line_reader.I
	reader line_reader.I
	geoSet *geo.Geo
}

func maskIP(ip net.IP) string {
	var ipMask net.IPMask
	if ip.DefaultMask() != nil {
		ipMask = net.CIDRMask(0, 32)
	} else {
		ipMask = net.CIDRMask(0, 128)
	}
	return ip.Mask(ipMask).String()
}

func ipGDPR(address string) string {
	ip := net.ParseIP(address)
	return maskIP(ip)
}

func findIPs(msg []byte) [][]byte {
	ret := [][]byte{}
	for _, ip := range ipv4Regex.FindAll(msg, -1) {
		if net.ParseIP(string(ip)) != nil {
			ret = append(ret, ip)
		}
	}
	for _, matches := range ipv6Regex.FindAllSubmatch(msg, -1) {
		if len(matches) > 2 && net.ParseIP(string(matches[1])) != nil {
			ret = append(ret, matches[1])
		}
	}
	return ret
}

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

func NewReader(lr line_reader.I, geoSet *geo.Geo) (gr *Reader) {
	return &Reader{
		reader: lr,
		geoSet: geoSet,
	}
}

func (r *Reader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	return r.ApplyGDPR(line), offset, err
}
