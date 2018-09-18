package gdpr

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"net"
	"regexp"
)

var ipv4Regex = regexp.MustCompile(`([0-9]{1,3}\.){3}[0-9 ]{1,3}`)

type GdprReader struct {
	line_reader.I
	reader line_reader.I
}

func maskIP(ip net.IP) string {
	var ipMask net.IPMask
	if ip.DefaultMask() != nil {
		ipMask = net.CIDRMask(24, 32)
	} else {
		ipMask = net.CIDRMask(48, 128)
	}
	return ip.Mask(ipMask).String()
}

func ipGDPR(address *string) string {
	ip := net.ParseIP(*address)
	return maskIP(ip)
}

func findIPs(msg *[]byte) [][]byte {
	return ipv4Regex.FindAll(*msg, -1)
}

func maskIPs(msg []byte, ips [][]byte) []byte {
	var replacer [][]byte
	for _, v := range ips {
		replacer = append(replacer, v)
		sv := string(v)
		replacer = append(replacer, []byte(ipGDPR(&sv)))
	}
	for i := 0; i < len(replacer); i = i + 2 {
		msg = bytes.Replace(msg, replacer[i], replacer[i+1], -1)
	}
	return msg
}

func applyGDPR(msg *[]byte) []byte {
	return maskIPs(*msg, findIPs(msg))
}

func NewGdprReader(lr line_reader.I) (gr *GdprReader) {
	return &GdprReader{
		reader: lr,
	}
}

func (r *GdprReader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()
	return applyGDPR(&line), offset, err
}
