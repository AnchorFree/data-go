package gdpr

import (
	"net"
	"regexp"
)

var ipv4Regex = regexp.MustCompile(`([0-9]{1,3}\.){3}[0-9]{1,3}`)

//no support for IPv4-IPv6 embedded notation
var ipv6Regex = regexp.MustCompile(`(?i)(?:[^0-9a-f:])(([0-9a-f]{1,4}:){1,7}:|:(:[0-9a-f]{1,4}){1,7}|([0-9a-f]{1,4}:){1,7}[0-9a-f]{0,4}(:[0-9a-f]{1,4}){1,7})(?:[^0-9a-f:])`)

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
