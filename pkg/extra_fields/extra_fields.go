package extra_fields

import (
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/golang/gddo/httputil/header"
	geoip2 "github.com/oschwald/geoip2-golang"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type ExtraFields struct {
	Country       string `json:"from_country,omitempty"`
	CountrySource string `json:"from_country_source,omitempty"`
	City          string `json:"from_city,omitempty"`
	CitySource    string `json:"from_city_source,omitempty"`
	CloudFront    int    `json:"cloudfront"`
	Host          string `json:"host,omitempty"`
	FromASN       string `json:"from_asn,omitempty"`
	FromASDesc    string `json:"from_as_desc,omitempty"`
	FromISP       string `json:"from_isp,omitempty"`
	FromOrgName   string `json:"from_org_name,omitempty"`
}

func (f *ExtraFields) GeoOrigin(req *http.Request) {
	ip := GetIPAdress(req)

	f.fromISP(req, ip)
	if geoSet.Get(ip.String()) == "af" && IsCloudfront(req) == 1 {
		return
	}

	f.countryName(req, ip)
	f.cityName(req, ip)
}

func GetNginxHostname(req *http.Request) string {
	return req.Header.Get(http.CanonicalHeaderKey("host"))
}

func GetIPAdress(req *http.Request) net.IP {
	var realIP net.IP
	var remoteAddr string
	if strings.ContainsRune(
		req.RemoteAddr,
		':',
	) {
		remoteAddr, _, _ = net.SplitHostPort(req.RemoteAddr)
	} else {
		remoteAddr = req.RemoteAddr
	}
	realIP = net.ParseIP(remoteAddr)

	for _, h := range []string{"X-Forwarded-For", "X-Real-Ip"} {
		if len(req.Header.Get(http.CanonicalHeaderKey(h))) > 0 {
			addresses := header.ParseList(req.Header, http.CanonicalHeaderKey(h))
			for i := 0; i < len(addresses); i++ {
				ip := strings.TrimSpace(addresses[i])
				// header can contain spaces too, strip those out.
				realIP = net.ParseIP(ip)
				if !realIP.IsGlobalUnicast() || isPrivateSubnet(realIP) {
					// bad address, go to next
					continue
				}
				return realIP
			}
		}
	}
	return realIP
}

func (f *ExtraFields) countryName(req *http.Request, ip net.IP) error {
	afCountry := GetMatchingHeader(req.Header, "x_af_c_country")
	if afCountry == "" && ip != nil {
		// header key doesn't exist we should use GeoIP
		cityMux.RLock()
		record, err := cityDB.Country(ip)
		cityMux.RUnlock()
		if err != nil {
			logger.Get().Warnf("Error: %v, for ip: %s", err, ip.String())
			return err
		}
		f.Country = record.Country.IsoCode
		if f.Country != "" {
			f.CountrySource = "ngx.var.geoip_country_code"
		}
	} else {
		f.Country = afCountry
		f.CountrySource = "x_af_c_country"
	}
	return nil

}

func (f *ExtraFields) cityName(req *http.Request, ip net.IP) error {
	afCity := GetMatchingHeader(req.Header, "x_af_c_city")
	if afCity == "" && ip != nil {
		cityMux.RLock()
		record, err := cityDB.City(ip)
		cityMux.RUnlock()
		if err != nil {
			logger.Get().Warnf("Error: %v, for ip: %s", err, ip.String())
			return err
		}
		f.City = record.City.Names["en"]
		if f.City != "" {
			f.CitySource = "ngx.var.geoip_country_code"
		}
	} else {
		f.City = afCity
		f.CitySource = "x_af_c_city"
	}
	return nil
}

func (f *ExtraFields) fromISP(req *http.Request, ip net.IP) error {
	var isp *geoip2.ISP
	var err error
	if ip != nil {
		ispMux.RLock()
		isp, err = ispDB.ISP(ip)
		ispMux.RUnlock()
		if err != nil {
			logger.Get().Warnf("Error: %v, for ip: %s", err, ip.String())
		}
	}

	fromASN := GetMatchingHeader(req.Header, "x_af_asn")
	if fromASN == "" && isp != nil {
		f.FromASN = strconv.FormatUint(uint64(isp.AutonomousSystemNumber), 10)
	} else {
		f.FromASN = fromASN
	}

	fromASNDesc := GetMatchingHeader(req.Header, "x_af_asdescription")
	if fromASNDesc == "" && isp != nil {
		f.FromASDesc = isp.AutonomousSystemOrganization
	} else {
		f.FromASDesc = fromASNDesc
	}

	fromISP := GetMatchingHeader(req.Header, "X_AF_ISPNAME")
	if fromISP == "" && isp != nil {
		f.FromISP = isp.ISP
	} else {
		f.FromISP = fromISP
	}

	fromOrgName := GetMatchingHeader(req.Header, "X_AF_ORGNAME")
	if fromOrgName == "" && isp != nil {
		f.FromOrgName = isp.Organization
	} else {
		f.FromOrgName = fromOrgName
	}
	return err
}

func IsCloudfront(req *http.Request) int {
	amzId := req.Header.Get(http.CanonicalHeaderKey("X-Amz-Cf-Id"))
	if len(amzId) > 0 {
		return 1
	}
	return 0
}

var private24BitBlock net.IPNet = net.IPNet{IP: net.IPv4(10, 0, 0, 0), Mask: net.IPv4Mask(255, 0, 0, 0)}
var private20BitBlock net.IPNet = net.IPNet{IP: net.IPv4(172, 16, 0, 0), Mask: net.IPv4Mask(255, 240, 0, 0)}
var private16BitBlock net.IPNet = net.IPNet{IP: net.IPv4(192, 168, 0, 0), Mask: net.IPv4Mask(255, 255, 0, 0)}

// isPrivateSubnet - check to see if this ip is in a private subnet
func isPrivateSubnet(IP net.IP) bool {
	return private24BitBlock.Contains(IP) || private20BitBlock.Contains(IP) || private16BitBlock.Contains(IP)
}

func GetMatchingHeader(headers http.Header, key string) string {
	res := headers.Get(key)
	if res == "" {
		res = headers.Get(strings.Replace(key, "-", "_", -1))
	}
	if res == "" {
		res = headers.Get(strings.Replace(key, "_", "-", -1))
	}
	return res
}
