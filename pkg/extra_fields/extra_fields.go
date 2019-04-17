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
	Country       string  `json:"from_country,omitempty"`
	CountrySource string  `json:"from_country_source,omitempty"`
	City          string  `json:"from_city,omitempty"`
	CitySource    string  `json:"from_city_source,omitempty"`
	Longitude     float64 `json:"from_longitude,omitempty"`
	Latitude      float64 `json:"from_latitude,omitempty"`
	CloudFront    int     `json:"cloudfront"`
	Host          string  `json:"host,omitempty"`
	FromASN       string  `json:"from_asn,omitempty"`
	FromASDesc    string  `json:"from_as_desc,omitempty"`
	FromISP       string  `json:"from_isp,omitempty"`
	FromOrgName   string  `json:"from_org_name,omitempty"`
	Region        string  `json:"from_region,omitempty"`

	cityDBRec *geoip2.City
}

func (f *ExtraFields) GeoOrigin(req *http.Request) {
	ip := GetIPAdress(req)

	f.fromISP(req, ip)
	if geoSet.Get(ip.String()) == "af" && IsCloudfront(req) == 1 {
		return
	}

	f.countryName(req, ip)
	f.cityName(req, ip)
	f.coordinates(req, ip)
}

//cityDB.City() wrapper function, to cache the result.
//WARNING: It's suggested that IP argument will be the same within same ExtraFields set!
func (f *ExtraFields) GetCityDBRecord(ip net.IP) (*geoip2.City, error) {
	var (
		err error
		rec *geoip2.City
	)
	if f.cityDBRec == nil {
		cityMux.RLock()
		rec, err = cityDB.City(ip)
		cityMux.RUnlock()
		if err != nil {
			return nil, err
		}
		f.cityDBRec = rec
	}
	return f.cityDBRec, err
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

	for _, h := range []string{"X-Real-Ip", "X-Forwarded-For"} {
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

func (f *ExtraFields) coordinates(req *http.Request, ip net.IP) error {

	afLongitude := GetMatchingHeader(req.Header, "x_af_longitude")
	if afLongitude == "" {
		geoRec, err := f.GetCityDBRecord(ip)
		if err != nil {
			logger.Get().Warnf("Could not get geoip cityDB record: %v", err)
			return err
		}
		f.Longitude = geoRec.Location.Longitude
	} else {
		val, err := strconv.ParseFloat(afLongitude, 64)
		if err == nil {
			f.Longitude = val
		} else {
			logger.Get().Warnf("Could not parse longitude value: %s", afLongitude)
		}
	}

	afLatitude := GetMatchingHeader(req.Header, "x_af_latitude")
	if afLatitude == "" {
		geoRec, err := f.GetCityDBRecord(ip)
		if err != nil {
			logger.Get().Warnf("Could not get geoip cityDB record: %v", err)
			return err
		}
		f.Latitude = geoRec.Location.Latitude
	} else {
		val, err := strconv.ParseFloat(afLatitude, 64)
		if err == nil {
			f.Latitude = val
		} else {
			logger.Get().Warnf("Could not parse latitude value: %s", afLatitude)
		}
	}
	return nil
}

func (f *ExtraFields) countryName(req *http.Request, ip net.IP) error {
	afCountry := GetMatchingHeader(req.Header, "x_af_c_country")
	if afCountry == "" && ip != nil {
		// header key doesn't exist we should use GeoIP
		record, err := f.GetCityDBRecord(ip)
		if err != nil {
			logger.Get().Warnf("Error: %v, for ip: %s", err, ip.String())
			return err
		}
		f.Country = record.Country.IsoCode
		if f.Country != "" {
			f.CountrySource = "geoip"
		}
	} else {
		f.Country = afCountry
		f.CountrySource = "x_af_c_country"
	}
	return nil

}

func (f *ExtraFields) cityName(req *http.Request, ip net.IP) error {
	afCity := GetMatchingHeader(req.Header, "x_af_c_city")
	afRegion := GetMatchingHeader(req.Header, "x_af_c_region")
	if afCity == "" && ip != nil {
		record, err := f.GetCityDBRecord(ip)
		if err != nil {
			logger.Get().Warnf("Error: %v, for ip: %s", err, ip.String())
			return err
		}
		f.City = record.City.Names["en"]
		if afRegion == "" && len(record.Subdivisions) > 0 {
			afRegion = record.Subdivisions[0].IsoCode
		}
		if f.City != "" {
			f.CitySource = "geoip"
		}
	} else {
		f.City = afCity
		f.CitySource = "x_af_c_city"
	}
	f.Region = afRegion
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
