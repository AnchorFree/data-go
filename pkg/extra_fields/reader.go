package extra_fields

import (
	"bytes"
	"encoding/json"
	"github.com/anchorfree/gpr-edge/pkg/confreader"
	"github.com/anchorfree/ula-edge/pkg/line_reader"
	"github.com/anchorfree/ula-edge/pkg/logger"
	geoip2 "github.com/oschwald/geoip2-golang"
	"net/http"
	"time"
)

var (
	cityDB    *geoip2.Reader
	ispDB     *geoip2.Reader
	afGeoFile string
)

func reloadAFGeoIPData() {
	confreader.ReadGeo(afGeoFile)
}

func Init(geoip2CityPath string, geoip2IspPath string, afGeoPath string) {
	var err error
	afGeoFile = afGeoPath
	cityDB, err = geoip2.Open(geoip2CityPath)
	if err != nil {
		logger.Get().Fatal(err)
	}
	ispDB, err = geoip2.Open(geoip2IspPath)
	if err != nil {
		logger.Get().Fatal(err)
	}
	// update AF geo data (which come from consul) every 60 seconds
	go func() {
		for {
			logger.Get().Info("reloading AF geoip data")
			reloadAFGeoIPData()
			time.Sleep(60 * time.Second)
		}
	}()
}

type ExtraFieldsReader struct {
	line_reader.I
	reader      line_reader.I
	headers     http.Header
	extraFields []byte
}

func NewExtraFieldsReader(r line_reader.I, h http.Header) *ExtraFieldsReader {
	return &ExtraFieldsReader{
		reader:      r,
		headers:     h,
		extraFields: []byte(""),
	}
}

func AppendJsonExtraFields(line []byte, extra []byte) []byte {
	if len(line) == 0 {
		return extra
	}
	// we don't need to merge empty extra fileds
	if bytes.Equal(extra, []byte("{}")) {
		return line
	}
	// could not find closing breaket, broken json
	if bytes.LastIndex(line, []byte("}")) == -1 {
		return line
	}
	return bytes.Join([][]byte{(line)[:bytes.LastIndex(line, []byte("}"))], extra[1:]}, []byte(","))
}

func (r *ExtraFieldsReader) With(extra map[string]interface{}) *ExtraFieldsReader {
	extraJson, err := json.Marshal(extra)
	if err != nil {
		logger.Get().Errorf("Could not marshal extra fields: %v", extra)
	} else {
		r.extraFields = extraJson
	}
	return r
}

//func populateExtraTags(r *http.Request, raw *[]byte, cityDB *geoip2.Reader, ispDB *geoip2.Reader, geo *confreader.AfGeo) []byte {

func (r *ExtraFieldsReader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()

	fields := new(ExtraFields)
	fields.GeoOrigin(r.headers, cityDB, ispDB)
	fields.CloudFront = IsCloudfront(r.headers)
	fields.Host = GetNginxHostname(r.headers)

	extra, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		return line, offset, err
	}
	line = AppendJsonExtraFields(line, extra)
	if len(r.extraFields) > 0 {
		line = AppendJsonExtraFields(line, r.extraFields)
	}
	return line, offset, err
}
