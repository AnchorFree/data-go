package extra_fields

import (
	"bytes"
	"encoding/json"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/gpr-edge/pkg/confreader"
	geoip2 "github.com/oschwald/geoip2-golang"
	//"github.com/rjeczalik/notify"
	"github.com/fsnotify/fsnotify"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

var (
	cityDB     *geoip2.Reader
	ispDB      *geoip2.Reader
	cityDBFile string
	ispDBFile  string
	afGeoFile  string
	cityMux    *sync.RWMutex
	ispMux     *sync.RWMutex
	afGeoMux   *sync.RWMutex
)

func loadAFGeoIPData() {
	afGeoMux.Lock()
	logger.Get().Infof("Loading AF geoip data from: %s", afGeoFile)
	confreader.ReadGeo(afGeoFile)
	afGeoMux.Unlock()
}

func loadCityDB(panicOnFail bool) {
	logger.Get().Infof("Loading geoip2 City database from: %s", cityDBFile)
	tmpDB, err := geoip2.Open(cityDBFile)
	if err != nil {
		logger.Get().Errorf("Error loading City database: %v", err)
		if panicOnFail {
			logger.Get().Fatal("Configured to faild with err: %v", err)
		}
	} else {
		cityMux.Lock()
		if cityDB != nil {
			logger.Get().Debug("Closing old cityDB")
			cityDB.Close()
		}
		cityDB = tmpDB
		cityMux.Unlock()
	}
}

func loadIspDB(panicOnFail bool) {
	logger.Get().Infof("Loading geoip2 ISP database from: %s", ispDBFile)
	tmpDB, err := geoip2.Open(ispDBFile)
	if err != nil {
		logger.Get().Errorf("Error loading ISP database: %v", err)
		if panicOnFail {
			logger.Get().Fatal("Configured to faild with err: %v", err)
		}
	} else {
		ispMux.Lock()
		if ispDB != nil {
			logger.Get().Debug("Closing old ispDB")
			ispDB.Close()
		}
		ispDB = tmpDB
		ispMux.Unlock()
	}
}

func Init(geoip2CityPath string, geoip2IspPath string, afGeoPath string) {
	var err error
	cityMux = &sync.RWMutex{}
	ispMux = &sync.RWMutex{}
	afGeoMux = &sync.RWMutex{}
	afGeoFile = afGeoPath
	cityDBFile = geoip2CityPath
	ispDBFile = geoip2IspPath

	loadAFGeoIPData()
	loadCityDB(true)
	loadIspDB(true)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Get().Fatal(err)
	}
	go func() {
		timeoutAfterLastEvent := 5 * time.Second
		defer logger.Get().Error("File watcher shutdown")
		timers := map[string]*time.Timer{}
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					logger.Get().Error("File watcher not OK")
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if event.Name == cityDBFile || event.Name == ispDBFile || event.Name == afGeoFile {
						logger.Get().Debugf("modified file (%v): %s", event.Op, event.Name)
						if timer, found := timers[event.Name]; !found || !timer.Reset(timeoutAfterLastEvent) {
							if found {
								timers[event.Name].Stop()
							}
							switch event.Name {
							case cityDBFile:
								timers[event.Name] = time.AfterFunc(timeoutAfterLastEvent, func() { loadCityDB(false) })
							case ispDBFile:
								timers[event.Name] = time.AfterFunc(timeoutAfterLastEvent, func() { loadIspDB(false) })
							case afGeoFile:
								timers[event.Name] = time.AfterFunc(timeoutAfterLastEvent, func() { loadAFGeoIPData() })
							default:
								logger.Get().Errorf("Unknown modified file to process: %s", event.Name)
							}
						}
					}
				}
			case err, ok := <-watcher.Errors:
				logger.Get().Debugf("error: %v", err)
				if !ok {
					logger.Get().Debug("watcher not OK")
					return
				}
			}
		}
	}()

	for _, file := range []string{geoip2CityPath, geoip2IspPath, afGeoPath} {
		logger.Get().Debugf("Watching %s file", file)
		err = watcher.Add(filepath.Dir(file))
		if err != nil {
			logger.Get().Fatal(err)
		}
	}
}

type ExtraFieldsReader struct {
	line_reader.I
	reader      line_reader.I
	request     *http.Request
	extraFields []byte
}

func NewExtraFieldsReader(reader line_reader.I, req *http.Request) *ExtraFieldsReader {
	return &ExtraFieldsReader{
		reader:      reader,
		request:     req,
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

func (r *ExtraFieldsReader) ReadLine() (line []byte, offset uint64, err error) {
	line, offset, err = r.reader.ReadLine()

	fields := new(ExtraFields)
	fields.GeoOrigin(r.request)
	fields.CloudFront = IsCloudfront(r.request)
	fields.Host = GetNginxHostname(r.request)

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
