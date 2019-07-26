package extra_fields

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	geoip2 "github.com/oschwald/geoip2-golang"

	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
)

var (
	cityDB     *geoip2.Reader
	ispDB      *geoip2.Reader
	cityDBFile string
	ispDBFile  string
	cityMux    *sync.RWMutex
	ispMux     *sync.RWMutex
	geoSet     *geo.Geo
)

func loadCityDB(panicOnFail bool) {
	logger.Get().Infof("Loading geoip2 City database from: %s", cityDBFile)
	tmpDB, err := geoip2.Open(cityDBFile)
	if err != nil {
		logger.Get().Errorf("Error loading City database: %v", err)
		if panicOnFail {
			logger.Get().Fatalf("Configured to fail with err: %v", err)
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

func Init(geoip2CityPath string, geoip2IspPath string, gSet *geo.Geo) {
	var err error
	cityMux = &sync.RWMutex{}
	ispMux = &sync.RWMutex{}
	cityDBFile = geoip2CityPath
	ispDBFile = geoip2IspPath
	geoSet = gSet

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
					if event.Name == cityDBFile || event.Name == ispDBFile {
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

	for _, file := range []string{geoip2CityPath, geoip2IspPath} {
		logger.Get().Debugf("Watching %s file", file)
		err = watcher.Add(filepath.Dir(file))
		if err != nil {
			logger.Get().Fatal(err)
		}
	}
}

// Deprecated: use ExtraFieldsEventReader instead
type ExtraFieldsReader struct {
	line_reader.I
	reader         line_reader.I
	request        *http.Request
	extraFields    []byte
	extraFieldFunc map[string]func() interface{}
}

// Deprecated: use NewExtraFieldsEventReader instead
func NewExtraFieldsReader(reader line_reader.I, req *http.Request) *ExtraFieldsReader {
	return &ExtraFieldsReader{
		reader:         reader,
		request:        req,
		extraFields:    []byte(""),
		extraFieldFunc: make(map[string]func() interface{}),
	}
}

// Deprecated: use EventReader.With() instead
func (r *ExtraFieldsReader) With(extra map[string]interface{}) *ExtraFieldsReader {
	extraJson, err := json.Marshal(extra)
	if err != nil {
		logger.Get().Errorf("Could not marshal extra fields: %v", extra)
	} else {
		r.extraFields = AppendJsonExtraFields(r.extraFields, extraJson)
	}
	return r
}

// Deprecated: use EventReader.WithFuncUint64() instead
func (r *ExtraFieldsReader) WithFuncUint64(key string, f func() uint64) *ExtraFieldsReader {
	r.WithFunc(key, func() interface{} {
		return interface{}(f())
	})
	return r
}

// Deprecated: use EventReader.WithFunc() instead
func (r *ExtraFieldsReader) WithFunc(key string, f func() interface{}) *ExtraFieldsReader {
	r.extraFieldFunc[key] = f
	return r
}

func (r *ExtraFieldsReader) renderExtraFieldsFunc() []byte {
	extraFields := make(map[string]interface{})
	for key, f := range r.extraFieldFunc {
		extraFields[key] = f()
	}
	renderedExtraFields, err := json.Marshal(extraFields)
	if err != nil {
		logger.Get().Errorf("Could not marshal function extra fields: %v", extraFields)
	}
	return renderedExtraFields
}

// Deprecated: use EventReader.ReadEvent() instead
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
	if len(r.extraFieldFunc) > 0 {
		line = AppendJsonExtraFields(line, r.renderExtraFieldsFunc())
	}

	return line, offset, err
}
