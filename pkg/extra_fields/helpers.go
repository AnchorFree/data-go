package extra_fields

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/geo"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/fsnotify/fsnotify"
	"github.com/oschwald/geoip2-golang"
	"path/filepath"
	"sync"
	"time"
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
			_ = cityDB.Close()
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
			_ = ispDB.Close()
		}
		ispDB = tmpDB
		ispMux.Unlock()
	}
}

func AppendJsonExtraFields(line []byte, extra []byte) []byte {
	if len(line) == 0 {
		return extra
	}
	// we don't need to merge empty extra fileds
	if 0 == len(extra) || bytes.Equal(extra, []byte("{}")) {
		return line
	}
	// could not find closing breaket, broken json
	if bytes.LastIndex(line, []byte("}")) == -1 {
		return line
	}
	return bytes.Join([][]byte{(line)[:bytes.LastIndex(line, []byte("}"))], extra[1:]}, []byte(","))
}
