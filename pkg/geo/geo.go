package geo

import (
	"bytes"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Geo struct {
	GeoFile string
	geoMux  *sync.RWMutex
	watcher *fsnotify.Watcher
	//afGeoFile  string
	records      map[string]string
	defaultValue string
}

var DefaultValue = "-"

//var IPs MyIPs

func NewGeo() *Geo {
	var g Geo
	g.defaultValue = DefaultValue
	g.geoMux = &sync.RWMutex{}
	return &g
}

func (g *Geo) FromFile(file string) *Geo {
	var err error
	g.GeoFile = file
	g.loadFileData()
	g.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		logger.Get().Fatal(err)
	}
	go func(geo *Geo) {
		timeoutAfterLastEvent := 5 * time.Second
		defer logger.Get().Error("File watcher shutdown")
		timers := map[string]*time.Timer{}
		for {
			select {
			case event, ok := <-geo.watcher.Events:
				if !ok {
					logger.Get().Error("File watcher not OK")
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if event.Name == geo.GeoFile {
						logger.Get().Debugf("modified file (%v): %s", event.Op, event.Name)
						if timer, found := timers[event.Name]; !found || !timer.Reset(timeoutAfterLastEvent) {
							if found {
								timers[event.Name].Stop()
							}
							switch event.Name {
							case geo.GeoFile:
								timers[event.Name] = time.AfterFunc(timeoutAfterLastEvent, func() { geo.loadFileData() })
							default:
								logger.Get().Errorf("Unknown modified file to process: %s", event.Name)
							}
						}
					}
				}
			case err, ok := <-geo.watcher.Errors:
				logger.Get().Debugf("error: %v", err)
				if !ok {
					logger.Get().Debug("watcher not OK")
					return
				}
			}
		}
	}(g)

	for _, file := range []string{g.GeoFile} {
		logger.Get().Debugf("Watching %s file", file)
		err = g.watcher.Add(filepath.Dir(file))
		if err != nil {
			logger.Get().Fatal(err)
		}
	}
	return g
}

func (g *Geo) loadFileData() {
	logger.Get().Infof("Loading AF geoip data from: %s", g.GeoFile)
	data, err := ReadFile(g.GeoFile)
	if err != nil {
		logger.Get().Fatalf("Error loading geo data from %s: %v", g.GeoFile, err)
		return
	}
	g.geoMux.Lock()
	g.LoadFromBytes(*data)
	g.geoMux.Unlock()
}

func (g *Geo) LoadFromBytes(data []byte) {
	ipList := map[string]string{}
	lines := bytes.Split(data, []byte("\n"))
	for _, ipline := range lines {
		trimmedLine := bytes.TrimRight(bytes.TrimSpace(ipline), ";")
		if len(trimmedLine) > 0 {
			recordParts := bytes.Split(trimmedLine, []byte(" "))
			if len(recordParts) == 2 {
				ip := net.ParseIP(string(recordParts[0]))
				if ip != nil {
					ipList[string(recordParts[0])] = string(recordParts[1])
				} else {
					logger.Get().Warnf("Could not parse IP from geo file %s: %s", g.GeoFile, recordParts[0])
				}
			} else {
				logger.Get().Warnf("Malformed geo record in %s: %s", g.GeoFile, ipline)
			}
		}
	}
	logger.Get().Warnf("Loaded %d records from %s", len(ipList), g.GeoFile)
	g.records = ipList
}

func (g *Geo) Get(ip string) string {
	g.geoMux.RLock()
	val, found := g.records[ip]
	g.geoMux.RUnlock()
	if found {
		return val
	}
	return g.defaultValue
}

func (g *Geo) Len() int {
	return len(g.records)
}

func ReadFile(filename string) (*[]byte, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(filename)

	if err != nil {
		return nil, err
	}

	return &data, nil

}
