package geo

import (
	"bytes"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/fsnotify/fsnotify"
	"github.com/yl2chen/cidranger"
)

type Geo struct {
	GeoFile string
	geoMux  *sync.RWMutex
	watcher *fsnotify.Watcher
	//afGeoFile  string
	rangers      map[string]cidranger.Ranger
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
	g.FromBytes(*data)
}

func (g *Geo) FromBytes(data []byte) *Geo {
	rangers := map[string]cidranger.Ranger{}
	lines := bytes.Split(data, []byte("\n"))
	for _, ipline := range lines {
		trimmedLine := bytes.TrimRight(bytes.TrimSpace(ipline), ";")
		if len(trimmedLine) > 0 {
			recordParts := bytes.Split(trimmedLine, []byte(" "))
			var (
				ranger  cidranger.Ranger
				netAddr []byte
				found   bool
			)
			if len(recordParts) == 2 && len(recordParts[1]) > 0 {
				ranger, found = rangers[string(recordParts[1])]
				if !found {
					ranger = cidranger.NewPCTrieRanger()
				}
				addr := bytes.Split(recordParts[0], []byte("/"))
				if len(addr) == 2 {
					netAddr = recordParts[0]
				} else {
					netAddr = addr[0]
					netAddr = append(netAddr, []byte("/32")...)
				}
				_, ipNet, err := net.ParseCIDR(string(netAddr))
				if err != nil {
					logger.Get().Warnf("Could not parse IP from geo file %s: %s", g.GeoFile, recordParts[0])
					continue
				}
				ranger.Insert(cidranger.NewBasicRangerEntry(*ipNet))
				rangers[string(recordParts[1])] = ranger
			} else {
				logger.Get().Warnf("Malformed geo record in %s: %s", g.GeoFile, ipline)
			}
		}
	}
	g.geoMux.Lock()
	g.rangers = rangers
	g.geoMux.Unlock()
	logger.Get().Warnf("Loaded %d records from %s", g.Len(), g.GeoFile)
	return g
}

// Returns first match label
func (g *Geo) Get(ip string) string {
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return g.defaultValue
	}
	g.geoMux.RLock()
	defer g.geoMux.RUnlock()
	for label, ranger := range g.rangers {
		contains, err := ranger.Contains(ipAddr)
		if err != nil {
			logger.Get().Warnf("IP Ranger lookup err: %v", err)
		}
		if contains {
			return label
		}
	}
	return g.defaultValue
}

func (g *Geo) Match(ip string, label string) bool {
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return false
	}
	g.geoMux.RLock()
	defer g.geoMux.RUnlock()
	ranger, found := g.rangers[label]
	if found {
		contains, err := ranger.Contains(ipAddr)
		if err != nil {
			logger.Get().Warnf("IP Ranger lookup err: %v", err)
		}
		return contains
	}
	return false
}

func (g *Geo) Len() int {
	ret := 0
	for _, ranger := range g.rangers {
		_, wildnet, _ := net.ParseCIDR("0.0.0.0/0")
		rangerEntries, err := ranger.CoveredNetworks(*wildnet)
		if err != nil {
			logger.Get().Errorf("IP Ranger ContainingNetworks err: %v", err)
		}
		ret = ret + len(rangerEntries)
	}
	return ret
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
