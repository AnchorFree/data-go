package file_watcher

import (
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

var DefaultTimeoutAfterLastEvent = 5 * time.Second

type T struct {
	file                  string
	watcher               *fsnotify.Watcher
	cb                    func(file string)
	TimeoutAfterLastEvent time.Duration
}

func New(file string, callback func(file string)) (*T, error) {

	var err error
	w := &T{
		file:                  file,
		cb:                    callback,
		TimeoutAfterLastEvent: DefaultTimeoutAfterLastEvent,
	}
	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	absPath, err := filepath.Abs(file)
	if err != nil {
		return nil, err
	}

	err = w.watcher.Add(filepath.Dir(absPath))
	if err != nil {
		return nil, err
	}

	go func(w *T) {
		var timer *time.Timer
		for {
			select {
			case event := <-w.watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if filepath.Base(event.Name) == w.file || event.Name == w.file {
						if timer != nil {
							timer.Stop()
						}
						timer = time.AfterFunc(w.TimeoutAfterLastEvent, func() { w.cb(event.Name) })
					}
				}
				/*
					case err, ok := <-w.watcher.Errors:
						logger.Get().Debugf("error: %v", err)
				*/
			}
		}
	}(w)

	return w, nil
}
