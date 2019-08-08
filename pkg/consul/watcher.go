package consul

import (
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/anchorfree/data-go/pkg/logger"
)

const (
	DefaultRetryInterval  = 5 * time.Second
	DefaultMaxBackoffTime = 100 * time.Second
)

type Watcher struct {
	client *api.Client
	config *WatcherConfig
}

type WatcherConfig struct {
	RetryInterval  time.Duration
	MaxBackoffTime time.Duration
}

type Callback func([]byte) error

func NewWatcher(client *api.Client, config *WatcherConfig) *Watcher {
	if config == nil {
		config = &WatcherConfig{
			RetryInterval:  DefaultRetryInterval,
			MaxBackoffTime: DefaultMaxBackoffTime,
		}
	}
	return &Watcher{client: client, config: config}
}

func (w *Watcher) Watch(key string, callback Callback) {
	kv := w.client.KV()

	go func() {
		curIndex := uint64(0)
		retry := w.config.RetryInterval
		for {
			pair, meta, err := kv.Get(key, &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				retry *= 2
				if retry > w.config.MaxBackoffTime {
					retry = w.config.MaxBackoffTime
				}
				logger.Get().Errorf("Can't get data from consul: %v, new retry timeout: %ds", err, retry/time.Second)
			} else {
				retry = w.config.RetryInterval
			}
			if pair == nil || meta == nil {
				time.Sleep(retry)
			} else {
				err := callback(pair.Value)
				if err != nil {
					logger.Get().Errorf("Callback error: %v, with value: %s", err, string(pair.Value))
				}
				curIndex = meta.LastIndex
			}
		}
	}()
}
