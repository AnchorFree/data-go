package kafka_proxy

import (
	"errors"
	"time"
	"io"

	"github.com/cenk/backoff"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	circuit "github.com/rubyist/circuitbreaker"

	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/clients/grpc_client"
	"github.com/anchorfree/data-go/pkg/clients/http_client"
	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
)

//import context "golang.org/x/net/context"

type KafkaProxy struct {
	//client     *circuit.HTTPClient
	client         client.I
	Config         Props
	prom           *prometheus.Registry
	breaker        *circuit.Breaker
	Topics         []string
	MetadataTopics []string
}

type Props struct {
	Url                    string
	CircuitBreakerMaxFails int64         `yaml:"circuitbreaker_max_fails"`
	TopicRefreshInterval   time.Duration `yaml:"topic_refresh_interval"`
	Topics                 []TopicProps
	GrpcClientConfig       grpc_client.Props `yaml:"grpc"`
	HttpClientConfig       http_client.Props `yaml:"http"`
}

type TopicProps struct {
	Name     string
	Format   string
	Validate bool
}

var DefaultConfig Props = Props{
	Url:                    "grpc://localhost:19094",
	CircuitBreakerMaxFails: 3,
	TopicRefreshInterval:   60 * time.Second,
	//GrpcClientConfig:       grpc_client.Props{},
	//HttpClientConfig:       http_client.Props{},
	Topics: []TopicProps{
		TopicProps{
			Name:     "test",
			Format:   "json",
			Validate: false,
		},
	},
}

func NewKafkaProxy(cl client.I, cfg Props, prom *prometheus.Registry) *KafkaProxy {
	if err := mergo.Merge(&cfg, DefaultConfig); err != nil {
		logger.Get().Panic("Could not merge config: %s", err)
	}
	logger.Get().Debugf("KafkaProxy config loaded: %+v", cfg)
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Millisecond * 500
	b.MaxElapsedTime = time.Second * 0
	b.MaxInterval = time.Second * 30
	cb := circuit.NewBreakerWithOptions(&circuit.Options{
		BackOff:    b,
		ShouldTrip: circuit.ThresholdTripFunc(cfg.CircuitBreakerMaxFails),
	})
	var topics []string
	validateJsonTopics := make(map[string]bool)
	for _, tconf := range cfg.Topics {
		topics = append(topics, tconf.Name)
		if tconf.Format == "json" && tconf.Validate {
			validateJsonTopics[tconf.Name] = true
		}
	}
	logger.Get().Infof("kafka_proxy.topics: %s", topics)

	logger.Get().Infof("Enabling JSON validation for topics: %+v", validateJsonTopics)
	cl.SetValidateJsonTopics(validateJsonTopics)
	logger.Get().Infof("Done with: %+v", validateJsonTopics)
	kp := &KafkaProxy{
		client:  cl,
		prom:    prom,
		Config:  cfg,
		breaker: cb,
		Topics:  topics,
	}
	kp.Run()
	return kp
}

func (kp *KafkaProxy) Run() {
	logger.Get().Infof("Running topic refresh routine with %v interval", kp.Config.TopicRefreshInterval)
	go func() {
		for {
			logger.Get().Debug("Fetching kafka topic list")
			topics, err := kp.client.ListTopics()
			if err != nil {
				logger.Get().Warn("Could NOT fetch topic list: %v", err)
			} else {
				kp.MetadataTopics = topics
				logger.Get().Debugf("Fetched %d topics successfully: %+v", len(topics), topics)
			}
			time.Sleep(10 * kp.Config.TopicRefreshInterval)
		}
	}()
}

func (kp *KafkaProxy) IsTopicValid(topic string) bool {
	for _, v := range kp.Topics {
		if topic == v {
			return true
		}
	}
	for _, mt := range kp.MetadataTopics {
		if topic == mt {
			return true
		}
	}
	logger.Get().Debugf("Topic is not valid: %s", topic)
	return false
}

// Deprecated: use SendEvents instead
func (kp *KafkaProxy) SendMessages(topic string, lor line_reader.I) (confirmedCnt uint64, filteredCnt uint64, err error) {
	var lastConfirmedOffset uint64
	if kp.breaker.Ready() {
		confirmedCnt, lastConfirmedOffset, filteredCnt, err = kp.client.SendMessages(topic, lor)
		logger.Get().Debugf("Topic: %s, LastConfirmedOffset: %d", topic, lastConfirmedOffset)
		if err != nil && err != io.EOF {
			logger.Get().Debugf("Kafka proxy SendMessages error: %s", err)
			kp.breaker.Fail() // This will trip the breaker once it's failed 10 times
		}
		kp.breaker.Success()
	} else {
		err = errors.New("Circuit breaker open")
		logger.Get().Debug("Making no kafka proxy request; CircuitBreaker is open.")
	}
	return confirmedCnt, filteredCnt, err
}

func (kp *KafkaProxy) SendEvents(eventReader event.Reader) (confirmedCnt uint64, filteredCnt uint64, err error) {
	var lastConfirmedOffset uint64
	if kp.breaker.Ready() {
		confirmedCnt, lastConfirmedOffset, filteredCnt, err = kp.client.SendEvents(eventReader)
		logger.Get().Debugf("LastConfirmedOffset: %d", lastConfirmedOffset)
		if err != nil && err != io.EOF {
			logger.Get().Debugf("Kafka proxy SendMessages error: %s", err)
			kp.breaker.Fail() // This will trip the breaker once it's failed 10 times
		}
		kp.breaker.Success()
	} else {
		err = errors.New("Circuit breaker open")
		logger.Get().Debug("Making no kafka proxy request; CircuitBreaker is open.")
	}
	return confirmedCnt, filteredCnt, err
}

func (kp *KafkaProxy) ListTopics() ([]string, error) {
	return kp.client.ListTopics()
}

func (kp *KafkaProxy) GetBreaker() *circuit.Breaker {
	return kp.breaker
}

func (kp *KafkaProxy) GetClient() client.I {
	return kp.client
}

func (kp *KafkaProxy) GetTopicProps(topic string) TopicProps {
	for _, tp := range kp.Config.Topics {
		if tp.Name == topic {
			return tp
		}
	}
	for _, tp := range kp.MetadataTopics {
		if tp == topic {
			return TopicProps{}
		}
	}
	logger.Get().Errorf("Could not find topic props: %s", topic)
	return TopicProps{}
}
