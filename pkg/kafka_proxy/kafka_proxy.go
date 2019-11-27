package kafka_proxy

import (
	"errors"
	"time"

	"github.com/cenk/backoff"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	circuit "github.com/rubyist/circuitbreaker"

	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/clients/grpc_client"
	"github.com/anchorfree/data-go/pkg/clients/http_client"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

//import context "golang.org/x/net/context"

type KafkaProxy struct {
	//client     *circuit.HTTPClient
	client         client.ClientTransport
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

func NewKafkaProxy(cl client.ClientTransport, cfg Props, prom *prometheus.Registry) *KafkaProxy {
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

func (kp *KafkaProxy) SendEvents(eventIterator types.EventIterator) (uint64, uint64, error) {
	if !kp.breaker.Ready() {
		err := errors.New("Circuit breaker open")
		logger.Get().Debug("Making no kafka proxy request; CircuitBreaker is open.")
		return 0, 0, err
	}

	confirmedCnt, lastConfirmedOffset, filteredCnt, err := kp.client.SendEvents(eventIterator)
	logger.Get().Debugf("LastConfirmedOffset: %d", lastConfirmedOffset)
	if err != nil {
		switch err.(type) {
		case *types.ErrClientRequest:
			logger.Get().Debugf("GrpcClient request error: %s", err)
		default:
			logger.Get().Debugf("Kafka proxy SendEvents error: %s", err)
			kp.breaker.Fail()
			return confirmedCnt, filteredCnt, err
		}
	}
	kp.breaker.Success()

	return confirmedCnt, filteredCnt, err
}

func (kp *KafkaProxy) ListTopics() ([]string, error) {
	return kp.client.ListTopics()
}

func (kp *KafkaProxy) GetBreaker() *circuit.Breaker {
	return kp.breaker
}

func (kp *KafkaProxy) GetClient() client.ClientTransport {
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
