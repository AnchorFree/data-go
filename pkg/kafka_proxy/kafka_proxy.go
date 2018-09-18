package kafka_proxy

import (
	"errors"
	"github.com/anchorfree/ula-edge/pkg/clients/client"
	"github.com/anchorfree/ula-edge/pkg/clients/grpc_client"
	"github.com/anchorfree/ula-edge/pkg/clients/http_client"
	//"github.com/anchorfree/ula-edge/pkg/config"
	"github.com/anchorfree/ula-edge/pkg/line_reader"
	"github.com/anchorfree/ula-edge/pkg/logger"
	"github.com/cenk/backoff"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	circuit "github.com/rubyist/circuitbreaker"
	//"strconv"
	"strings"
	"time"
)

//import context "golang.org/x/net/context"

type KafkaProxy struct {
	//client     *circuit.HTTPClient
	client  client.I
	Config  Props
	prom    *prometheus.Registry
	breaker *circuit.Breaker
	Topics  []string
}

type Props struct {
	Url                    string
	CircuitBreakerMaxFails int64 `yaml:"circuitbreaker_max_fails"`
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
	Url: "grpc://localhost:19094",
	CircuitBreakerMaxFails: 3,
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

func NewKafkaProxy(cfg Props, prom *prometheus.Registry) *KafkaProxy {
	//maxFails := int64(cfg.GetInt("kafka_proxy.circuitbreaker.max_fails"))

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

	var cl client.I

	if strings.HasPrefix(cfg.Url, "grpc://") {
		trimmedUrl := strings.TrimLeft(cfg.Url, "grpc://")
		cl = &grpc_client.Client{}
		if len(cfg.GrpcClientConfig.Url) == 0 {
			cfg.GrpcClientConfig.Url = trimmedUrl
		}
		cl.Init(cfg.GrpcClientConfig, prom)
	} else {
		cl = &http_client.Client{}
		if len(cfg.HttpClientConfig.Url) == 0 {
			cfg.HttpClientConfig.Url = cfg.Url
		}
		cl.Init(cfg.HttpClientConfig, prom)
	}
	logger.Get().Infof("Enabling JSON validation for topics: %+v", validateJsonTopics)
	cl.SetValidateJsonTopics(validateJsonTopics)
	return &KafkaProxy{
		client:  cl,
		Config:  cfg,
		breaker: cb,
		Topics:  topics,
	}

}

func (kp *KafkaProxy) IsTopicValid(topic string) bool {
	for _, v := range kp.Topics {
		if topic == v {
			return true
		}
	}
	logger.Get().Debugf("Topic is not valid: %s", topic)
	return false
}

func (kp *KafkaProxy) SendMessages(topic string, lor line_reader.I) (confirmedCnt uint64, filteredCnt uint64, err error) {
	var lastConfirmedOffset uint64
	if kp.breaker.Ready() {
		confirmedCnt, lastConfirmedOffset, filteredCnt, err = kp.client.SendMessages(topic, lor)
		logger.Get().Debugf("Topic: %s, LastConfirmedOffset: %d", topic, lastConfirmedOffset)
		if err != nil {
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

func (kp *KafkaProxy) GetBreaker() *circuit.Breaker {
	return kp.breaker
}

func (kp *KafkaProxy) GetClient() client.I {
	return kp.client
}
