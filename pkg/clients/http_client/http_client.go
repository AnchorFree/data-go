package http_client

import (
	"fmt"
	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"
	"strings"
	"time"
)

type Props struct {
	client.Props
	Url            string
	RequestTimeout time.Duration
}

type Client struct {
	client.T
	client *fasthttp.Client
	Config Props
}

var DefaultConfig Props = Props{
	Url:            "http://localhost:19092",
	RequestTimeout: 5 * time.Second,
}

func (c *Client) Init(config interface{}, prom *prometheus.Registry) {
	c.Prom = prom
	c.Config = config.(Props)
	if err := mergo.Merge(&c.Config, DefaultConfig); err != nil {
		logger.Get().Panicf("Could not merge config: %s", err)
	}
	logger.Get().Debugf("HttpClient config loaded: %+v", c.Config)
	/*
		var timeErr error
		c.proxyRequestTimeout, timeErr = time.ParseDuration(c.Config.GetString("kafka_proxy.http.request_timeout"))
		if timeErr != nil {
			logger.Get().Fatalf("Failed to parse kafka_proxy.http.request_timeout: %s", timeErr)
		}
	*/
	c.client = &fasthttp.Client{
		MaxConnsPerHost: 1000,
	}
}

func (c *Client) SendMessage(topic string, message []byte) error {
	var err error

	fullUrl := fmt.Sprintf("%s/topics/%s/messages", strings.Trim(c.Config.Url, "/ "), strings.Trim(topic, "/ "))
	//startTime := time.Now()
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fullUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.SetBody(message)

	resp := fasthttp.AcquireResponse()
	err = c.client.DoTimeout(req, resp, c.Config.RequestTimeout)

	//bodyBytes := resp.Body()

	//reqTime := time.Since(startTime).Seconds()
	if resp != nil {
		if resp.StatusCode() > 299 {
			logger.Get().Debugf("Kafka proxy request status: %s", resp.StatusCode())
		}
	}
	if err != nil {
		logger.Get().Debugf("Kafka proxy request error: %s", err)
	}

	return err
}

func (c *Client) SendMessages(topic string, lor line_reader.I) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	confirmedCnt = 0
	filteredCnt = 0
	var lorErr error

	for {
		line, lastOffset, lorErr := lor.ReadLine()
		if line != nil && len(line) > 0 {
			filteredTopic, filteredMessage, filtered := c.FilterTopicMessage(topic, line)
			if filtered {
				filteredCnt++
			}
			err = c.SendMessage(filteredTopic, filteredMessage)
			if err != nil {
				logger.Get().Debugf("Could not send a message: %s", err)
				return confirmedCnt, lastOffset, filteredCnt, err
			}
			confirmedCnt++
			lastConfirmedOffset = lastOffset
		}
		if lorErr != nil {
			break
		}
	}
	logger.Get().Debugf("Error SendMessages: %s", err)
	return confirmedCnt, lastConfirmedOffset, filteredCnt, lorErr
}
