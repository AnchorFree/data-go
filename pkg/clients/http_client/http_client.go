package http_client

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"

	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type Props struct {
	client.Props
	RequestTimeout time.Duration
}

type Client struct {
	client.T
	client *fasthttp.Client
	Config Props
	Url    string
}

var _ client.I = (*Client)(nil)

var DefaultConfig Props = Props{
	RequestTimeout: 5 * time.Second,
}

func NewClient(url string, config interface{}, prom *prometheus.Registry) *Client {
	c := &Client{}
	c.Url = url
	c.Prom = prom
	c.Config = config.(Props)
	if err := mergo.Merge(&c.Config, DefaultConfig); err != nil {
		logger.Get().Panicf("Could not merge config: %s", err)
	}
	logger.Get().Debugf("HttpClient config loaded: %+v", c.Config)
	c.client = &fasthttp.Client{
		MaxConnsPerHost: 1000,
	}
	return c
}

func (c *Client) SendEvent(event *types.Event) error {
	var err error

	fullUrl := fmt.Sprintf("%s/topics/%s/messages", strings.Trim(c.Url, "/ "), strings.Trim(event.Topic, "/ "))
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fullUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.SetBody(event.Message)

	resp := fasthttp.AcquireResponse()
	err = c.client.DoTimeout(req, resp, c.Config.RequestTimeout)

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

func (c *Client) SendEvents(iterator types.EventIterator) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	confirmedCnt = 0
	filteredCnt = 0

	for iterator.Next() {
		event := iterator.At()
		if event.Message != nil && len(event.Message) > 0 {
			filteredEvent, filtered := c.FilterTopicEvent(event)
			if filtered {
				filteredCnt++
			}
			err = c.SendEvent(filteredEvent)
			if err != nil {
				logger.Get().Debugf("Could not send a message: %s", err)
				return confirmedCnt, filteredEvent.Offset, filteredCnt, err
			}
			confirmedCnt++
			lastConfirmedOffset = filteredEvent.Offset
		}
	}
	if iterator.Err() != nil {
		logger.Get().Debugf("Client request error: %s", err)
		err = types.NewErrClientRequest(iterator.Err().Error())
	}
	return confirmedCnt, lastConfirmedOffset, filteredCnt, err
}

func (c *Client) ListTopics() ([]string, error) {
	var topics []string

	var err error
	fullUrl := fmt.Sprintf("%s/topics", strings.Trim(c.Url, "/ "))
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fullUrl)
	req.Header.SetMethod("GET")
	req.Header.SetContentType("text/plain")

	resp := fasthttp.AcquireResponse()
	err = c.client.DoTimeout(req, resp, c.Config.RequestTimeout)
	if resp == nil {
		if resp.StatusCode() > 299 {
			logger.Get().Debugf("Kafka proxy topic list request status: %d", resp.StatusCode())
			return topics, errors.New("Non-200 http code returned")
		}
	}
	if err != nil {
		logger.Get().Debugf("Kafka proxy topic list request error: %s", err)
		return topics, err
	}
	bodyBytes := resp.Body()
	for _, t := range bytes.Split(bodyBytes, []byte("\n")) {
		if len(t) > 0 {
			topics = append(topics, string(t))
		}
	}
	return topics, err
}
