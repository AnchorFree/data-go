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
	"github.com/anchorfree/data-go/pkg/event"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/logger"
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

// Deprecated: use SendEvent instead
func (c *Client) SendMessage(topic string, message []byte) error {
	var err error

	fullUrl := fmt.Sprintf("%s/topics/%s/messages", strings.Trim(c.Url, "/ "), strings.Trim(topic, "/ "))
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fullUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.SetBody(message)

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

// Deprecated: use SendEvents instead
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

func (c *Client) SendEvent(eventEntry *event.Event) error {
	var err error

	fullUrl := fmt.Sprintf("%s/topics/%s/messages", strings.Trim(c.Url, "/ "), strings.Trim(eventEntry.Topic, "/ "))
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fullUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("text/plain")
	req.SetBody(eventEntry.Message)

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

func (c *Client) SendEvents(eventReader event.Reader) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	confirmedCnt = 0
	filteredCnt = 0
	var erErr error

	for {
		eventEntry := eventReader.ReadEvent()
		if eventEntry.Message != nil && len(eventEntry.Message) > 0 {
			filteredEvent, filtered := c.FilterTopicEvent(eventEntry)
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
		if eventEntry.Error != nil {
			erErr = eventEntry.Error
			break
		}
	}
	logger.Get().Debugf("Error SendMessages: %s", err)
	return confirmedCnt, lastConfirmedOffset, filteredCnt, erErr
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
