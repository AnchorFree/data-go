package client

import (
	//"fmt"
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

type jsonFilterTest struct {
	name    string
	message []byte
	valid   bool
}

var jsonTests = []jsonFilterTest{
	{
		"valid JSON",
		[]byte(`{"event":"test","properties":{"field": 123}}`),
		true,
	},
	{
		"malformed JSON",
		[]byte(`{"event":"test","properties",{"field": 123}}`),
		false,
	},
	{
		"invalid escape symbol",
		[]byte(`{"event":"test\u0:"}`),
		false,
	},
}

func TestClientFilter(t *testing.T) {
	topic := "test"
	//prom := prometheus.NewRegistry()
	cfg := Props{}

	cl := &T{}
	cl.Config = cfg
	validateJsonTopics := map[string]bool{
		topic: true,
	}
	cl.SetValidateJsonTopics(validateJsonTopics)
	for _, test := range jsonTests {
		filteredTopic, filteredMessage, filtered := cl.FilterTopicMessage(topic, test.message)
		assert.Equalf(t, test.valid, !filtered, "Filter was not applied correctly. filtered: %v, message: %s", filtered, test.message)
		expectedTopic := topic
		expectedMessage := test.message
		if filtered {
			expectedTopic = cl.GetInvalidMessagesTopic()
			expectedMessage = bytes.Join([][]byte{[]byte(topic), []byte(test.message)}, []byte("\t"))
		}
		assert.Equalf(t, expectedTopic, filteredTopic, "Topic was not correctly filtered. test: %s", test.name)
		assert.Equalf(t, expectedMessage, filteredMessage, "Message was not correctly filtered. test: %s", test.name)
	}
}
