package http_client

import (
	"bytes"
	"fmt"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestHttpRequests(t *testing.T) {
	topic := "test"
	message := []byte("This is a test string")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		body, err := ioutil.ReadAll(r.Body)
		expectedPath := fmt.Sprintf("/topics/%s/messages", topic)

		assert.Equal(t, expectedPath, r.URL.EscapedPath(), "Request path is not what expected")
		assert.Equal(t, message, body, "Request body is not what expected")
		assert.Equal(t, "text/plain", r.Header.Get("Content-Type"), "Wrong Content-Type")
		assert.Equal(t, "POST", r.Method, "Wrong request method")

		if err != nil {
			t.Errorf("Could not read request body: %s", err)
		}
	}))
	defer ts.Close()

	prom := prometheus.NewRegistry()
	cl := &Client{}
	cfg := Props{
		Url: ts.URL,
	}
	cl.Init(cfg, prom)
	lor := line_offset_reader.NewLineOffsetReader(bytes.NewReader(message))
	//confirmedCnt, lastConfirmedOffset, err := cl.SendMessages(topic, lor)
	_, _, _, err := cl.SendMessages(topic, lor)
	if err != nil {
		t.Errorf("KafkaProxy returned an error: %s", err)
	}
}

type TopicMessage struct {
	topic   string
	message []byte
}

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

func TestJsonFilter(t *testing.T) {
	topic := "test"
	testCh := make(chan TopicMessage, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		body, err := ioutil.ReadAll(r.Body)
		re := regexp.MustCompile(".*/topics/([A-z0-9_-]+)/messages.*")
		match := re.FindStringSubmatch(r.URL.EscapedPath())
		assert.Equal(t, 2, len(match))
		requestTopic := match[1]
		testCh <- TopicMessage{requestTopic, body}

		if err != nil {
			t.Errorf("Could not read request body: %s", err)
		}
	}))
	defer ts.Close()
	prom := prometheus.NewRegistry()
	cl := &Client{}
	cfg := Props{
		Url: ts.URL,
	}
	cl.Init(cfg, prom)
	validateJsonTopics := map[string]bool{
		topic: true,
	}
	cl.SetValidateJsonTopics(validateJsonTopics)
	for _, test := range jsonTests {
		lor := line_offset_reader.NewLineOffsetReader(bytes.NewReader(test.message))
		_, _, filteredCnt, err := cl.SendMessages(topic, lor)
		if err != nil {
			t.Errorf("Error sending messages: %s", err)
		}
		loopedRecord := <-testCh
		expectedTopic := topic
		expectedMessage := test.message
		expectedFilteredCnt := 0
		if !test.valid {
			expectedFilteredCnt = 1
			expectedTopic = cl.GetInvalidMessagesTopic()
			expectedMessage = bytes.Join([][]byte{[]byte(topic), test.message}, []byte("\t"))
		}
		assert.Equalf(t, string(expectedMessage), string(loopedRecord.message), "test: %s", test.name)
		assert.Equalf(t, expectedTopic, loopedRecord.topic, "test: %s", test.name)
		assert.Equalf(t, uint64(expectedFilteredCnt), filteredCnt, "test: %s", test.name)
	}
}
