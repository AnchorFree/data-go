package kafka_proxy

import (
	"bytes"
	"io"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/anchorfree/data-go/pkg/line_reader"
	"github.com/anchorfree/data-go/pkg/testutils"
	"github.com/anchorfree/data-go/pkg/event"
)

//Mock the kafka_proxy transport client
type MockedClient struct {
	mock.Mock
}

var _ client.I = (*MockedClient)(nil)

func (m *MockedClient) SendMessages(topic string, lor line_reader.I) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	args := m.Called(topic, lor)
	return args.Get(0).(uint64), args.Get(1).(uint64), args.Get(2).(uint64), args.Error(3)
}

func (m *MockedClient) SendEvents(eventReader event.Reader) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	args := m.Called(eventReader)
	return args.Get(0).(uint64), args.Get(1).(uint64), args.Get(2).(uint64), args.Error(3)
}

func (m *MockedClient) FilterTopicMessage(topic string, message []byte) (string, []byte, bool) {
	args := m.Called(topic, message)
	return args.String(0), args.Get(1).([]byte), args.Bool(2)
}

func (m *MockedClient) SetValidateJsonTopics(topics map[string]bool) {
	m.Called(topics)
}

func (m *MockedClient) GetValidateJsonTopics() map[string]bool {
	args := m.Called()
	return args.Get(0).(map[string]bool)
}

func (m *MockedClient) ListTopics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func TestSendMessages(t *testing.T) {
	topic := "test"
	message := []byte("This is a test string\nwith an extra tailing line")
	lor := line_offset_reader.NewReader(bytes.NewReader(message))

	prom := prometheus.NewRegistry()
	cl := &MockedClient{}
	expectedCount := uint64(testutils.GetLineCount(string(message)))
	offsets := testutils.GetLineOffsets(t, string(message))
	expectedOffset := offsets[len(offsets)-1]
	expectedFilteredLines := uint64(1)
	cl.On("SendMessages", "test", lor).Return(expectedCount, expectedOffset, expectedFilteredLines, io.EOF)
	cl.On("SetValidateJsonTopics", mock.Anything).Maybe()
	cl.On("ListTopics").Return([]string{topic}, nil).Maybe()
	proxy := NewKafkaProxy(cl, DefaultConfig, prom)
	confirmedCnt, filteredCnt, err := proxy.SendMessages(topic, lor)
	if err != nil && err != io.EOF {
		t.Errorf("KafkaProxy returned an error and it is not EOF: %s", err)
	}
	cl.AssertExpectations(t)
	assert.Equal(t, expectedCount, confirmedCnt, "Wrong confirmed message count")
	assert.Equal(t, expectedFilteredLines, filteredCnt, "Wrong filtered lines count")
}

func TestListTopics(t *testing.T) {
	topics := []string{"test", "one", "two", "last-topic"}
	prom := prometheus.NewRegistry()
	cl := &MockedClient{}
	cl.On("ListTopics").Return(topics, nil)
	cl.On("SetValidateJsonTopics", mock.Anything).Maybe()
	proxy := NewKafkaProxy(cl, DefaultConfig, prom)
	resultTopics, err := proxy.ListTopics()
	cl.AssertExpectations(t)
	assert.Equal(t, topics, resultTopics, "Wrong topic list returned")
	assert.NoErrorf(t, err, "ListTopics returned error: %+v", err)
}

func TestKafkaProxyFiltersInitialization(t *testing.T) {
	prom := prometheus.NewRegistry()
	cfg := Props{
		Url: "grpc://localhost:19043",
		Topics: []TopicProps{
			TopicProps{
				Name:     "test",
				Format:   "json",
				Validate: true,
			},
			TopicProps{
				Name: "unfiltered",
			},
		},
	}
	validateJsonTopics := map[string]bool{"test": true}
	cl := &MockedClient{}
	cl.On("ListTopics").Return([]string{"test", "unfiltered"}, nil).Maybe()
	cl.On("SetValidateJsonTopics", validateJsonTopics)
	_ = NewKafkaProxy(cl, cfg, prom)
	cl.AssertExpectations(t)
}

func TestKafkaProxyTopicListInitialization(t *testing.T) {
	topic := "test"
	prom := prometheus.NewRegistry()
	cfg := Props{
		Url: "grpc://localhost:19043",
		Topics: []TopicProps{
			TopicProps{
				Name: "topic1",
			},
			TopicProps{
				Name: "topic2",
			},
			TopicProps{
				Name: "topic3",
			},
		},
	}
	cl := &MockedClient{}
	cl.On("ListTopics").Return([]string{topic}, nil).Maybe()
	cl.On("SetValidateJsonTopics", mock.Anything).Maybe()
	proxy := NewKafkaProxy(cl, cfg, prom)
	expectedTopics := []string{"topic1", "topic2", "topic3"}
	sort.Strings(proxy.Topics)
	assert.Equal(t, expectedTopics, proxy.Topics)
	assert.Equal(t, cfg.Topics[0], proxy.GetTopicProps("topic1"))
}
