package kafka_proxy

import (
	"bytes"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/anchorfree/data-go/pkg/testutils"
	"github.com/anchorfree/data-go/pkg/types"
)

//Mock the kafka_proxy transport client
type MockedClient struct {
	mock.Mock
}

var _ client.KafkaClient = (*MockedClient)(nil)

func (m *MockedClient) SendEvents(eventIterator types.EventIterator) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	args := m.Called(eventIterator)
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

func TestKafkaProxy_SendEvents(t *testing.T) {
	topic := "test"
	message := []byte("This is a test string\nwith an extra tailing line")
	lor := line_offset_reader.NewIterator(bytes.NewReader(message), topic)

	prom := prometheus.NewRegistry()
	cl := &MockedClient{}
	expectedCount := uint64(testutils.GetLineCount(t, string(message)))
	offsets := testutils.GetLineOffsets(t, string(message))
	expectedOffset := offsets[len(offsets)-1]
	expectedFilteredLines := uint64(1)
	cl.On("SendEvents", lor).Return(expectedCount, expectedOffset, expectedFilteredLines, nil)
	cl.On("SetValidateJsonTopics", mock.Anything).Maybe()
	cl.On("ListTopics").Return([]string{topic}, nil).Maybe()
	proxy := NewKafkaProxy(cl, DefaultConfig, prom)
	confirmedCnt, filteredCnt, err := proxy.SendEvents(lor)
	if err != nil {
		t.Errorf("KafkaProxy returned an error: %s", err)
	}
	cl.AssertExpectations(t)
	assert.Equal(t, expectedCount, confirmedCnt, "Wrong confirmed message count")
	assert.Equal(t, expectedFilteredLines, filteredCnt, "Wrong filtered lines count")
}

func TestKafkaProxy_ListTopics(t *testing.T) {
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

func TestKafkaProxy_FiltersInitialization(t *testing.T) {
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

func TestKafkaProxy_TopicListInitialization(t *testing.T) {
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
