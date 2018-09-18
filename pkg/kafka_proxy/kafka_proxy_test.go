package kafka_proxy

import (
	"bytes"
	"fmt"
	pb "github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver/pb"
	"github.com/anchorfree/ula-edge/pkg/line_offset_reader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

func TestKafkaProxyHttpRequests(t *testing.T) {
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
	cfg := Props{
		Url: ts.URL,
	}
	proxy := NewKafkaProxy(cfg, prom)
	lor := line_offset_reader.NewLineOffsetReader(bytes.NewReader(message))
	_, _, err := proxy.SendMessages(topic, lor)
	if err != nil {
		t.Errorf("KafkaProxy returned an error: %s", err)
	}
}

type TestServer struct {
	t *testing.T
}

func (s *TestServer) Produce(stream pb.KafkaAmbassador_ProduceServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
	}
}

func TestKafkaProxyGrpcRequests(t *testing.T) {
	topic := "test"
	fullMessage := []byte(`Eins zwei Polizei
drei vier Grenadier
fünf sechs alte Gags
sieben acht gute Nacht`)
	var grpcSrv *grpc.Server

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("Could not connect: %s", err)
	}
	addr := lis.Addr().String()
	grpcSrv = grpc.NewServer()
	pb.RegisterKafkaAmbassadorServer(grpcSrv, &TestServer{t: t})
	go func() {
		grpcSrv.Serve(lis)
	}()
	prom := prometheus.NewRegistry()
	cfg := Props{
		Url: "grpc://" + addr,
	}
	proxy := NewKafkaProxy(cfg, prom)

	lor := line_offset_reader.NewLineOffsetReader(bytes.NewReader(fullMessage))
	_, _, err = proxy.SendMessages(topic, lor)
	if err != nil {
		t.Errorf("KafkaProxy returned an error: %s", err)
	}
	//Stop GRPC server to test CircuitBreaker
	grpcSrv.Stop()
	assert.True(t, proxy.GetBreaker().Ready(), "Circuit breaker should be close (ready)")
	for i := 1; i <= int(proxy.Config.CircuitBreakerMaxFails); i++ {
		lor = line_offset_reader.NewLineOffsetReader(bytes.NewReader(fullMessage))
		_, _, err = proxy.SendMessages(topic, lor)
		if err == nil {
			t.Error("KafkaProxy is supposed to return error as GRPC server is down")
		}
	}
	assert.False(t, proxy.GetBreaker().Ready(), "Circuit breaker should be open (not ready)")

	lis, err = net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("Could not listend at %s: %s", addr, err)
	}
	grpcSrv = grpc.NewServer()
	pb.RegisterKafkaAmbassadorServer(grpcSrv, &TestServer{t: t})
	go func() {
		er := grpcSrv.Serve(lis)
		t.Error(er)
	}()
	lor = line_offset_reader.NewLineOffsetReader(bytes.NewReader(fullMessage))
	_, _, err = proxy.SendMessages(topic, lor)
	fmt.Println(err)
	if err == nil {
		t.Error("KafkaProxy is supposed to return error circuitbreaker is open (not ready)")
	}
	time.Sleep(proxy.GetBreaker().BackOff.NextBackOff() + 1*time.Second)
	lor = line_offset_reader.NewLineOffsetReader(bytes.NewReader(fullMessage))
	_, _, err = proxy.SendMessages(topic, lor)
	if err != nil {
		t.Errorf("KafkaProxy returned an error: %s", err)
	}
}

func TestKafkaProxyFiltersInitialization(t *testing.T) {
	topic := "test"
	prom := prometheus.NewRegistry()
	cfg := Props{
		Url: "grpc://localhost:19043",
		Topics: []TopicProps{
			TopicProps{
				Name:     topic,
				Format:   "json",
				Validate: true,
			},
		},
	}
	proxy := NewKafkaProxy(cfg, prom)
	validateTopics := proxy.GetClient().GetValidateJsonTopics()
	validate, validateOK := validateTopics[topic]
	assert.Truef(t, validateOK, "Topic %s is not in validation topics list", topic)
	assert.Truef(t, validate, "Validation for topic %s is not enabled", topic)
}

func TestKafkaProxyTopicListInitialization(t *testing.T) {
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
	proxy := NewKafkaProxy(cfg, prom)
	expectedTopics := []string{"topic1", "topic2", "topic3"}
	sort.Strings(proxy.Topics)
	assert.Equal(t, expectedTopics, proxy.Topics)
}
