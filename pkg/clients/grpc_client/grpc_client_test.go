package grpc_client

import (
	"bytes"
	//"fmt"
	pb "github.com/anchorfree/data-go/pkg/ambassador/pb"
	"github.com/anchorfree/data-go/pkg/line_offset_reader"
	"github.com/anchorfree/data-go/pkg/testutils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"net"
	"testing"
)

type TopicMessage struct {
	topic   string
	message []byte
}

type TestServer struct {
	ch chan TopicMessage
	t  *testing.T
}

func (s *TestServer) Produce(stream pb.KafkaAmbassador_ProduceServer) error {
	var res *pb.ProdRs
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		s.ch <- TopicMessage{req.Topic, req.Message}
		res = &pb.ProdRs{StreamOffset: req.StreamOffset}
		err = stream.Send(res)
		if err != nil {
			s.t.Errorf("Could not send repsponse from GRPC server: %s", err)
		}
	}
}

func TestGrpcRequests(t *testing.T) {
	topic := "test"
	fullMessage := []byte(`Eins zwei Polizei
drei vier Grenadier
fünf sechs alte Gags
sieben acht gute Nacht`)
	chSize := len(bytes.Split(fullMessage, []byte("\n")))
	testCh := make(chan TopicMessage, chSize)
	var grpcSrv *grpc.Server

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("Could not connect: %s", err)
	}
	addr := lis.Addr().String()
	grpcSrv = grpc.NewServer()
	pb.RegisterKafkaAmbassadorServer(grpcSrv, &TestServer{t: t, ch: testCh})
	go func() {
		grpcSrv.Serve(lis)
	}()

	prom := prometheus.NewRegistry()
	cl := &Client{}
	cfg := Props{
		Url: addr,
	}
	cl.Init(cfg, prom)
	lor := line_offset_reader.NewReader(bytes.NewReader(fullMessage))
	_, lastConfirmedOffset, _, err := cl.SendMessages(topic, lor)
	if err != nil {
		t.Errorf("Error sending messages: %s", err)
	}

	var resMessages [][]byte
	for i := 1; i <= chSize; i++ {
		//for m := range testCh {
		m := <-testCh
		resMessages = append(resMessages, m.message)
		assert.Equal(t, topic, m.topic)
	}
	offsets := testutils.GetLineOffsets(t, string(fullMessage))
	assert.Equalf(t, fullMessage, bytes.Join(resMessages, []byte("\n")), "Sent and received messages do not match:\n -------- \n%s\n -- VS -- \n%s\n -------- ",
		string(fullMessage), string(bytes.Join(resMessages, []byte("\n"))))
	expectedLastOffset := offsets[len(offsets)-1]
	assert.Equal(t, expectedLastOffset, lastConfirmedOffset, "Last message offset does not match")

	grpcSrv.Stop()
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
	var grpcSrv *grpc.Server
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("Could not connect: %s", err)
	}
	addr := lis.Addr().String()
	grpcSrv = grpc.NewServer()
	pb.RegisterKafkaAmbassadorServer(grpcSrv, &TestServer{t: t, ch: testCh})
	go func() {
		grpcSrv.Serve(lis)
	}()
	prom := prometheus.NewRegistry()
	cl := &Client{}
	cfg := Props{
		Url: addr,
	}
	cl.Init(cfg, prom)
	validateJsonTopics := map[string]bool{
		topic: true,
	}
	cl.SetValidateJsonTopics(validateJsonTopics)
	for _, test := range jsonTests {
		lor := line_offset_reader.NewReader(bytes.NewReader(test.message))
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

	grpcSrv.Stop()
}
