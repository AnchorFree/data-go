package grpc_client

import (
	"io"
	"strings"

	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/anchorfree/data-go/pkg/ambassador/pb"
	"github.com/anchorfree/data-go/pkg/clients/client"
	"github.com/anchorfree/data-go/pkg/logger"
	"github.com/anchorfree/data-go/pkg/types"
)

type Props struct {
	client.Props
	GrpcEnableMetrics   bool `yaml:"enable_metrics"`
	GrpcEnableHistogram bool `yaml:"enable_histogram"`
}

type GrpcClient struct {
	client.T
	client pb.KafkaAmbassadorClient
	Config Props
	Url    string
}

var _ client.ClientTransport = (*GrpcClient)(nil)

var DefaultConfig Props = Props{
	GrpcEnableMetrics:   false,
	GrpcEnableHistogram: false,
}

func NewClient(url string, config interface{}, prom *prometheus.Registry) *GrpcClient {
	c := &GrpcClient{}
	c.Prom = prom
	c.Url = url
	c.Config = config.(Props)
	if err := mergo.Merge(&c.Config, DefaultConfig); err != nil {
		logger.Get().Panicf("Could not merge config: %s", err)
	}
	logger.Get().Debugf("GrpcClient config loaded: %+v", c.Config)
	logger.Get().Infof("Initialized GRPC target address: %s", c.Url)
	c.RegisterMetrics()
	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	if c.Config.GrpcEnableMetrics {
		logger.Get().Info("Turning on GRPC metrics")
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(grpcMetrics.UnaryClientInterceptor()))
		dialOpts = append(dialOpts, grpc.WithStreamInterceptor(grpcMetrics.StreamClientInterceptor()))
	}
	if c.Config.GrpcEnableHistogram {
		logger.Get().Info("Turning on GRPC metric histograms")
		grpcMetrics.EnableClientHandlingTimeHistogram()
	}

	trimmedUrl := strings.TrimLeft(c.Url, "grpc://")
	cc, err := grpc.Dial(trimmedUrl, dialOpts...)
	if err != nil {
		logger.Get().Error(err)
	}
	if cc == nil {
		logger.Get().Fatal("didn't make connection")
	}
	c.client = pb.NewKafkaAmbassadorClient(cc)
	return c
}

func (c *GrpcClient) SendEvents(iterator types.EventIterator) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
	stream, streamErr := c.client.Produce(context.Background())
	cnt := 0
	confirmedCnt = 0
	filteredCnt = 0
	if streamErr != nil {
		logger.Get().Error("Could not create GRPC stream: %s", streamErr)
		return confirmedCnt, lastConfirmedOffset, filteredCnt, streamErr
	} else {
		waitc := make(chan struct{})
		go func() {
			for {
				srvResponse, err := stream.Recv()
				if err == io.EOF {
					close(waitc)
					return
				}
				if err != nil {
					close(waitc)
					_ = stream.CloseSend()
					logger.Get().Errorf("Failed to receive GRPC server response: %v", err)
					return
				}
				lastConfirmedOffset = srvResponse.StreamOffset
				confirmedCnt++
				//logger.Get().Printf("Got confirmed offset: %d", lastConfirmedOffset)
			}
		}()
		for iterator.Next() {
			cnt++
			event := iterator.At()
			if event.Message != nil && len(event.Message) > 0 {
				filteredEvent, filtered := c.FilterTopicEvent(event)
				if filtered {
					filteredCnt++
				}
				rq := pb.ProdRq{
					Topic:        filteredEvent.Topic,
					Message:      filteredEvent.Message,
					StreamOffset: filteredEvent.Offset,
				}
				if err := stream.Send(&rq); err != nil {
					return confirmedCnt, lastConfirmedOffset, filteredCnt, err
				}
			}
		}
		if srcErr := iterator.Err(); srcErr != nil {
			srcErr = types.NewErrClientRequest(srcErr.Error())
		}
		_ = stream.CloseSend()
		<-waitc
	}
	logger.Get().Debugf("Finished streaming. Lines: %d, confirmedLines: %d, LastConfirmedOffset: %d, err: %s", cnt, confirmedCnt, lastConfirmedOffset, err)
	return confirmedCnt, lastConfirmedOffset, filteredCnt, err
}

func (c *GrpcClient) ListTopics() ([]string, error) {
	var topics []string
	resp, err := c.client.ListTopics(context.Background(), &pb.Empty{})
	if err != nil {
		return topics, err
	}
	for _, t := range resp.Topics {
		topics = append(topics, t)
	}
	return topics, err
}
