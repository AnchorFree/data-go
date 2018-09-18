package grpc_client

import (
	pb "github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver/pb"
	"github.com/anchorfree/ula-edge/pkg/clients/client"
	"github.com/anchorfree/ula-edge/pkg/line_reader"
	"github.com/anchorfree/ula-edge/pkg/logger"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	"io"
)

type Props struct {
	client.Props
	Url                 string
	GrpcEnableMetrics   bool `yaml:"enable_metrics"`
	GrpcEnableHistogram bool `yaml:"enable_histogram"`
}

type Client struct {
	client.T
	client pb.KafkaAmbassadorClient
	Config Props
}

var DefaultConfig Props = Props{
	Url:                 "localhost:19094",
	GrpcEnableMetrics:   false,
	GrpcEnableHistogram: false,
}

func (c *Client) Init(config interface{}, prom *prometheus.Registry) {
	c.Prom = prom
	c.Config = config.(Props)
	if err := mergo.Merge(&c.Config, DefaultConfig); err != nil {
		logger.Get().Panicf("Could not merge config: %s", err)
	}
	logger.Get().Debugf("GrpcClient config loaded: %+v", c.Config)
	logger.Get().Infof("Initialized GRPC target address: %s", c.Config.Url)
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

	cc, err := grpc.Dial(c.Config.Url, dialOpts...)
	if err != nil {
		logger.Get().Error(err)
	}
	if cc == nil {
		logger.Get().Fatal("didn't make connection")
	}
	c.client = pb.NewKafkaAmbassadorClient(cc)
}

func (c *Client) SendMessages(topic string, lor line_reader.I) (confirmedCnt uint64, lastConfirmedOffset uint64, filteredCnt uint64, err error) {
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
					stream.CloseSend()
					logger.Get().Errorf("Failed to receive GRPC server response: %v", err)
					return
				}
				lastConfirmedOffset = srvResponse.StreamOffset
				confirmedCnt++
				//logger.Get().Printf("Got confirmed offset: %d", lastConfirmedOffset)
			}
		}()
		for {
			cnt++
			line, lastOffset, err := lor.ReadLine()
			if line != nil && len(line) > 0 {
				filteredTopic, filteredMessage, filtered := c.FilterTopicMessage(topic, line)
				if filtered {
					filteredCnt++
				}
				rq := pb.ProdRq{
					Topic:        filteredTopic,
					Message:      filteredMessage,
					StreamOffset: lastOffset,
				}
				sendErr := stream.Send(&rq)
				if sendErr != nil {
					return confirmedCnt, lastConfirmedOffset, filteredCnt, sendErr
				}
			}
			if err != nil {
				break
			}
		}
		stream.CloseSend()
		<-waitc
	}
	logger.Get().Debugf("Finished streaming. Topic: %s, Lines: %d, confirmedLines: %d, LastConfirmedOffset: %d, err: %s", topic, cnt, confirmedCnt, lastConfirmedOffset, err)
	return confirmedCnt, lastConfirmedOffset, filteredCnt, err
}
