package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Writer interface {
	SetLog(Logger)
	Message(headers map[string][]byte, msg []byte) error
	Close() error
}

func (c *KafkaConfig) NewKafkaWriter(ctx context.Context, topic string) Writer {
	return &writerImpl{
		ctx: ctx,
		kafka: &kafka.Writer{
			Addr:     kafka.TCP(c.Brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

type writerImpl struct {
	ctx   context.Context
	kafka *kafka.Writer
	l     Logger
}

func (wi *writerImpl) SetLog(l Logger) {
	wi.l = l
}

func (wi *writerImpl) Message(headers map[string][]byte, msg []byte) error {
	var myheaders []kafka.Header
	for k, v := range headers {
		myheaders = append(myheaders, kafka.Header{
			Key:   k,
			Value: v,
		})
	}
	m := kafka.Message{
		Value:   msg,
		Headers: myheaders,
	}
	if wi.l != nil {
		wi.l.Info(fmt.Sprintf(
			"write kafka topic [%s], header [%v] message: %s",
			wi.kafka.Topic, myheaders, string(msg)))
	}
	err := wi.kafka.WriteMessages(wi.ctx, m)
	if err != nil {
		return err
	}
	return nil
}

func (wi *writerImpl) Close() error {
	return wi.kafka.Close()
}
