package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/94peter/log"
	"github.com/segmentio/kafka-go"
)

type Reader interface {
	SetLog(log.Logger)
	Read() (map[string]string, []byte, error)
	ReadHandler(handler ReaderHandler) error
	Close() error
}

func (c *KafkaConfig) NewKafkaReader(ctx context.Context, groupID, topic string) Reader {
	dial := kafka.DefaultDialer
	hostname, _ := os.Hostname()
	dial.ClientID = "kafka@" + hostname
	return &readerImpl{
		ctx: ctx,
		kafka: kafka.NewReader(kafka.ReaderConfig{
			Dialer:   dial,
			Brokers:  c.Brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}
}

type readerImpl struct {
	ctx   context.Context
	kafka *kafka.Reader
	log   log.Logger
}

type ReaderHandler func(headers map[string]string, data []byte) error

func (ri *readerImpl) SetLog(l log.Logger) {
	ri.log = l
}
func (ri *readerImpl) Read() (map[string]string, []byte, error) {
	m, err := ri.kafka.ReadMessage(ri.ctx)
	if err != nil {
		return nil, nil, err
	}
	if ri.log != nil {
		ri.log.Debug(fmt.Sprintf("message at topic: %v partition: %v offset: %v value: %s\n", m.Topic, m.Partition, m.Offset, string(m.Value)))
	}
	var headers map[string]string
	if len(m.Headers) > 0 {
		headers = map[string]string{}
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}
	}
	return headers, m.Value, nil
}

const waitingTime = time.Second * 5
const maxRetriedTimes = 5

func (ri *readerImpl) ReadHandler(handler ReaderHandler) error {
	m, err := ri.kafka.FetchMessage(ri.ctx)
	if err != nil {
		return errors.New("fetch message fail: " + err.Error())
	}
	var headers map[string]string
	if len(m.Headers) > 0 {
		headers = map[string]string{}
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}
	}
	retriedTimes := 0
	for err = handler(headers, m.Value); err != nil; err = handler(headers, m.Value) {
		if ri.log != nil {
			ri.log.WarnPkg(fmt.Errorf("hanlder data fail and waiting for 5 secs to retry: %w", err))
		} else {
			fmt.Println("hanlder data fail and waiting for 5 secs to retry: ", err)
		}

		retriedTimes++
		if retriedTimes >= maxRetriedTimes {
			return err
		}
		time.Sleep(waitingTime)
	}

	if err = ri.kafka.CommitMessages(ri.ctx, m); err != nil {
		return errors.New("commit messages fail: " + err.Error())
	}

	return nil
}

func (ri *readerImpl) Close() error {
	return ri.kafka.Close()
}
