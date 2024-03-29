package kafka

import (
	"context"
)

type CommonKafka interface {
	GetKafkaWriter() Writer
	Close()
	SetLog(Logger)
}

func NewCommonKafka(ctx context.Context, di ConfigDI, topic string) CommonKafka {
	return &commonKafkaImpl{
		ctx:      ctx,
		ConfigDI: di,
		topic:    topic,
	}
}

type commonKafkaImpl struct {
	ctx context.Context
	ConfigDI
	kafkaWriter Writer
	topic       string
	log         Logger
}

func (s *commonKafkaImpl) SetLog(l Logger) {
	s.log = l
}
func (s *commonKafkaImpl) GetKafkaWriter() Writer {
	if s.kafkaWriter != nil {
		return s.kafkaWriter
	}
	s.kafkaWriter = s.NewKafkaWriter(s.ctx, s.topic)
	if s.log != nil {
		s.kafkaWriter.SetLog(s.log)
	}
	return s.kafkaWriter
}

func (s *commonKafkaImpl) Close() {
	if s.kafkaWriter != nil {
		s.kafkaWriter.Close()
	}
}
