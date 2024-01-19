package kafka

import (
	"context"
)

type ConfigDI interface {
	NewKafkaWriter(ctx context.Context, topic string) Writer
	NewKafkaReader(ctx context.Context, groupID, topic string) Reader
}

type KafkaConfig struct {
	Brokers []string
}
