package eventlog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const Topic = "job-events"

type Producer interface {
	Publish(ctx context.Context, e Event) error
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    Topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(e.JobID), Value: data}); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() error { return p.writer.Close() }
