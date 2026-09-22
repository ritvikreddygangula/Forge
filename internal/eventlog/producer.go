package eventlog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// DefaultTopic is the topic the real coordinator binary publishes to and
// replays from. Tests take an explicit topic instead of this constant so
// each test run can use its own unique topic — sharing one topic across
// test runs (or across a long-lived dev broker) leaves queued-but-never-
// claimed jobs behind that pollute later runs' FIFO ordering.
const DefaultTopic = "job-events"

type Producer interface {
	Publish(ctx context.Context, e Event) error
}

// KafkaProducer publishes by dialing the topic leader fresh on every call
// (the same pattern ReadAll and EnsureTopic already use) rather than holding
// a long-lived kafka.Writer. Deliberate: a pooled Writer's internal
// per-partition connection reuse was observed to get stuck after a single
// transient failure (surfaced as repeated "i/o timeout" even across many
// retries against the same Writer instance, under concurrent load from
// multiple coordinator replicas) — dialing fresh each time trades a little
// per-publish connection overhead, acceptable at this project's job-mutation
// frequency, for never being able to reuse a bad connection.
type KafkaProducer struct {
	brokers []string
	topic   string
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{brokers: brokers, topic: topic}
}

func (p *KafkaProducer) Publish(ctx context.Context, e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	msg := kafka.Message{Key: []byte(e.JobID), Value: data}

	if err := retryTransient(ctx, func() error {
		conn, err := kafka.DialLeader(ctx, "tcp", p.brokers[0], p.topic, 0)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		_, err = conn.WriteMessages(msg)
		return err
	}); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() error { return nil }
