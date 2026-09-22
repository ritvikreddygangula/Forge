package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			// BatchTimeout defaults to 1s — Publish is called once per job
			// mutation (not high-throughput batching), so waiting up to a
			// full second per call for the batch timer to fire would make
			// every Create/ClaimNext/Complete needlessly slow.
			BatchTimeout: 10 * time.Millisecond,
			// Transport.MetadataTTL defaults to 15s (the historical
			// WriterConfig.RebalanceInterval default carried over). A Writer
			// created right after EnsureTopic creates a brand new topic can
			// cache a stale "topic doesn't exist" answer from before creation
			// fully propagated — the retry loop in Publish is useless against
			// that without a short TTL to actually pick up refreshed metadata
			// within the retry window.
			Transport: &kafka.Transport{MetadataTTL: 250 * time.Millisecond},
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	msg := kafka.Message{Key: []byte(e.JobID), Value: data}

	// A topic that was just created (EnsureTopic) can briefly answer produce
	// requests with UnknownTopicOrPartition while creation propagates through
	// the broker, even after EnsureTopic itself confirmed the topic is
	// dial-able — that confirmation and the write path apparently don't share
	// the same readiness signal. Retry a few times rather than fail the
	// caller's first-ever publish to a brand new topic.
	var writeErr error
	for attempt := 0; attempt < 10; attempt++ {
		writeErr = p.writer.WriteMessages(ctx, msg)
		if writeErr == nil || !errors.Is(writeErr, kafka.UnknownTopicOrPartition) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to publish event: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if writeErr != nil {
		return fmt.Errorf("failed to publish event: %w", writeErr)
	}
	return nil
}

func (p *KafkaProducer) Close() error { return p.writer.Close() }
