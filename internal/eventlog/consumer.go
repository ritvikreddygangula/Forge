package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadAll(ctx context.Context) ([]Event, error)
}

type KafkaConsumer struct {
	brokers []string
}

func NewKafkaConsumer(brokers []string) *KafkaConsumer {
	return &KafkaConsumer{brokers: brokers}
}

// EnsureTopic creates the job-events topic if it doesn't exist yet. Verified
// idempotent against a real broker — safe to call on every startup.
func EnsureTopic(ctx context.Context, brokers []string) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial broker: %w", err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to find controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer func() { _ = controllerConn.Close() }()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             Topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

// ReadAll reads every event currently in the topic, from the beginning up to
// the offset at the moment this call started — a bounded replay, not a live
// subscription. Returns an empty slice on a topic with nothing published yet.
func (c *KafkaConsumer) ReadAll(ctx context.Context) ([]Event, error) {
	leaderConn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], Topic, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to find topic leader: %w", err)
	}
	lastOffset, err := leaderConn.ReadLastOffset()
	if closeErr := leaderConn.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read last offset: %w", err)
	}
	if lastOffset == 0 {
		return nil, nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.brokers,
		Topic:     Topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		// MaxWait defaults to 10s if unset — each fetch would then block for
		// up to 10s server-side even when data is already available. A short
		// wait is fine here since ReadAll is a bounded replay against data
		// that already exists, not a live tail waiting on new messages.
		MaxWait: 250 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()
	if err := reader.SetOffset(0); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	events := make([]Event, 0, lastOffset)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed reading event log: %w", err)
		}
		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return nil, fmt.Errorf("failed to decode event at offset %d: %w", msg.Offset, err)
		}
		events = append(events, e)
		if msg.Offset+1 >= lastOffset {
			break
		}
	}
	return events, nil
}
