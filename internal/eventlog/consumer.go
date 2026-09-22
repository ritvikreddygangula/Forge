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
	ReadAll(ctx context.Context) ([]Event, int64, error)
}

type KafkaConsumer struct {
	brokers []string
	topic   string
}

func NewKafkaConsumer(brokers []string, topic string) *KafkaConsumer {
	return &KafkaConsumer{brokers: brokers, topic: topic}
}

// EnsureTopic creates the given topic if it doesn't exist yet. Verified
// idempotent against a real broker — safe to call on every startup.
//
// Every step here retries via retryTransient rather than failing on the
// first hiccup. Confirmed necessary, not defensive-for-its-own-sake: 3
// coordinator replicas starting within milliseconds of each other against a
// real single-shard Redpanda broker reliably hit transient connection
// errors (io.ErrNoProgress — kafka-go's own signal that it considers a
// connection corrupted and needs a fresh one) on this very first dial. A
// single resource-constrained dev broker serving a burst of near-
// simultaneous startups is exactly the situation a real multi-replica
// coordinator faces.
func EnsureTopic(ctx context.Context, brokers []string, topic string) error {
	if err := retryTransient(ctx, func() error {
		return createTopicOnce(ctx, brokers, topic)
	}); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	// CreateTopics returning doesn't mean the topic is immediately visible to
	// every connection yet — a fresh topic can briefly answer produce/fetch
	// requests with "Unknown Topic Or Partition" while metadata propagates,
	// even on a single-node broker. Retrying the dial until the topic's
	// leader is actually discoverable means callers never race this.
	return retryTransient(ctx, func() error {
		conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
		if err != nil {
			return err
		}
		return conn.Close()
	})
}

func createTopicOnce(ctx context.Context, brokers []string, topic string) error {
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
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

// ReadAll reads every event currently in the topic, from the beginning up to
// the offset at the moment this call started, and returns that offset too —
// callers that need to keep following the log (Tail, below) resume from
// exactly there, with no gap or overlap. A bounded replay, not a live
// subscription; returns an empty slice on a topic with nothing published yet.
func (c *KafkaConsumer) ReadAll(ctx context.Context) ([]Event, int64, error) {
	// Dialing the leader and reading its last offset are retried together as
	// one unit, not just the dial — a connection that dials fine can still
	// hit "unknown topic" on the read immediately after, during the same
	// propagation window EnsureTopic's own retry guards against elsewhere.
	var lastOffset int64
	err := retryTransient(ctx, func() error {
		conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], c.topic, 0)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		lastOffset, err = conn.ReadLastOffset()
		return err
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read last offset: %w", err)
	}
	if lastOffset == 0 {
		return nil, 0, nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.brokers,
		Topic:     c.topic,
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
		return nil, 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	events := make([]Event, 0, lastOffset)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("failed reading event log: %w", err)
		}
		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return nil, 0, fmt.Errorf("failed to decode event at offset %d: %w", msg.Offset, err)
		}
		events = append(events, e)
		if msg.Offset+1 >= lastOffset {
			break
		}
	}
	return events, lastOffset, nil
}

// Tail continuously reads events starting at fromOffset, invoking onEvent
// for each one, until ctx is cancelled or onEvent returns an error. Unlike
// ReadAll (a bounded, one-shot replay), this runs indefinitely — every
// replica (leader and followers alike) runs one of these for the lifetime
// of the process to stay in sync with writes published by whichever replica
// is currently leader.
//
// A transient read/connection error (e.g. kafka-go's Reader closing its own
// connection after an io.ErrNoProgress — its documented response to what it
// considers a corrupted connection, meant to be recovered by reconnecting,
// not surfaced as fatal) reconnects and resumes from the last successfully
// processed offset instead of returning. A tailer meant to run for a
// process's entire lifetime has to outlast brief broker/connection blips.
func (c *KafkaConsumer) Tail(ctx context.Context, fromOffset int64, onEvent func(Event) error) error {
	offset := fromOffset
	for {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   c.brokers,
			Topic:     c.topic,
			Partition: 0,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   250 * time.Millisecond,
		})
		if err := reader.SetOffset(offset); err != nil {
			_ = reader.Close()
			return fmt.Errorf("failed to seek to offset %d: %w", offset, err)
		}

		done, nextOffset, err := tailOnce(ctx, reader, offset, onEvent)
		_ = reader.Close()
		offset = nextOffset
		if done {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// tailOnce reads from an already-positioned reader until ctx is cancelled,
// onEvent returns an error, a message fails to decode, or the read itself
// fails. done is true for the first three (permanent stop, err is what Tail
// should return) and false for the last (transient, Tail reconnects and
// resumes from nextOffset).
func tailOnce(ctx context.Context, reader *kafka.Reader, offset int64, onEvent func(Event) error) (done bool, nextOffset int64, err error) {
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return true, offset, nil // context cancelled — clean shutdown
			}
			return false, offset, nil // transient — caller reconnects
		}
		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return true, offset, fmt.Errorf("failed to decode event at offset %d: %w", msg.Offset, err)
		}
		if err := onEvent(e); err != nil {
			return true, offset, err
		}
		offset = msg.Offset + 1
	}
}
