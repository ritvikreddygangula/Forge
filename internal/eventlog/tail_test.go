//go:build integration

package eventlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
)

// TestKafka_Tail_ReceivesNewEventsAfterReadAll proves Tail picks up exactly
// where ReadAll left off — no gap (missing the event published right after
// ReadAll returns) and no overlap (re-delivering the event ReadAll already
// returned).
func TestKafka_Tail_ReceivesNewEventsAfterReadAll(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := testTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}
	producer := eventlog.NewKafkaProducer(brokers, topic)
	defer func() { _ = producer.Close() }()

	// Publish one event, ReadAll it (simulating startup replay), then start
	// tailing from the returned offset, then publish a second event and
	// confirm Tail picks it up live.
	if err := producer.Publish(ctx, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "before-tail", Timestamp: time.Now()}); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	events, offset, err := eventlog.NewKafkaConsumer(brokers, topic).ReadAll(ctx)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(events) != 1 || offset != 1 {
		t.Fatalf("expected 1 event and offset 1, got %d events offset %d", len(events), offset)
	}

	received := make(chan eventlog.Event, 1)
	tailCtx, tailCancel := context.WithCancel(ctx)
	defer tailCancel()
	go func() {
		_ = eventlog.NewKafkaConsumer(brokers, topic).Tail(tailCtx, offset, func(e eventlog.Event) error {
			received <- e
			return nil
		})
	}()

	// Give Tail a moment to establish its reader before publishing, then
	// publish the event Tail should pick up live.
	time.Sleep(200 * time.Millisecond)
	if err := producer.Publish(ctx, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "after-tail", Timestamp: time.Now()}); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	select {
	case e := <-received:
		if e.JobID != "after-tail" {
			t.Fatalf("expected to tail job 'after-tail', got %q", e.JobID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Tail to deliver the new event")
	}
}
