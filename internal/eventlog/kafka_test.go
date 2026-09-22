//go:build integration

package eventlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestKafka_PublishAndReadAll_RoundTrip(t *testing.T) {
	brokers := []string{"localhost:9092"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}

	producer := eventlog.NewKafkaProducer(brokers)
	defer func() { _ = producer.Close() }()

	jobID := "kafka-roundtrip-" + time.Now().Format(time.RFC3339Nano)
	if err := producer.Publish(ctx, eventlog.Event{
		Type: eventlog.EventJobCreated, JobID: jobID, Image: "alpine",
		Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	events, err := eventlog.NewKafkaConsumer(brokers).ReadAll(ctx)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}

	var found bool
	for _, e := range events {
		if e.JobID == jobID && e.Type == eventlog.EventJobCreated {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find published event for job %s in %d replayed events", jobID, len(events))
	}

	jobs := eventlog.Rebuild(events)
	var rebuiltFound bool
	for _, j := range jobs {
		if j.ID == jobID && j.Status == job.StatusQueued {
			rebuiltFound = true
		}
	}
	if !rebuiltFound {
		t.Fatalf("expected rebuilt job %s in queued state", jobID)
	}
}
