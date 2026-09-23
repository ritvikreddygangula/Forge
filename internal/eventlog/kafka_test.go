//go:build integration

package eventlog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// testTopic returns a unique topic name per test, so runs never see events
// left behind by other tests or prior runs against a long-lived dev broker.
func testTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("job-events-test-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestKafka_PublishAndReadAll_RoundTrip(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := testTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}

	producer := eventlog.NewKafkaProducer(brokers, topic)
	defer func() { _ = producer.Close() }()

	jobID := "kafka-roundtrip-job"
	if err := producer.Publish(ctx, eventlog.Event{
		Type: eventlog.EventJobCreated, JobID: jobID, Image: "alpine",
		Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	events, _, err := eventlog.NewKafkaConsumer(brokers, topic).ReadAll(ctx)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(events) != 1 || events[0].JobID != jobID || events[0].Type != eventlog.EventJobCreated {
		t.Fatalf("expected exactly one job_created event for %s, got %+v", jobID, events)
	}

	jobs := eventlog.Rebuild(events)
	if len(jobs) != 1 || jobs[0].ID != jobID || jobs[0].Status != job.StatusQueued {
		t.Fatalf("expected rebuilt job %s in queued state, got %+v", jobID, jobs)
	}
}
