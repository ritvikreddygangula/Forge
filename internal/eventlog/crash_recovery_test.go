//go:build integration

package eventlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// TestCrashRecovery_RebuildsStateFromLog simulates a coordinator crash and
// restart: a fresh eventlog.Store drives one job through its full lifecycle
// and leaves a second job queued, then a brand new job.MemoryStore is built
// purely by replaying the same Redpanda topic — proving the log, not memory,
// is the durable source of truth.
func TestCrashRecovery_RebuildsStateFromLog(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := testTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}

	// "Before the crash": drive one job to completion, leave a second queued.
	producer := eventlog.NewKafkaProducer(brokers, topic)
	beforeCrash := eventlog.NewStore(job.NewMemoryStore(), producer)

	completed, err := beforeCrash.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := beforeCrash.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := beforeCrash.Complete(completed.ID, job.StatusSucceeded, "done\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	stillQueued, err := beforeCrash.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("producer.Close returned error: %v", err)
	}

	// "The crash": beforeCrash and its in-memory MemoryStore are simply
	// dropped here — nothing more is done with them. Everything below is
	// built fresh, touching only the Redpanda topic.

	// "The restart": rebuild purely from the log.
	events, _, err := eventlog.NewKafkaConsumer(brokers, topic).ReadAll(ctx)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	rebuilt := job.NewMemoryStore()
	rebuilt.Rebuild(eventlog.Rebuild(events))
	restartProducer := eventlog.NewKafkaProducer(brokers, topic)
	defer func() { _ = restartProducer.Close() }()
	afterCrash := eventlog.NewStore(rebuilt, restartProducer)

	got, err := afterCrash.Get(completed.ID)
	if err != nil {
		t.Fatalf("Get(completed) returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "done\n" {
		t.Fatalf("expected completed job to survive restart as succeeded with stdout done, got %+v", got)
	}

	got, err = afterCrash.Get(stillQueued.ID)
	if err != nil {
		t.Fatalf("Get(stillQueued) returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected still-queued job to survive restart as queued, got %+v", got)
	}

	// And it's genuinely still claimable post-restart, not just readable.
	claimed, err := afterCrash.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != stillQueued.ID {
		t.Fatalf("expected to claim the rebuilt queued job, got %+v", claimed)
	}
}
