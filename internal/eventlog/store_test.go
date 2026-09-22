package eventlog_test

import (
	"context"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

type fakeProducer struct {
	events []eventlog.Event
}

func (f *fakeProducer) Publish(ctx context.Context, e eventlog.Event) error {
	f.events = append(f.events, e)
	return nil
}

func TestEventlogStore_Create_PublishesJobCreated(t *testing.T) {
	fake := &fakeProducer{}
	s := eventlog.NewStore(job.NewMemoryStore(), fake)

	created, err := s.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.events) != 1 || fake.events[0].Type != eventlog.EventJobCreated {
		t.Fatalf("expected one job_created event, got %+v", fake.events)
	}
	if fake.events[0].JobID != created.ID {
		t.Fatalf("expected event job id %s, got %s", created.ID, fake.events[0].JobID)
	}
}

func TestEventlogStore_ClaimNext_PublishesJobClaimed_OnlyWhenClaimed(t *testing.T) {
	fake := &fakeProducer{}
	base := job.NewMemoryStore()
	s := eventlog.NewStore(base, fake)

	if _, err := s.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if len(fake.events) != 0 {
		t.Fatalf("expected no events on empty queue, got %+v", fake.events)
	}

	created, err := base.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed.ID != created.ID {
		t.Fatalf("expected to claim %s, got %s", created.ID, claimed.ID)
	}
	if len(fake.events) != 1 || fake.events[0].Type != eventlog.EventJobClaimed {
		t.Fatalf("expected one job_claimed event, got %+v", fake.events)
	}
}

func TestEventlogStore_Complete_PublishesJobCompleted(t *testing.T) {
	fake := &fakeProducer{}
	base := job.NewMemoryStore()
	s := eventlog.NewStore(base, fake)
	created, err := base.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := base.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	if err := s.Complete(created.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if len(fake.events) != 1 || fake.events[0].Type != eventlog.EventJobCompleted {
		t.Fatalf("expected one job_completed event, got %+v", fake.events)
	}
	if fake.events[0].Stdout != "ok\n" {
		t.Fatalf("expected stdout ok in event, got %q", fake.events[0].Stdout)
	}
}

func TestEventlogStore_Get_PublishesNothing(t *testing.T) {
	fake := &fakeProducer{}
	base := job.NewMemoryStore()
	s := eventlog.NewStore(base, fake)
	created, err := base.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if _, err := s.Get(created.ID); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if len(fake.events) != 0 {
		t.Fatalf("expected Get to publish nothing, got %+v", fake.events)
	}
}
