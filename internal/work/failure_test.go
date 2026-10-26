package work

import (
	"context"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
)

func TestFailureInjection_RetriesWithExistingBackoffThenSucceedsOrDies(t *testing.T) {
	t.Parallel()
	t.Run("succeeds after injected failures", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		start := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
		now := start
		clock := func() time.Time { return now }
		spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
		provider := fake.New()
		provider.FailNext(2)
		q := NewQueue()
		cfg := Config{
			MaxAttempts: DefaultMaxAttempts,
			BaseDelay:   DefaultBaseDelay,
			MaxDelay:    DefaultMaxDelay,
			Poll:        DefaultPoll,
			Now:         clock,
		}
		worker := NewWorker(q, provider, NewLock(), cfg)
		if _, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: start, CreatedAt: start}); err != nil {
			t.Fatal(err)
		}

		if _, err := worker.Once(ctx); err != nil {
			t.Fatal(err)
		}
		job := onlyJob(t, q)
		firstWait := Backoff(1, DefaultBaseDelay, DefaultMaxDelay)
		if job.Status != StatusPending || job.Attempts != 1 || job.LastError != "injected provider failure" || !job.RunAfter.Equal(start.Add(firstWait)) {
			t.Fatalf("after first failure %+v", job)
		}
		if _, ok, err := q.Claim(ctx, job.RunAfter.Add(-time.Millisecond)); err != nil || ok {
			t.Fatalf("claimed during backoff ok=%v err=%v", ok, err)
		}

		now = job.RunAfter
		if _, err := worker.Once(ctx); err != nil {
			t.Fatal(err)
		}
		job = onlyJob(t, q)
		secondWait := Backoff(2, DefaultBaseDelay, DefaultMaxDelay)
		if job.Attempts != 2 || !job.RunAfter.Equal(now.Add(secondWait)) {
			t.Fatalf("after second failure %+v", job)
		}

		now = job.RunAfter
		if _, err := worker.Once(ctx); err != nil {
			t.Fatal(err)
		}
		job = onlyJob(t, q)
		list, err := provider.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != StatusSucceeded || len(list) != 1 || list[0].Name != "web-1" {
			t.Fatalf("job=%+v actual=%+v", job, list)
		}
		if firstWait != 100*time.Millisecond || secondWait != 200*time.Millisecond {
			t.Fatalf("backoff = %s then %s", firstWait, secondWait)
		}
	})

	t.Run("dies on the fifth failure", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
		provider := fake.New()
		provider.FailNext(10)
		q := NewQueue()
		worker := NewWorker(q, provider, NewLock(), Config{
			MaxAttempts: DefaultMaxAttempts,
			BaseDelay:   DefaultBaseDelay,
			MaxDelay:    DefaultMaxDelay,
			Poll:        DefaultPoll,
			Now:         clock,
		})
		if _, _, err := q.Enqueue(ctx, Job{Name: "web-2", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
		var job Job
		for range DefaultMaxAttempts {
			job = onlyJob(t, q)
			if job.Status == StatusDead {
				break
			}
			if job.Status == StatusPending && job.RunAfter.After(now) {
				now = job.RunAfter
			}
			if _, err := worker.Once(ctx); err != nil {
				t.Fatal(err)
			}
		}
		job = onlyJob(t, q)
		if job.Status != StatusDead || job.Attempts != DefaultMaxAttempts {
			t.Fatalf("dead job = %+v", job)
		}
		if _, ok, err := q.Claim(ctx, now.Add(time.Hour)); err != nil || ok {
			t.Fatalf("dead job was claimed ok=%v err=%v", ok, err)
		}
		list, err := provider.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 0 {
			t.Fatalf("actual = %+v", list)
		}
	})
}
