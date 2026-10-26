package work

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

func TestQueue_OneActiveJobPerInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	first, fresh, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec})
	if err != nil || !fresh {
		t.Fatalf("fresh=%v err=%v", fresh, err)
	}
	again, fresh, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec})
	if err != nil || fresh || again.ID != first.ID {
		t.Fatalf("again fresh=%v id=%s err=%v", fresh, again.ID, err)
	}

	next := fleet.Spec{Image: "debian-12", Size: "small", Region: "local"}
	updated, fresh, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: next})
	if err != nil || !fresh || updated.ID != first.ID || updated.Spec != next || updated.Attempts != 0 {
		t.Fatalf("updated=%+v fresh=%v err=%v", updated, fresh, err)
	}
}

func TestQueue_RunningJobIsNotReplaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	q := NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	job, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.Claim(ctx, now); err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	other := fleet.Spec{Image: "debian-12", Size: "small", Region: "local"}
	got, fresh, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpUpdate, Spec: other})
	if err != nil || fresh || got.ID != job.ID || got.Op != fleet.OpCreate {
		t.Fatalf("got=%+v fresh=%v err=%v", got, fresh, err)
	}
}

func TestQueue_ClaimIsExclusiveAndReclaimsStaleRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	q := NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if _, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	const n = 8
	okCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := q.Claim(ctx, now)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if ok {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if okCount != 1 {
		t.Fatalf("claims = %d, want 1", okCount)
	}
	if _, ok, err := q.Claim(ctx, now.Add(time.Second)); err != nil || ok {
		t.Fatalf("fresh running job was claimed again ok=%v err=%v", ok, err)
	}

	staleAt := now.Add(StaleAfter + time.Second)
	job, ok, err := q.Claim(ctx, staleAt)
	if err != nil || !ok {
		t.Fatalf("stale claim ok=%v err=%v", ok, err)
	}
	if job.Status != StatusRunning || job.Attempts != 0 {
		t.Fatalf("reclaimed = %+v", job)
	}
}

func TestQueue_RetryDoesNotClaimBeforeRunAfter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	q := NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	job, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.Claim(ctx, now); err != nil || !ok {
		t.Fatal(err)
	}
	runAfter := now.Add(200 * time.Millisecond)
	if err := q.Retry(ctx, job.ID, now, runAfter, 2, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.Claim(ctx, runAfter.Add(-time.Millisecond)); err != nil || ok {
		t.Fatalf("claimed early ok=%v err=%v", ok, err)
	}
	got, ok, err := q.Claim(ctx, runAfter)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if got.Attempts != 2 || got.LastError != "boom" {
		t.Fatalf("got = %+v", got)
	}
}
