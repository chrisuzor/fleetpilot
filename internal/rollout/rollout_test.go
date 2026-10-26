package rollout_test

import (
	"context"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/rollout"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

func TestRollout_UpdatesOneInstanceAtATimeAndStopsOnFailedHealth(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := fake.New()
	queue := work.NewQueue()
	oldSpec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	newSpec := fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"}
	for _, name := range []string{"web-1", "web-2", "web-3"} {
		if _, err := store.Create(ctx, name, oldSpec); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Create(ctx, name, oldSpec); err != nil {
			t.Fatal(err)
		}
	}
	provider.SetUnhealthy("web-2", "injected failure")
	worker := work.NewWorker(queue, provider, work.NewLock(), work.DefaultConfig())
	roller := rollout.New(store, provider, queue, provider)

	roll, err := roller.Start(ctx, newSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roller.Start(ctx, newSpec); err == nil {
		t.Fatal("second running rollout was accepted")
	}

	roll, err = roller.Step(ctx, roll.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := specsAt(t, ctx, store, provider, "medium", "small", "small", "small", "small", "small"); err != nil {
		t.Fatal(err)
	}

	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	roll, err = roller.Step(ctx, roll.ID)
	if err != nil {
		t.Fatal(err)
	}
	if roll.Status != fleet.RolloutRunning || roll.Next != 1 {
		t.Fatalf("after web-1 = %+v", roll)
	}
	if err := specsAt(t, ctx, store, provider, "medium", "small", "small", "medium", "small", "small"); err != nil {
		t.Fatal(err)
	}
	web1, err := healthOf(ctx, store, "web-1")
	if err != nil || web1.Status != fleet.HealthHealthy {
		t.Fatalf("web-1 health = %+v err %v", web1, err)
	}

	roll, err = roller.Step(ctx, roll.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	roll, err = roller.Step(ctx, roll.ID)
	if err != nil {
		t.Fatal(err)
	}
	if roll.Status != fleet.RolloutPaused || roll.PauseReason != fleet.PauseHealthFailed || roll.Next != 1 {
		t.Fatalf("paused = %+v", roll)
	}
	if err := specsAt(t, ctx, store, provider, "medium", "medium", "small", "medium", "medium", "small"); err != nil {
		t.Fatal(err)
	}
	web2, err := healthOf(ctx, store, "web-2")
	if err != nil || web2.Status != fleet.HealthUnhealthy || web2.Detail != "injected failure" {
		t.Fatalf("web-2 health = %+v err %v", web2, err)
	}
	if _, err := healthOf(ctx, store, "web-3"); err == nil {
		t.Fatal("web-3 has a health check")
	}

	roll, err = roller.Step(ctx, roll.ID)
	if err != nil {
		t.Fatal(err)
	}
	if roll.Status != fleet.RolloutPaused {
		t.Fatalf("step after pause = %+v", roll)
	}
	if err := specsAt(t, ctx, store, provider, "medium", "medium", "small", "medium", "medium", "small"); err != nil {
		t.Fatal(err)
	}
}

func TestRollout_DeadJobStopsBeforeTheNextInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := fake.New()
	queue := work.NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	for _, name := range []string{"web-1", "web-2"} {
		if _, err := store.Create(ctx, name, spec); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Create(ctx, name, spec); err != nil {
			t.Fatal(err)
		}
	}
	provider.FailNext(20)
	cfg := work.DefaultConfig()
	cfg.BaseDelay = time.Millisecond
	cfg.MaxDelay = time.Millisecond
	cfg.Poll = time.Millisecond
	worker := work.NewWorker(queue, provider, work.NewLock(), cfg)
	roller := rollout.New(store, provider, queue, provider)
	roll, err := roller.Start(ctx, fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"})
	if err != nil {
		t.Fatal(err)
	}
	for range 30 {
		roll, err = roller.Step(ctx, roll.ID)
		if err != nil {
			t.Fatal(err)
		}
		if roll.Status != fleet.RolloutRunning {
			break
		}
		if _, err := worker.Once(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if roll.Status != fleet.RolloutPaused || roll.PauseReason != fleet.PauseJobDead {
		t.Fatalf("rollout = %+v", roll)
	}
	if err := specsAt(t, ctx, store, provider, "medium", "small", "small", "small"); err != nil {
		t.Fatal(err)
	}
}

func specsAt(t *testing.T, ctx context.Context, store *memory.Store, provider *fake.Provider, sizes ...string) error {
	t.Helper()
	desired, err := store.List(ctx)
	if err != nil {
		return err
	}
	actual, err := provider.List(ctx)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(desired)+len(actual))
	for _, item := range desired {
		got = append(got, item.Spec.Size)
	}
	for _, item := range actual {
		got = append(got, item.Spec.Size)
	}
	if len(got) != len(sizes) {
		t.Fatalf("sizes = %v, want %v", got, sizes)
	}
	for i := range sizes {
		if got[i] != sizes[i] {
			t.Fatalf("sizes = %v, want %v", got, sizes)
		}
	}
	return nil
}

func healthOf(ctx context.Context, store *memory.Store, name string) (fleet.HealthCheck, error) {
	desired, err := store.List(ctx)
	if err != nil {
		return fleet.HealthCheck{}, err
	}
	for _, item := range desired {
		if item.Name == name {
			return store.LatestHealth(ctx, item.ID)
		}
	}
	return fleet.HealthCheck{}, fleet.ErrNotFound
}
