package reconcile_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/reconcile"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

func TestReconcile_EnqueuesThenSecondRunIsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := newCountingProvider()
	queue := work.NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if _, err := store.Create(ctx, "web-1", spec); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider, queue)

	first, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobSummary(first.Jobs), []string{"create web-1"}; !slices.Equal(got, want) {
		t.Fatalf("jobs = %v", got)
	}
	if provider.snapshot() != (opCounts{}) {
		t.Fatalf("reconcile called the provider: %+v", provider.snapshot())
	}

	second, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Jobs) != 0 {
		t.Fatalf("second run before the worker = %+v", second.Jobs)
	}
	if queue.Count(work.StatusPending) != 1 {
		t.Fatalf("pending = %d", queue.Count(work.StatusPending))
	}

	worker := work.NewWorker(queue, provider, work.NewLock(), work.DefaultConfig())
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if provider.snapshot() != (opCounts{creates: 1}) {
		t.Fatalf("counts = %+v", provider.snapshot())
	}

	third, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Jobs) != 0 {
		t.Fatalf("third run jobs = %+v", third.Jobs)
	}
	if provider.snapshot() != (opCounts{creates: 1}) {
		t.Fatalf("counts after third run = %+v", provider.snapshot())
	}
}

func TestReconcile_UpdateAndDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	small := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	medium := fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"}
	store := memory.New()
	provider := newCountingProvider()
	queue := work.NewQueue()
	if _, err := provider.Create(ctx, "web-1", small); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "web-1", medium); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider, queue)
	res, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobSummary(res.Jobs), []string{"update web-1"}; !slices.Equal(got, want) {
		t.Fatalf("jobs = %v", got)
	}
	worker := work.NewWorker(queue, provider, work.NewLock(), work.DefaultConfig())
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	list, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Spec != medium {
		t.Fatalf("actual = %+v", list)
	}
	again, err := rec.Reconcile(ctx)
	if err != nil || len(again.Jobs) != 0 {
		t.Fatalf("jobs=%+v err=%v", again.Jobs, err)
	}

	id := mustOne(t, store).ID
	if err := store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	res, err = rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobSummary(res.Jobs), []string{"delete web-1"}; !slices.Equal(got, want) {
		t.Fatalf("jobs = %v", got)
	}
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	list, err = provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("actual after delete = %+v", list)
	}
	if provider.snapshot() != (opCounts{creates: 1, updates: 1, deletes: 1}) {
		t.Fatalf("counts = %+v", provider.snapshot())
	}
	again, err = rec.Reconcile(ctx)
	if err != nil || len(again.Jobs) != 0 {
		t.Fatalf("jobs=%+v err=%v", again.Jobs, err)
	}
}

func TestReconcile_MixedCreateUpdateDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	small := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	medium := fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"}
	store := memory.New()
	provider := fake.New()
	queue := work.NewQueue()
	if _, err := provider.Create(ctx, "keep", small); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Create(ctx, "orphan", small); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "keep", medium); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "new", small); err != nil {
		t.Fatal(err)
	}
	res, err := reconcile.New(store, provider, queue).Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobSummary(res.Jobs), []string{"update keep", "create new", "delete orphan"}; !slices.Equal(got, want) {
		t.Fatalf("jobs = %v, want %v", got, want)
	}
}

func TestReconcile_ConcurrentEnqueueOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := newCountingProvider()
	queue := work.NewQueue()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if _, err := store.Create(ctx, "web-1", spec); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider, queue)
	const n = 8
	var wg sync.WaitGroup
	fresh := 0
	var mu sync.Mutex
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rec.Reconcile(ctx)
			if err != nil {
				t.Errorf("reconcile: %v", err)
				return
			}
			mu.Lock()
			fresh += len(res.Jobs)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if fresh != 1 {
		t.Fatalf("fresh jobs = %d, want 1", fresh)
	}
	if provider.snapshot() != (opCounts{}) {
		t.Fatalf("provider called: %+v", provider.snapshot())
	}
	worker := work.NewWorker(queue, provider, work.NewLock(), work.DefaultConfig())
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if provider.snapshot().creates != 1 {
		t.Fatalf("creates = %d", provider.snapshot().creates)
	}
}

func TestReconcile_DuplicateNamesDoNotEnqueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	queue := work.NewQueue()
	store := listStore{items: []fleet.Desired{{ID: "1", Name: "web"}, {ID: "2", Name: "web"}}}
	_, err := reconcile.New(store, fake.New(), queue).Reconcile(ctx)
	if err == nil || err.Error() != `duplicate desired instance name "web"` {
		t.Fatalf("err = %v", err)
	}
	if queue.Count(work.StatusPending) != 0 {
		t.Fatal("enqueued a job")
	}

	provider := listProvider{items: []fleet.Actual{{ID: "vm_1", Name: "web"}, {ID: "vm_2", Name: "web"}}}
	_, err = reconcile.New(memory.New(), provider, queue).Reconcile(ctx)
	if err == nil || err.Error() != `duplicate provider instance name "web"` {
		t.Fatalf("err = %v", err)
	}
}

func TestReconcile_EmptyIsNoop(t *testing.T) {
	t.Parallel()
	res, err := reconcile.New(memory.New(), fake.New(), work.NewQueue()).Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Jobs) != 0 {
		t.Fatalf("jobs = %+v", res.Jobs)
	}
}

func TestReconcile_CancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := reconcile.New(memory.New(), fake.New(), work.NewQueue()).Reconcile(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func mustOne(t *testing.T, store *memory.Store) fleet.Desired {
	t.Helper()
	list, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("rows = %d", len(list))
	}
	return list[0]
}

func jobSummary(jobs []work.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job.Op+" "+job.Name)
	}
	return out
}

type opCounts struct {
	creates int
	updates int
	deletes int
}

type countingProvider struct {
	inner   *fake.Provider
	mu      sync.Mutex
	creates int
	updates int
	deletes int
}

func newCountingProvider() *countingProvider {
	return &countingProvider{inner: fake.New()}
}

func (p *countingProvider) snapshot() opCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return opCounts{creates: p.creates, updates: p.updates, deletes: p.deletes}
}

func (p *countingProvider) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Actual, error) {
	created, err := p.inner.Create(ctx, name, spec)
	if err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	p.creates++
	p.mu.Unlock()
	return created, nil
}

func (p *countingProvider) Update(ctx context.Context, id string, spec fleet.Spec) (fleet.Actual, error) {
	updated, err := p.inner.Update(ctx, id, spec)
	if err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	p.updates++
	p.mu.Unlock()
	return updated, nil
}

func (p *countingProvider) Delete(ctx context.Context, id string) error {
	if err := p.inner.Delete(ctx, id); err != nil {
		return err
	}
	p.mu.Lock()
	p.deletes++
	p.mu.Unlock()
	return nil
}

func (p *countingProvider) List(ctx context.Context) ([]fleet.Actual, error) {
	return p.inner.List(ctx)
}

type listStore struct {
	items []fleet.Desired
}

func (s listStore) Create(context.Context, string, fleet.Spec) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected create")
}
func (s listStore) Get(context.Context, string) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected get")
}
func (s listStore) List(context.Context) ([]fleet.Desired, error) { return s.items, nil }
func (s listStore) UpdateSpec(context.Context, string, fleet.Spec) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected update")
}
func (s listStore) Delete(context.Context, string) error { return errors.New("unexpected delete") }
func (s listStore) Ping(context.Context) error           { return nil }
func (s listStore) RecordHealth(context.Context, string, string, string, time.Time) (fleet.HealthCheck, error) {
	return fleet.HealthCheck{}, errors.New("unexpected health")
}
func (s listStore) LatestHealth(context.Context, string) (fleet.HealthCheck, error) {
	return fleet.HealthCheck{}, errors.New("unexpected health")
}
func (s listStore) CreateRollout(context.Context, fleet.Spec, []string, time.Time) (fleet.Rollout, error) {
	return fleet.Rollout{}, errors.New("unexpected rollout")
}
func (s listStore) GetRollout(context.Context, string) (fleet.Rollout, error) {
	return fleet.Rollout{}, errors.New("unexpected rollout")
}
func (s listStore) SaveRollout(context.Context, fleet.Rollout) error {
	return errors.New("unexpected rollout")
}
func (s listStore) ListRunningRollouts(context.Context) ([]fleet.Rollout, error) {
	return nil, errors.New("unexpected rollout")
}

type listProvider struct {
	items []fleet.Actual
}

func (p listProvider) Create(context.Context, string, fleet.Spec) (fleet.Actual, error) {
	return fleet.Actual{}, errors.New("unexpected create")
}
func (p listProvider) Update(context.Context, string, fleet.Spec) (fleet.Actual, error) {
	return fleet.Actual{}, errors.New("unexpected update")
}
func (p listProvider) Delete(context.Context, string) error { return errors.New("unexpected delete") }
func (p listProvider) List(context.Context) ([]fleet.Actual, error) {
	return p.items, nil
}
