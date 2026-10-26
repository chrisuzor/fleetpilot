package work

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
)

func TestWorker_ReplayDoesNotDoubleCreateOrDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	q := NewQueue()
	provider := newCountingProvider()
	worker := NewWorker(q, provider, NewLock(), fixedClock(now))

	created, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	worker.finish(ctx, created)
	if provider.snapshot() != (opCounts{creates: 1}) {
		t.Fatalf("after replay create counts = %+v", provider.snapshot())
	}

	if _, err := provider.Create(ctx, "gone", spec); err != nil {
		t.Fatal(err)
	}
	deleted, _, err := q.Enqueue(ctx, Job{Name: "gone", Op: fleet.OpDelete, RunAfter: now, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	before := provider.snapshot()
	worker.finish(ctx, deleted)
	if provider.snapshot() != before {
		t.Fatalf("delete replay changed counts from %+v to %+v", before, provider.snapshot())
	}
	list, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "web-1" {
		t.Fatalf("actual = %+v", list)
	}
}

func TestWorker_CreateOfExistingNameDoesNotCallCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	provider := newCountingProvider()
	if _, err := provider.Create(ctx, "web-1", spec); err != nil {
		t.Fatal(err)
	}
	q := NewQueue()
	if _, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	before := provider.snapshot()
	worker := NewWorker(q, provider, NewLock(), fixedClock(now))
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if provider.snapshot() != before {
		t.Fatalf("counts = %+v, want %+v", provider.snapshot(), before)
	}
}

func TestWorker_RetriesWithExponentialBackoffThenDies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	start := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	now := start
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	q := NewQueue()
	provider := newCountingProvider()
	provider.failCreates = 2
	cfg := Config{
		MaxInFlight: 1,
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
		Poll:        time.Millisecond,
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
	if job.Status != StatusPending || job.Attempts != 1 || job.LastError == "" {
		t.Fatalf("after first failure %+v", job)
	}
	if !job.RunAfter.Equal(start.Add(100 * time.Millisecond)) {
		t.Fatalf("run_after = %s", job.RunAfter)
	}
	if _, ok, err := q.Claim(ctx, start.Add(99*time.Millisecond)); err != nil || ok {
		t.Fatalf("claimed during backoff ok=%v err=%v", ok, err)
	}

	advance(100 * time.Millisecond)
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	job = onlyJob(t, q)
	if job.Attempts != 2 || !job.RunAfter.Equal(clock().Add(200*time.Millisecond)) {
		t.Fatalf("after second failure %+v now=%s", job, clock())
	}

	advance(200 * time.Millisecond)
	if _, err := worker.Once(ctx); err != nil {
		t.Fatal(err)
	}
	job = onlyJob(t, q)
	if job.Status != StatusSucceeded || provider.snapshot().creates != 1 {
		t.Fatalf("job=%+v counts=%+v", job, provider.snapshot())
	}

	provider.failCreates = 100
	deadQueue := NewQueue()
	deadWorker := NewWorker(deadQueue, provider, NewLock(), cfg)
	if _, _, err := deadQueue.Enqueue(ctx, Job{Name: "web-2", Op: fleet.OpCreate, Spec: spec, RunAfter: clock(), CreatedAt: clock()}); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		job = onlyJob(t, deadQueue)
		if job.Status == StatusDead {
			break
		}
		if job.Status == StatusPending && job.RunAfter.After(clock()) {
			advance(job.RunAfter.Sub(clock()))
		}
		if _, err := deadWorker.Once(ctx); err != nil {
			t.Fatal(err)
		}
	}
	job = onlyJob(t, deadQueue)
	if job.Status != StatusDead || job.Attempts != 5 {
		t.Fatalf("dead job = %+v", job)
	}
	if _, ok, err := deadQueue.Claim(ctx, clock().Add(time.Hour)); err != nil || ok {
		t.Fatalf("dead job was claimed ok=%v err=%v", ok, err)
	}
}

func TestWorker_LockBusyDoesNotCallProviderOrBurnAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	q := NewQueue()
	provider := newCountingProvider()
	lock := NewLock()
	current := now
	worker := NewWorker(q, provider, lock, Config{
		MaxAttempts: 5,
		BaseDelay:   time.Second,
		MaxDelay:    time.Second,
		Poll:        50 * time.Millisecond,
		Now:         func() time.Time { return current },
	})
	if _, _, err := q.Enqueue(ctx, Job{Name: "web-1", Op: fleet.OpCreate, Spec: spec, RunAfter: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := q.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}

	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = lock.WithLock(ctx, "web-1", func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	worker.finish(ctx, claimed)
	if provider.snapshot().creates != 0 {
		t.Fatalf("provider was called: %+v", provider.snapshot())
	}
	job := onlyJob(t, q)
	if job.Status != StatusPending || job.Attempts != 0 || !job.RunAfter.Equal(now.Add(50*time.Millisecond)) {
		t.Fatalf("released job = %+v", job)
	}
	close(release)
	<-done

	current = now.Add(50 * time.Millisecond)
	ok, err = worker.Once(ctx)
	if err != nil || !ok {
		t.Fatalf("apply after unlock ok=%v err=%v", ok, err)
	}
	if provider.snapshot().creates != 1 {
		t.Fatalf("counts = %+v", provider.snapshot())
	}
}

func TestLock_SecondHolderDoesNotEnter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lock := NewLock()
	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 2)
	go func() {
		errCh <- lock.WithLock(ctx, "web-1", func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	entered := false
	go func() {
		errCh <- lock.WithLock(ctx, "web-1", func(context.Context) error {
			entered = true
			return nil
		})
	}()
	var second error
	select {
	case second = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("second lock blocked")
	}
	if !errors.Is(second, ErrLocked) {
		t.Fatalf("second err = %v", second)
	}
	if entered {
		t.Fatal("second holder ran")
	}
	close(release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first lock did not finish")
	}
}

func TestWorker_BoundsInFlightJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := NewQueue()
	hold := make(chan struct{})
	var mu sync.Mutex
	current, maxSeen := 0, 0
	entered := make(chan struct{}, 8)
	provider := &gateProvider{
		inner: fake.New(),
		enter: func() {
			mu.Lock()
			current++
			if current > maxSeen {
				maxSeen = current
			}
			mu.Unlock()
			entered <- struct{}{}
			<-hold
			mu.Lock()
			current--
			mu.Unlock()
		},
	}
	worker := NewWorker(q, provider, NewLock(), Config{
		MaxInFlight: 2,
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		Poll:        time.Millisecond,
		Now:         time.Now,
	})
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	for i := range 6 {
		name := "n" + string(rune('a'+i))
		if _, _, err := q.Enqueue(ctx, Job{Name: name, Op: fleet.OpCreate, Spec: spec}); err != nil {
			t.Fatal(err)
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go worker.Run(runCtx)

	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for in-flight jobs")
		}
	}
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	seen := maxSeen
	mu.Unlock()
	if seen != 2 {
		t.Fatalf("max in flight = %d, want 2", seen)
	}
	close(hold)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if q.Count(StatusPending) == 0 && q.Count(StatusRunning) == 0 && q.Count(StatusSucceeded) == 6 {
			cancel()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("jobs pending=%d running=%d succeeded=%d max=%d", q.Count(StatusPending), q.Count(StatusRunning), q.Count(StatusSucceeded), maxSeen)
}

func fixedClock(now time.Time) Config {
	return Config{
		MaxInFlight: 2,
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
		Poll:        50 * time.Millisecond,
		Now:         func() time.Time { return now },
	}
}

func onlyJob(t *testing.T, q *MemoryQueue) Job {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.byID) != 1 {
		t.Fatalf("jobs = %d", len(q.byID))
	}
	for _, job := range q.byID {
		return job
	}
	return Job{}
}

type opCounts struct {
	creates int
	updates int
	deletes int
}

type countingProvider struct {
	inner       *fake.Provider
	mu          sync.Mutex
	creates     int
	updates     int
	deletes     int
	createCalls int
	failCreates int
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
	p.mu.Lock()
	p.createCalls++
	fail := p.createCalls <= p.failCreates
	p.mu.Unlock()
	if fail {
		return fleet.Actual{}, errors.New("provider create failed")
	}
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

type gateProvider struct {
	inner *fake.Provider
	enter func()
}

func (p *gateProvider) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Actual, error) {
	if p.enter != nil {
		p.enter()
	}
	return p.inner.Create(ctx, name, spec)
}

func (p *gateProvider) Update(ctx context.Context, id string, spec fleet.Spec) (fleet.Actual, error) {
	return p.inner.Update(ctx, id, spec)
}

func (p *gateProvider) Delete(ctx context.Context, id string) error {
	return p.inner.Delete(ctx, id)
}

func (p *gateProvider) List(ctx context.Context) ([]fleet.Actual, error) {
	return p.inner.List(ctx)
}
