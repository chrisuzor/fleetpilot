package work

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// MemoryQueue is the in-memory job queue used by tests.
type MemoryQueue struct {
	mu     sync.Mutex
	byID   map[string]Job
	active map[string]string
}

var _ Queue = (*MemoryQueue)(nil)

// NewQueue returns an empty queue.
func NewQueue() *MemoryQueue {
	return &MemoryQueue{
		byID:   make(map[string]Job),
		active: make(map[string]string),
	}
}

// Enqueue inserts a job or updates the pending job for that instance name.
// fresh is true when a new job was inserted or a pending job's op or spec changed.
// A running job is left alone and fresh is false.
func (q *MemoryQueue) Enqueue(ctx context.Context, in Job) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if id, ok := q.active[in.Name]; ok {
		job := q.byID[id]
		if job.Status == StatusRunning || sameWork(job, in) {
			return job, false, nil
		}
		now := in.UpdatedAt
		if now.IsZero() {
			now = time.Now().UTC()
		}
		job.Op = in.Op
		job.Spec = in.Spec
		job.ProviderID = in.ProviderID
		job.TraceParent = in.TraceParent
		job.Attempts = 0
		job.LastError = ""
		job.RunAfter = now
		job.UpdatedAt = now
		q.byID[job.ID] = job
		return job, true, nil
	}

	now := in.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if in.ID == "" {
		id, err := fleet.NewID("job_")
		if err != nil {
			return Job{}, false, err
		}
		in.ID = id
	}
	in.CreatedAt = now
	in.UpdatedAt = now
	if in.RunAfter.IsZero() {
		in.RunAfter = now
	}
	in.Status = StatusPending
	in.Attempts = 0
	in.LastError = ""
	q.byID[in.ID] = in
	q.active[in.Name] = in.ID
	return in, true, nil
}

// Claim marks one ready job running and returns it.
// A pending job is ready when RunAfter has passed. A running job is ready
// when it has been running longer than StaleAfter.
func (q *MemoryQueue) Claim(ctx context.Context, now time.Time) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	ready := make([]Job, 0)
	for _, job := range q.byID {
		if claimable(job, now) {
			ready = append(ready, job)
		}
	}
	if len(ready) == 0 {
		return Job{}, false, nil
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].CreatedAt.Equal(ready[j].CreatedAt) {
			return ready[i].ID < ready[j].ID
		}
		return ready[i].CreatedAt.Before(ready[j].CreatedAt)
	})
	job := ready[0]
	job.Status = StatusRunning
	job.UpdatedAt = now
	q.byID[job.ID] = job
	q.active[job.Name] = job.ID
	return job, true, nil
}

// GetJob returns a copy of one job.
func (q *MemoryQueue) GetJob(ctx context.Context, id string) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.byID[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return job, nil
}

// Succeed marks a running job succeeded and frees the instance name.
func (q *MemoryQueue) Succeed(ctx context.Context, id string, now time.Time) error {
	return q.update(ctx, id, func(job Job) (Job, bool) {
		if job.Status == StatusSucceeded {
			return job, false
		}
		if job.Status != StatusRunning {
			return job, false
		}
		job.Status = StatusSucceeded
		job.LastError = ""
		job.UpdatedAt = now
		return job, true
	}, true)
}

// Retry puts a running job back to pending with a future RunAfter.
func (q *MemoryQueue) Retry(ctx context.Context, id string, now, runAfter time.Time, attempts int, lastError string) error {
	return q.update(ctx, id, func(job Job) (Job, bool) {
		if job.Status != StatusRunning {
			return job, false
		}
		job.Status = StatusPending
		job.Attempts = attempts
		job.RunAfter = runAfter
		job.LastError = lastError
		job.UpdatedAt = now
		return job, true
	}, false)
}

// Dead marks a running job dead and frees the instance name.
func (q *MemoryQueue) Dead(ctx context.Context, id string, now time.Time, attempts int, lastError string) error {
	return q.update(ctx, id, func(job Job) (Job, bool) {
		if job.Status == StatusDead {
			return job, false
		}
		if job.Status != StatusRunning {
			return job, false
		}
		job.Status = StatusDead
		job.Attempts = attempts
		job.LastError = lastError
		job.UpdatedAt = now
		return job, true
	}, true)
}

// Release puts a running job back to pending without changing Attempts.
func (q *MemoryQueue) Release(ctx context.Context, id string, now, runAfter time.Time) error {
	return q.update(ctx, id, func(job Job) (Job, bool) {
		if job.Status != StatusRunning {
			return job, false
		}
		job.Status = StatusPending
		job.RunAfter = runAfter
		job.UpdatedAt = now
		return job, true
	}, false)
}

// Count reports how many jobs have status. Tests use it.
func (q *MemoryQueue) Count(status string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, job := range q.byID {
		if job.Status == status {
			n++
		}
	}
	return n
}

func (q *MemoryQueue) update(ctx context.Context, id string, fn func(Job) (Job, bool), dropActive bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.byID[id]
	if !ok {
		return ErrNotFound
	}
	next, changed := fn(job)
	if !changed {
		return nil
	}
	q.byID[id] = next
	if dropActive {
		if q.active[next.Name] == id {
			delete(q.active, next.Name)
		}
		return nil
	}
	q.active[next.Name] = id
	return nil
}

func claimable(job Job, now time.Time) bool {
	if job.Status == StatusPending && !job.RunAfter.After(now) {
		return true
	}
	return job.Status == StatusRunning && !job.UpdatedAt.After(now.Add(-StaleAfter))
}

func sameWork(job, in Job) bool {
	return job.Op == in.Op && job.Spec == in.Spec
}
