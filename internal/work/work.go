// Package work queues provisioning jobs and applies them to a provider.
package work

import (
	"context"
	"errors"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusDead      = "dead"

	// A running job older than this can be claimed again. Applying a job is
	// idempotent, so a reclaimed job does not double-create or double-delete.
	StaleAfter = 30 * time.Second

	DefaultMaxInFlight = 4
	DefaultMaxAttempts = 5
	DefaultBaseDelay   = 100 * time.Millisecond
	DefaultMaxDelay    = 5 * time.Second
	DefaultPoll        = 50 * time.Millisecond
)

// ErrLocked means another applier holds the instance lock.
// The provider call was not started.
var ErrLocked = errors.New("instance locked")

// ErrNotFound means the job id is not in the queue.
var ErrNotFound = errors.New("not found")

// Job is one provisioning operation for a single instance name.
type Job struct {
	ID          string
	Name        string
	Op          string
	Spec        fleet.Spec
	ProviderID  string
	Attempts    int
	Status      string
	RunAfter    time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	TraceParent string
}

// Queue stores provisioning jobs.
// At most one pending or running job exists per instance name.
type Queue interface {
	Enqueue(ctx context.Context, job Job) (stored Job, fresh bool, err error)
	Claim(ctx context.Context, now time.Time) (Job, bool, error)
	GetJob(ctx context.Context, id string) (Job, error)
	Succeed(ctx context.Context, id string, now time.Time) error
	Retry(ctx context.Context, id string, now, runAfter time.Time, attempts int, lastError string) error
	Dead(ctx context.Context, id string, now time.Time, attempts int, lastError string) error
	Release(ctx context.Context, id string, now, runAfter time.Time) error
}

// Locker is the per-instance lock around a provider call.
// WithLock runs fn only when this caller holds the lock.
// If another caller holds it, WithLock returns ErrLocked and does not run fn.
type Locker interface {
	WithLock(ctx context.Context, instanceName string, fn func(context.Context) error) error
}

// Config controls the worker loop.
type Config struct {
	MaxInFlight int
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Poll        time.Duration
	Now         func() time.Time
}

// DefaultConfig is what the API process runs.
func DefaultConfig() Config {
	return Config{
		MaxInFlight: DefaultMaxInFlight,
		MaxAttempts: DefaultMaxAttempts,
		BaseDelay:   DefaultBaseDelay,
		MaxDelay:    DefaultMaxDelay,
		Poll:        DefaultPoll,
		Now:         func() time.Time { return time.Now().UTC() },
	}
}

func (c Config) normalize() Config {
	if c.MaxInFlight < 1 {
		c.MaxInFlight = DefaultMaxInFlight
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = DefaultBaseDelay
	}
	if c.MaxDelay < c.BaseDelay {
		c.MaxDelay = c.BaseDelay
	}
	if c.Poll <= 0 {
		c.Poll = DefaultPoll
	}
	if c.Now == nil {
		c.Now = func() time.Time { return time.Now().UTC() }
	}
	return c
}

// Backoff is the wait after provider failure number attempt.
// attempt is 1-based. The delay is min(max, base * 2^(attempt-1)).
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		return 0
	}
	if max < base {
		max = base
	}
	delay := base
	for n := 1; n < attempt; n++ {
		if delay >= max || delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}
