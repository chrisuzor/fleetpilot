package work

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// Worker claims jobs and applies them to a provider.
type Worker struct {
	queue    Queue
	provider fleet.Provider
	locker   Locker
	cfg      Config
}

// NewWorker returns a worker. Zero config fields use the defaults.
func NewWorker(queue Queue, provider fleet.Provider, locker Locker, cfg Config) *Worker {
	return &Worker{
		queue:    queue,
		provider: provider,
		locker:   locker,
		cfg:      cfg.normalize(),
	}
}

// Run claims jobs until ctx is cancelled.
// At most MaxInFlight jobs are inside the provider at once.
func (w *Worker) Run(ctx context.Context) {
	slots := make(chan struct{}, w.cfg.MaxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}

		job, ok, err := w.queue.Claim(ctx, w.cfg.Now())
		if err != nil || !ok {
			<-slots
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				slog.Error("claim job", "err", err)
			}
			if !sleep(ctx, w.cfg.Poll) {
				return
			}
			continue
		}

		wg.Add(1)
		go func(job Job) {
			defer wg.Done()
			defer func() { <-slots }()
			w.finish(ctx, job)
		}(job)
	}
}

// Once claims and applies a single ready job.
// ok is false when nothing is ready.
func (w *Worker) Once(ctx context.Context) (bool, error) {
	job, ok, err := w.queue.Claim(ctx, w.cfg.Now())
	if err != nil || !ok {
		return false, err
	}
	w.finish(ctx, job)
	return true, nil
}

func (w *Worker) finish(ctx context.Context, job Job) {
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()

	err := w.locker.WithLock(ctx, job.Name, func(ctx context.Context) error {
		return w.applyProvider(ctx, job)
	})
	now := w.cfg.Now()
	if err == nil {
		if saveErr := w.queue.Succeed(save, job.ID, now); saveErr != nil {
			slog.Error("succeed job", "job", job.ID, "err", saveErr)
		}
		return
	}
	if errors.Is(err, ErrLocked) || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if saveErr := w.queue.Release(save, job.ID, now, now.Add(w.cfg.Poll)); saveErr != nil {
			slog.Error("release job", "job", job.ID, "err", saveErr)
		}
		return
	}

	attempts := job.Attempts + 1
	if attempts >= w.cfg.MaxAttempts {
		slog.Error("job dead", "job", job.ID, "name", job.Name, "op", job.Op, "attempts", attempts, "err", err)
		if saveErr := w.queue.Dead(save, job.ID, now, attempts, err.Error()); saveErr != nil {
			slog.Error("mark job dead", "job", job.ID, "err", saveErr)
		}
		return
	}
	delay := Backoff(attempts, w.cfg.BaseDelay, w.cfg.MaxDelay)
	slog.Info("job retry", "job", job.ID, "name", job.Name, "op", job.Op, "attempt", attempts, "backoff", delay.String(), "err", err)
	if saveErr := w.queue.Retry(save, job.ID, now, now.Add(delay), attempts, err.Error()); saveErr != nil {
		slog.Error("retry job", "job", job.ID, "err", saveErr)
	}
}

// applyProvider mutates the provider. Create and delete are idempotent:
// a name that already exists is not created again, and a name that is
// already gone is not deleted again.
func (w *Worker) applyProvider(ctx context.Context, job Job) error {
	list, err := w.provider.List(ctx)
	if err != nil {
		return err
	}
	byName, err := fleet.ActualByName(list)
	if err != nil {
		return err
	}
	have, exists := byName[job.Name]
	switch job.Op {
	case fleet.OpCreate:
		if exists {
			return nil
		}
		_, err = w.provider.Create(ctx, job.Name, job.Spec)
		if errors.Is(err, fleet.ErrConflict) {
			return nil
		}
		return err
	case fleet.OpUpdate:
		if !exists {
			return fmt.Errorf("update %s: %w", job.Name, fleet.ErrNotFound)
		}
		if have.Spec.Equal(job.Spec) {
			return nil
		}
		_, err = w.provider.Update(ctx, have.ID, job.Spec)
		return err
	case fleet.OpDelete:
		if !exists {
			return nil
		}
		err = w.provider.Delete(ctx, have.ID)
		if errors.Is(err, fleet.ErrNotFound) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("unknown op %q", job.Op)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
