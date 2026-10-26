package work

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/chrisuzor/fleetpilot/internal/audit"
	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/metrics"
	"github.com/chrisuzor/fleetpilot/internal/telemetry"
)

// Worker claims jobs and applies them to a provider.
type Worker struct {
	queue    Queue
	provider fleet.Provider
	locker   Locker
	cfg      Config
	audit    audit.Log
	metrics  *metrics.Metrics
	tracer   trace.Tracer
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

// SetAudit records job outcomes. A nil log leaves outcomes unrecorded.
func (w *Worker) SetAudit(log audit.Log) {
	w.audit = log
}

// SetMetrics counts terminal outcomes and in-flight applies. Nil ignores them.
func (w *Worker) SetMetrics(m *metrics.Metrics) {
	w.metrics = m
}

// SetTracer records the job.apply span. Call it before Run.
func (w *Worker) SetTracer(tr trace.Tracer) {
	w.tracer = tr
}

func (w *Worker) otelTracer() trace.Tracer {
	if w.tracer != nil {
		return w.tracer
	}
	return otel.Tracer(telemetry.TracerName)
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
			w.applyClaimed(ctx, job)
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
	w.applyClaimed(ctx, job)
	return true, nil
}

func (w *Worker) applyClaimed(ctx context.Context, job Job) {
	if w.metrics != nil {
		w.metrics.JobStarted()
		defer w.metrics.JobFinished()
	}
	w.finish(ctx, job)
}

func (w *Worker) finish(ctx context.Context, job Job) {
	ctx, span := w.otelTracer().Start(telemetry.Extract(ctx, job.TraceParent), "job.apply", trace.WithSpanKind(trace.SpanKindConsumer))
	outcome := "error"
	defer func() {
		span.SetAttributes(
			attribute.String("job.op", job.Op),
			attribute.String("instance.name", job.Name),
			attribute.String("job.outcome", outcome),
		)
		span.End()
	}()

	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()

	err := w.locker.WithLock(ctx, job.Name, func(ctx context.Context) error {
		return w.applyProvider(ctx, job)
	})
	now := w.cfg.Now()
	if err == nil {
		if saveErr := w.queue.Succeed(save, job.ID, now); saveErr != nil {
			slog.Error("succeed job", "job", job.ID, "err", saveErr)
			span.RecordError(saveErr)
			span.SetStatus(codes.Error, "succeed job")
			return
		}
		outcome = "succeeded"
		span.SetStatus(codes.Ok, "")
		w.metrics.JobOutcome(outcome)
		w.record(save, audit.ActionJobSucceeded, job, job.Name+" "+job.Op)
		return
	}
	if errors.Is(err, ErrLocked) || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		outcome = "released"
		if saveErr := w.queue.Release(save, job.ID, now, now.Add(w.cfg.Poll)); saveErr != nil {
			slog.Error("release job", "job", job.ID, "err", saveErr)
		}
		return
	}

	attempts := job.Attempts + 1
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	if attempts >= w.cfg.MaxAttempts {
		slog.Error("job dead", "job", job.ID, "name", job.Name, "op", job.Op, "attempts", attempts, "err", err)
		if saveErr := w.queue.Dead(save, job.ID, now, attempts, err.Error()); saveErr != nil {
			slog.Error("mark job dead", "job", job.ID, "err", saveErr)
			return
		}
		outcome = "dead"
		w.metrics.JobOutcome(outcome)
		w.record(save, audit.ActionJobDead, job, job.Name+" "+job.Op+": "+err.Error())
		return
	}
	delay := Backoff(attempts, w.cfg.BaseDelay, w.cfg.MaxDelay)
	slog.Info("job retry", "job", job.ID, "name", job.Name, "op", job.Op, "attempt", attempts, "backoff", delay.String(), "err", err)
	if saveErr := w.queue.Retry(save, job.ID, now, now.Add(delay), attempts, err.Error()); saveErr != nil {
		slog.Error("retry job", "job", job.ID, "err", saveErr)
		return
	}
	outcome = "retry"
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

func (w *Worker) record(ctx context.Context, action string, job Job, detail string) {
	if w.audit == nil {
		return
	}
	if _, err := w.audit.AppendAudit(ctx, audit.Event{
		Actor:    audit.ActorWorker,
		Action:   action,
		Resource: job.ID,
		Detail:   detail,
	}); err != nil {
		slog.Error("audit", "action", action, "job", job.ID, "err", err)
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
