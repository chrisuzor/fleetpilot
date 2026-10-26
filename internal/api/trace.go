package api

import (
	"context"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/telemetry"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

// traceQueue copies the current span onto a job before it is stored.
// The worker reads that traceparent when it applies the job.
type traceQueue struct {
	inner work.Queue
}

func (q traceQueue) Enqueue(ctx context.Context, job work.Job) (work.Job, bool, error) {
	job.TraceParent = telemetry.Inject(ctx)
	return q.inner.Enqueue(ctx, job)
}

func (q traceQueue) Claim(ctx context.Context, now time.Time) (work.Job, bool, error) {
	return q.inner.Claim(ctx, now)
}

func (q traceQueue) GetJob(ctx context.Context, id string) (work.Job, error) {
	return q.inner.GetJob(ctx, id)
}

func (q traceQueue) Succeed(ctx context.Context, id string, now time.Time) error {
	return q.inner.Succeed(ctx, id, now)
}

func (q traceQueue) Retry(ctx context.Context, id string, now, runAfter time.Time, attempts int, lastError string) error {
	return q.inner.Retry(ctx, id, now, runAfter, attempts, lastError)
}

func (q traceQueue) Dead(ctx context.Context, id string, now time.Time, attempts int, lastError string) error {
	return q.inner.Dead(ctx, id, now, attempts, lastError)
}

func (q traceQueue) Release(ctx context.Context, id string, now, runAfter time.Time) error {
	return q.inner.Release(ctx, id, now, runAfter)
}
