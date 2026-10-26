// Package reconcile compares desired state with the provider and enqueues
// provisioning jobs. It does not call the provider's mutating methods.
package reconcile

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

// Reconciler reads desired state and actual state, then enqueues the
// difference. A worker applies the jobs.
type Reconciler struct {
	store    fleet.Store
	provider fleet.Provider
	queue    work.Queue
	mu       sync.Mutex
}

// Result is the jobs a reconcile run newly queued or refreshed.
// Jobs is empty when nothing changed, including a second run with the same spec.
type Result struct {
	Jobs []work.Job
}

// New returns a reconciler. Runs are serialized in this process.
func New(store fleet.Store, provider fleet.Provider, queue work.Queue) *Reconciler {
	return &Reconciler{store: store, provider: provider, queue: queue}
}

// Reconcile enqueues create, update, and delete jobs until the queue
// describes the difference between desired and actual state.
// Instances are matched by name.
//
// A run that finds no difference, or whose jobs are already queued, returns
// no jobs and does not call Create, Update, or Delete.
func (r *Reconciler) Reconcile(ctx context.Context) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desired, err := r.store.List(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("list desired: %w", err)
	}
	actual, err := r.provider.List(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("list actual: %w", err)
	}
	desiredByName, err := fleet.DesiredByName(desired)
	if err != nil {
		return Result{}, err
	}
	actualByName, err := fleet.ActualByName(actual)
	if err != nil {
		return Result{}, err
	}

	names := make([]string, 0, len(desiredByName))
	for name := range desiredByName {
		names = append(names, name)
	}
	sort.Strings(names)

	jobs := make([]work.Job, 0)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return Result{Jobs: jobs}, err
		}
		want := desiredByName[name]
		have, ok := actualByName[name]
		var (
			op         string
			providerID string
		)
		switch {
		case !ok:
			op = fleet.OpCreate
		case want.Spec.Equal(have.Spec):
			continue
		default:
			op = fleet.OpUpdate
			providerID = have.ID
		}
		job, fresh, err := r.queue.Enqueue(ctx, work.Job{
			Name:       name,
			Op:         op,
			Spec:       want.Spec,
			ProviderID: providerID,
		})
		if err != nil {
			return Result{Jobs: jobs}, fmt.Errorf("enqueue %s: %w", name, err)
		}
		if fresh {
			jobs = append(jobs, job)
		}
	}

	orphans := make([]string, 0)
	for name := range actualByName {
		if _, wanted := desiredByName[name]; !wanted {
			orphans = append(orphans, name)
		}
	}
	sort.Strings(orphans)
	for _, name := range orphans {
		if err := ctx.Err(); err != nil {
			return Result{Jobs: jobs}, err
		}
		have := actualByName[name]
		job, fresh, err := r.queue.Enqueue(ctx, work.Job{
			Name:       name,
			Op:         fleet.OpDelete,
			ProviderID: have.ID,
		})
		if err != nil {
			return Result{Jobs: jobs}, fmt.Errorf("enqueue %s: %w", name, err)
		}
		if fresh {
			jobs = append(jobs, job)
		}
	}
	return Result{Jobs: jobs}, nil
}
