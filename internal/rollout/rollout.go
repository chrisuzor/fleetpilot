// Package rollout changes desired specs one instance at a time.
// It enqueues the update and waits until the worker has applied it, then
// stores a health check. An unhealthy check pauses the rollout.
package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/audit"
	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

// Roller advances stored rollouts.
type Roller struct {
	store    fleet.Store
	provider fleet.Provider
	queue    work.Queue
	checker  fleet.Checker
	audit    audit.Log
	mu       sync.Mutex
}

// New returns a roller. store holds desired state, health checks, and rollouts.
func New(store fleet.Store, provider fleet.Provider, queue work.Queue, checker fleet.Checker) *Roller {
	return &Roller{store: store, provider: provider, queue: queue, checker: checker}
}

// SetAudit records rollout pauses. A nil log leaves pauses unrecorded.
func (r *Roller) SetAudit(log audit.Log) {
	r.audit = log
}

// Start snapshots desired names in name order and stores a running rollout.
// An empty fleet is stored as succeeded. A rollout that is already running
// returns fleet.ErrConflict.
func (r *Roller) Start(ctx context.Context, spec fleet.Spec) (fleet.Rollout, error) {
	desired, err := r.store.List(ctx)
	if err != nil {
		return fleet.Rollout{}, fmt.Errorf("list desired: %w", err)
	}
	names := make([]string, len(desired))
	for i, item := range desired {
		names[i] = item.Name
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	roll, err := r.store.CreateRollout(ctx, spec, names, now)
	if err != nil {
		return fleet.Rollout{}, err
	}
	if len(names) == 0 {
		roll.Status = fleet.RolloutSucceeded
		if err := r.store.SaveRollout(ctx, roll); err != nil {
			return fleet.Rollout{}, err
		}
		roll, err = r.store.GetRollout(ctx, roll.ID)
		if err != nil {
			return fleet.Rollout{}, err
		}
	}
	return roll, nil
}

// Run advances every running rollout until ctx is cancelled.
func (r *Roller) Run(ctx context.Context, poll time.Duration) {
	if poll <= 0 {
		poll = work.DefaultPoll
	}
	for {
		if ctx.Err() != nil {
			return
		}
		rolls, err := r.store.ListRunningRollouts(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("list rollouts", "err", err)
		}
		for _, roll := range rolls {
			if _, err := r.Step(ctx, roll.ID); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("rollout step", "rollout", roll.ID, "err", err)
			}
		}
		if !sleep(ctx, poll) {
			return
		}
	}
}

// Step moves one rollout forward by at most one instance.
// A paused or succeeded rollout is returned unchanged.
func (r *Roller) Step(ctx context.Context, id string) (fleet.Rollout, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.step(ctx, id)
}

func (r *Roller) step(ctx context.Context, id string) (fleet.Rollout, error) {
	roll, err := r.store.GetRollout(ctx, id)
	if err != nil {
		return fleet.Rollout{}, err
	}
	if roll.Status != fleet.RolloutRunning {
		return roll, nil
	}
	if roll.Next >= len(roll.Names) {
		return r.finish(ctx, roll, fleet.RolloutSucceeded, "")
	}

	name := roll.Names[roll.Next]
	desired, err := r.store.List(ctx)
	if err != nil {
		return fleet.Rollout{}, fmt.Errorf("list desired: %w", err)
	}
	item, ok := findDesired(desired, name)
	if !ok {
		return r.finish(ctx, roll, fleet.RolloutPaused, "instance gone")
	}

	switch roll.Phase {
	case fleet.PhaseApplying:
		return r.observeJob(ctx, roll, item)
	case fleet.PhaseChecking:
		return r.check(ctx, roll, item)
	default:
		return r.begin(ctx, roll, item)
	}
}

func (r *Roller) begin(ctx context.Context, roll fleet.Rollout, item fleet.Desired) (fleet.Rollout, error) {
	actual, ok, err := r.actual(ctx, item.Name)
	if err != nil {
		return fleet.Rollout{}, err
	}
	if item.Spec.Equal(roll.Spec) && ok && actual.Spec.Equal(roll.Spec) {
		roll.Phase = fleet.PhaseChecking
		return r.check(ctx, roll, item)
	}
	if !item.Spec.Equal(roll.Spec) {
		if _, err := r.store.UpdateSpec(ctx, item.ID, roll.Spec); err != nil {
			return fleet.Rollout{}, fmt.Errorf("update desired %s: %w", item.Name, err)
		}
	}
	providerID := ""
	if ok {
		providerID = actual.ID
	}
	job, _, err := r.queue.Enqueue(ctx, work.Job{
		Name:       item.Name,
		Op:         fleet.OpUpdate,
		Spec:       roll.Spec,
		ProviderID: providerID,
	})
	if err != nil {
		return fleet.Rollout{}, fmt.Errorf("enqueue %s: %w", item.Name, err)
	}
	roll.JobID = job.ID
	roll.Phase = fleet.PhaseApplying
	if err := r.store.SaveRollout(ctx, roll); err != nil {
		return fleet.Rollout{}, err
	}
	return r.store.GetRollout(ctx, roll.ID)
}

func (r *Roller) observeJob(ctx context.Context, roll fleet.Rollout, item fleet.Desired) (fleet.Rollout, error) {
	if roll.JobID == "" {
		return fleet.Rollout{}, errors.New("rollout has no job")
	}
	job, err := r.queue.GetJob(ctx, roll.JobID)
	if err != nil {
		return fleet.Rollout{}, err
	}
	switch job.Status {
	case work.StatusPending, work.StatusRunning:
		return roll, nil
	case work.StatusDead:
		return r.finish(ctx, roll, fleet.RolloutPaused, fleet.PauseJobDead)
	case work.StatusSucceeded:
		actual, ok, err := r.actual(ctx, item.Name)
		if err != nil {
			return fleet.Rollout{}, err
		}
		if !ok || !actual.Spec.Equal(roll.Spec) {
			return r.finish(ctx, roll, fleet.RolloutPaused, "spec did not converge")
		}
		roll.Phase = fleet.PhaseChecking
		return r.check(ctx, roll, item)
	default:
		return fleet.Rollout{}, fmt.Errorf("job %s status %q", job.ID, job.Status)
	}
}

func (r *Roller) check(ctx context.Context, roll fleet.Rollout, item fleet.Desired) (fleet.Rollout, error) {
	actual, ok, err := r.actual(ctx, item.Name)
	if err != nil {
		return fleet.Rollout{}, err
	}
	var result fleet.Check
	if !ok {
		result = fleet.Check{Status: fleet.HealthUnhealthy, Detail: "not running"}
	} else {
		result, err = r.checker.Check(ctx, actual.ID)
		if errors.Is(err, fleet.ErrNotFound) {
			result = fleet.Check{Status: fleet.HealthUnhealthy, Detail: "not running"}
		} else if err != nil {
			return fleet.Rollout{}, err
		}
	}
	if _, err := r.store.RecordHealth(ctx, item.ID, result.Status, result.Detail, time.Now().UTC()); err != nil {
		return fleet.Rollout{}, fmt.Errorf("record health %s: %w", item.Name, err)
	}
	if result.Status != fleet.HealthHealthy {
		roll.Phase = fleet.PhaseChecking
		return r.finish(ctx, roll, fleet.RolloutPaused, fleet.PauseHealthFailed)
	}
	roll.Next++
	roll.JobID = ""
	roll.Phase = fleet.PhaseIdle
	roll.PauseReason = ""
	if roll.Next >= len(roll.Names) {
		roll.Status = fleet.RolloutSucceeded
	}
	if err := r.store.SaveRollout(ctx, roll); err != nil {
		return fleet.Rollout{}, err
	}
	return r.store.GetRollout(ctx, roll.ID)
}

func (r *Roller) finish(ctx context.Context, roll fleet.Rollout, status, reason string) (fleet.Rollout, error) {
	roll.Status = status
	roll.PauseReason = reason
	if err := r.store.SaveRollout(ctx, roll); err != nil {
		return fleet.Rollout{}, err
	}
	saved, err := r.store.GetRollout(ctx, roll.ID)
	if err != nil {
		return fleet.Rollout{}, err
	}
	if saved.Status == fleet.RolloutPaused {
		r.recordPause(ctx, saved)
	}
	return saved, nil
}

func (r *Roller) recordPause(ctx context.Context, roll fleet.Rollout) {
	if r.audit == nil {
		return
	}
	detail := roll.PauseReason
	if roll.Next >= 0 && roll.Next < len(roll.Names) {
		detail = roll.Names[roll.Next] + ": " + roll.PauseReason
	}
	if _, err := r.audit.AppendAudit(ctx, audit.Event{
		Actor:    audit.ActorRoller,
		Action:   audit.ActionRolloutPause,
		Resource: roll.ID,
		Detail:   detail,
	}); err != nil {
		slog.Error("audit", "action", audit.ActionRolloutPause, "rollout", roll.ID, "err", err)
	}
}

func (r *Roller) actual(ctx context.Context, name string) (fleet.Actual, bool, error) {
	list, err := r.provider.List(ctx)
	if err != nil {
		return fleet.Actual{}, false, err
	}
	byName, err := fleet.ActualByName(list)
	if err != nil {
		return fleet.Actual{}, false, err
	}
	item, ok := byName[name]
	return item, ok, nil
}

func findDesired(items []fleet.Desired, name string) (fleet.Desired, bool) {
	for _, item := range items {
		if item.Name == name {
			return item, true
		}
	}
	return fleet.Desired{}, false
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
