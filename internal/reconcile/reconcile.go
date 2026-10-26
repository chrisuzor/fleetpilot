// Package reconcile converges provider instances to desired state.
package reconcile

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// Reconciler reads desired state from the store and actual state from the
// provider. It does not write the store.
type Reconciler struct {
	store    fleet.Store
	provider fleet.Provider
	mu       sync.Mutex
}

// New returns a reconciler. Runs are serialized in this process.
func New(store fleet.Store, provider fleet.Provider) *Reconciler {
	return &Reconciler{store: store, provider: provider}
}

// Reconcile creates, updates, and deletes provider instances until actual
// state matches desired state. Instances are matched by name.
//
// A run that finds no difference returns no actions and does not call
// Create, Update, or Delete. It still lists both sides.
//
// On a provider error, actions already applied are returned with the error.
// A later run continues from the provider's current state.
func (r *Reconciler) Reconcile(ctx context.Context) (fleet.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desired, err := r.store.List(ctx)
	if err != nil {
		return fleet.Result{}, fmt.Errorf("list desired: %w", err)
	}
	actual, err := r.provider.List(ctx)
	if err != nil {
		return fleet.Result{}, fmt.Errorf("list actual: %w", err)
	}
	desiredByName, err := fleet.DesiredByName(desired)
	if err != nil {
		return fleet.Result{}, err
	}
	actualByName, err := fleet.ActualByName(actual)
	if err != nil {
		return fleet.Result{}, err
	}

	names := make([]string, 0, len(desiredByName))
	for name := range desiredByName {
		names = append(names, name)
	}
	sort.Strings(names)

	actions := make([]fleet.Action, 0)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return fleet.Result{Actions: actions}, err
		}
		want := desiredByName[name]
		have, ok := actualByName[name]
		if !ok {
			created, err := r.provider.Create(ctx, want.Name, want.Spec)
			if err != nil {
				return fleet.Result{Actions: actions}, fmt.Errorf("create %s: %w", name, err)
			}
			actions = append(actions, fleet.Action{
				Op:         fleet.OpCreate,
				Name:       name,
				ProviderID: created.ID,
			})
			continue
		}
		if want.Spec.Equal(have.Spec) {
			continue
		}
		updated, err := r.provider.Update(ctx, have.ID, want.Spec)
		if err != nil {
			return fleet.Result{Actions: actions}, fmt.Errorf("update %s: %w", name, err)
		}
		actions = append(actions, fleet.Action{
			Op:         fleet.OpUpdate,
			Name:       name,
			ProviderID: updated.ID,
		})
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
			return fleet.Result{Actions: actions}, err
		}
		have := actualByName[name]
		if err := r.provider.Delete(ctx, have.ID); err != nil {
			return fleet.Result{Actions: actions}, fmt.Errorf("delete %s: %w", name, err)
		}
		actions = append(actions, fleet.Action{
			Op:         fleet.OpDelete,
			Name:       name,
			ProviderID: have.ID,
		})
	}
	return fleet.Result{Actions: actions}, nil
}
