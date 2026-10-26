// Package fake is an in-process VM provider.
// It is the default provider so the control loop runs without cloud credentials.
package fake

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// ErrInjected is returned by Create, Update, and Delete while FailNext still
// has remaining failures. The call changes no instance.
var ErrInjected = errors.New("injected provider failure")

// Provider keeps actual instances in memory for the life of the process.
type Provider struct {
	mu            sync.Mutex
	byID          map[string]fleet.Actual
	byName        map[string]string
	failRemaining int
	unhealthy     map[string]string
}

var (
	_ fleet.Provider = (*Provider)(nil)
	_ fleet.Checker  = (*Provider)(nil)
)

// New returns an empty provider.
func New() *Provider {
	return &Provider{
		byID:      make(map[string]fleet.Actual),
		byName:    make(map[string]string),
		unhealthy: make(map[string]string),
	}
}

// FailNext makes the next n successful lookups in Create, Update, or Delete
// return ErrInjected before they change any instance.
func (p *Provider) FailNext(n int) {
	p.mu.Lock()
	p.failRemaining = n
	p.mu.Unlock()
}

// SetUnhealthy makes Check report unhealthy for name until the process restarts.
func (p *Provider) SetUnhealthy(name, detail string) {
	if detail == "" {
		detail = "unhealthy"
	}
	p.mu.Lock()
	p.unhealthy[name] = detail
	p.mu.Unlock()
}

// Create stores a running instance under name.
func (p *Provider) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Actual, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.byName[name]; exists {
		return fleet.Actual{}, fleet.ErrConflict
	}
	if err := p.consumeFailure(); err != nil {
		return fleet.Actual{}, err
	}
	id, err := p.allocateID()
	if err != nil {
		return fleet.Actual{}, err
	}
	inst := fleet.Actual{
		ID:     id,
		Name:   name,
		Spec:   spec,
		Status: fleet.StatusRunning,
	}
	p.byID[id] = inst
	p.byName[name] = id
	return inst, nil
}

// Update replaces the spec of an existing instance and keeps its id and name.
func (p *Provider) Update(ctx context.Context, id string, spec fleet.Spec) (fleet.Actual, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	inst, ok := p.byID[id]
	if !ok {
		return fleet.Actual{}, fleet.ErrNotFound
	}
	if err := p.consumeFailure(); err != nil {
		return fleet.Actual{}, err
	}
	inst.Spec = spec
	p.byID[id] = inst
	return inst, nil
}

// Delete removes an instance.
func (p *Provider) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	inst, ok := p.byID[id]
	if !ok {
		return fleet.ErrNotFound
	}
	if err := p.consumeFailure(); err != nil {
		return err
	}
	delete(p.byID, id)
	delete(p.byName, inst.Name)
	return nil
}

// List returns a snapshot of actual instances, sorted by name.
func (p *Provider) List(ctx context.Context) ([]fleet.Actual, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]fleet.Actual, 0, len(p.byID))
	for _, inst := range p.byID {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Check reports healthy for a running instance. SetUnhealthy overrides that.
// A missing id returns fleet.ErrNotFound.
func (p *Provider) Check(ctx context.Context, id string) (fleet.Check, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Check{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	inst, ok := p.byID[id]
	if !ok {
		return fleet.Check{}, fleet.ErrNotFound
	}
	if detail, bad := p.unhealthy[inst.Name]; bad {
		return fleet.Check{Status: fleet.HealthUnhealthy, Detail: detail}, nil
	}
	if inst.Status != fleet.StatusRunning {
		return fleet.Check{Status: fleet.HealthUnhealthy, Detail: inst.Status}, nil
	}
	return fleet.Check{Status: fleet.HealthHealthy, Detail: "running"}, nil
}

func (p *Provider) consumeFailure() error {
	if p.failRemaining <= 0 {
		return nil
	}
	p.failRemaining--
	return ErrInjected
}

func (p *Provider) allocateID() (string, error) {
	for range 5 {
		id, err := fleet.NewID("vm_")
		if err != nil {
			return "", err
		}
		if _, exists := p.byID[id]; !exists {
			return id, nil
		}
	}
	return "", errors.New("allocate id")
}
