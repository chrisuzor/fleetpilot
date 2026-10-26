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

// Provider keeps actual instances in memory for the life of the process.
type Provider struct {
	mu     sync.Mutex
	byID   map[string]fleet.Actual
	byName map[string]string
}

var _ fleet.Provider = (*Provider)(nil)

// New returns an empty provider.
func New() *Provider {
	return &Provider{
		byID:   make(map[string]fleet.Actual),
		byName: make(map[string]string),
	}
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
