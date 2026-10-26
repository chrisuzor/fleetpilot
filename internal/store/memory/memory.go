// Package memory is an in-memory desired-state store for tests.
package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// Store keeps desired instances in memory.
type Store struct {
	mu     sync.Mutex
	byID   map[string]fleet.Desired
	byName map[string]string
}

var _ fleet.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		byID:   make(map[string]fleet.Desired),
		byName: make(map[string]string),
	}
}

// Create inserts a desired instance.
func (s *Store) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Desired, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Desired{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byName[name]; exists {
		return fleet.Desired{}, fleet.ErrConflict
	}
	id, err := s.allocateID()
	if err != nil {
		return fleet.Desired{}, err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	item := fleet.Desired{
		ID:        id,
		Name:      name,
		Spec:      spec,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.byID[id] = item
	s.byName[name] = id
	return item, nil
}

// Get returns one desired instance by id.
func (s *Store) Get(ctx context.Context, id string) (fleet.Desired, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Desired{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.byID[id]
	if !ok {
		return fleet.Desired{}, fleet.ErrNotFound
	}
	return item, nil
}

// List returns desired instances sorted by name.
func (s *Store) List(ctx context.Context) ([]fleet.Desired, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fleet.Desired, 0, len(s.byID))
	for _, item := range s.byID {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// UpdateSpec replaces the spec of an existing desired instance.
func (s *Store) UpdateSpec(ctx context.Context, id string, spec fleet.Spec) (fleet.Desired, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Desired{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.byID[id]
	if !ok {
		return fleet.Desired{}, fleet.ErrNotFound
	}
	item.Spec = spec
	item.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	s.byID[id] = item
	return item, nil
}

// Delete removes a desired instance. The provider instance, if any, remains
// until reconcile runs.
func (s *Store) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.byID[id]
	if !ok {
		return fleet.ErrNotFound
	}
	delete(s.byID, id)
	delete(s.byName, item.Name)
	return nil
}

// Ping reports that the in-memory store is available.
func (s *Store) Ping(context.Context) error {
	return nil
}

func (s *Store) allocateID() (string, error) {
	for range 5 {
		id, err := fleet.NewID("inst_")
		if err != nil {
			return "", err
		}
		if _, exists := s.byID[id]; !exists {
			return id, nil
		}
	}
	return "", errors.New("allocate id")
}
