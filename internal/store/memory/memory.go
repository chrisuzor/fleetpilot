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
	mu       sync.Mutex
	byID     map[string]fleet.Desired
	byName   map[string]string
	health   []fleet.HealthCheck
	rollouts map[string]fleet.Rollout
}

var _ fleet.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		byID:     make(map[string]fleet.Desired),
		byName:   make(map[string]string),
		rollouts: make(map[string]fleet.Rollout),
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

// RecordHealth appends a check. LatestHealth returns the newest one for instanceID.
func (s *Store) RecordHealth(ctx context.Context, instanceID, status, detail string, at time.Time) (fleet.HealthCheck, error) {
	if err := ctx.Err(); err != nil {
		return fleet.HealthCheck{}, err
	}
	if status != fleet.HealthHealthy && status != fleet.HealthUnhealthy {
		return fleet.HealthCheck{}, errors.New("health status must be healthy or unhealthy")
	}
	if len(detail) > 256 {
		detail = detail[:256]
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	id, err := fleet.NewID("chk_")
	if err != nil {
		return fleet.HealthCheck{}, err
	}
	check := fleet.HealthCheck{
		ID:         id,
		InstanceID: instanceID,
		Status:     status,
		Detail:     detail,
		CheckedAt:  at,
	}
	s.health = append(s.health, check)
	return check, nil
}

// LatestHealth returns the newest stored check for instanceID.
func (s *Store) LatestHealth(ctx context.Context, instanceID string) (fleet.HealthCheck, error) {
	if err := ctx.Err(); err != nil {
		return fleet.HealthCheck{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var found fleet.HealthCheck
	ok := false
	for _, check := range s.health {
		if check.InstanceID != instanceID {
			continue
		}
		if !ok || check.CheckedAt.After(found.CheckedAt) || (check.CheckedAt.Equal(found.CheckedAt) && check.ID > found.ID) {
			found = check
			ok = true
		}
	}
	if !ok {
		return fleet.HealthCheck{}, fleet.ErrNotFound
	}
	return found, nil
}

// CreateRollout stores a running rollout. A second running rollout conflicts.
func (s *Store) CreateRollout(ctx context.Context, spec fleet.Spec, names []string, at time.Time) (fleet.Rollout, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Rollout{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.rollouts {
		if existing.Status == fleet.RolloutRunning {
			return fleet.Rollout{}, fleet.ErrConflict
		}
	}
	id, err := fleet.NewID("roll_")
	if err != nil {
		return fleet.Rollout{}, err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)
	roll := fleet.Rollout{
		ID:        id,
		Spec:      spec,
		Status:    fleet.RolloutRunning,
		Names:     copyNames(names),
		Phase:     fleet.PhaseIdle,
		CreatedAt: at,
		UpdatedAt: at,
	}
	s.rollouts[id] = roll
	return copyRollout(roll), nil
}

// GetRollout returns a copy of one rollout.
func (s *Store) GetRollout(ctx context.Context, id string) (fleet.Rollout, error) {
	if err := ctx.Err(); err != nil {
		return fleet.Rollout{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	roll, ok := s.rollouts[id]
	if !ok {
		return fleet.Rollout{}, fleet.ErrNotFound
	}
	return copyRollout(roll), nil
}

// SaveRollout replaces the mutable fields of an existing rollout.
func (s *Store) SaveRollout(ctx context.Context, rollout fleet.Rollout) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.rollouts[rollout.ID]
	if !ok {
		return fleet.ErrNotFound
	}
	rollout.CreatedAt = existing.CreatedAt
	rollout.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	rollout.Names = copyNames(rollout.Names)
	s.rollouts[rollout.ID] = rollout
	return nil
}

// ListRunningRollouts returns running rollouts ordered by creation time.
func (s *Store) ListRunningRollouts(ctx context.Context) ([]fleet.Rollout, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fleet.Rollout, 0)
	for _, roll := range s.rollouts {
		if roll.Status == fleet.RolloutRunning {
			out = append(out, copyRollout(roll))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func copyNames(names []string) []string {
	out := make([]string, len(names))
	copy(out, names)
	return out
}

func copyRollout(roll fleet.Rollout) fleet.Rollout {
	roll.Names = copyNames(roll.Names)
	return roll
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
