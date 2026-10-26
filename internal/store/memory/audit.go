package memory

import (
	"context"
	"sort"

	"github.com/chrisuzor/fleetpilot/internal/audit"
)

var _ audit.Log = (*Store)(nil)

// AppendAudit stores one event.
func (s *Store) AppendAudit(ctx context.Context, event audit.Event) (audit.Event, error) {
	if err := ctx.Err(); err != nil {
		return audit.Event{}, err
	}
	event, err := audit.Prepare(event)
	if err != nil {
		return audit.Event{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return event, nil
}

// ListAudit returns the newest events, up to limit.
func (s *Store) ListAudit(ctx context.Context, limit int) ([]audit.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit = audit.Limit(limit)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]audit.Event(nil), s.events...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].At.Equal(out[j].At) {
			return out[i].ID > out[j].ID
		}
		return out[i].At.After(out[j].At)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
