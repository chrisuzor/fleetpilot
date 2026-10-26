package postgres

import (
	"context"
	"fmt"

	"github.com/chrisuzor/fleetpilot/internal/audit"
)

var _ audit.Log = (*Store)(nil)

// AppendAudit inserts one audit row.
func (s *Store) AppendAudit(ctx context.Context, event audit.Event) (audit.Event, error) {
	event, err := audit.Prepare(event)
	if err != nil {
		return audit.Event{}, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO audit_logs (id, at, actor, action, resource, detail)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, event.ID, event.At, event.Actor, event.Action, event.Resource, event.Detail)
	if err != nil {
		return audit.Event{}, fmt.Errorf("insert audit: %w", err)
	}
	return event, nil
}

// ListAudit returns the newest audit rows, up to limit.
func (s *Store) ListAudit(ctx context.Context, limit int) ([]audit.Event, error) {
	limit = audit.Limit(limit)
	rows, err := s.pool.Query(ctx, `
		SELECT id, at, actor, action, resource, detail
		FROM audit_logs
		ORDER BY at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list audit: %w", err)
	}
	defer rows.Close()
	out := make([]audit.Event, 0)
	for rows.Next() {
		var event audit.Event
		if err := rows.Scan(&event.ID, &event.At, &event.Actor, &event.Action, &event.Resource, &event.Detail); err != nil {
			return nil, fmt.Errorf("scan audit: %w", err)
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list audit: %w", err)
	}
	return out, nil
}
