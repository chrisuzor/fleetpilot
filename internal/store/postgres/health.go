package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// RecordHealth appends a check for a desired instance.
func (s *Store) RecordHealth(ctx context.Context, instanceID, status, detail string, at time.Time) (fleet.HealthCheck, error) {
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
	_, err = s.pool.Exec(ctx, `
		INSERT INTO health_checks (id, instance_id, status, detail, checked_at)
		VALUES ($1, $2, $3, $4, $5)
	`, check.ID, check.InstanceID, check.Status, check.Detail, check.CheckedAt)
	if err != nil {
		return fleet.HealthCheck{}, fmt.Errorf("insert health check: %w", err)
	}
	return check, nil
}

// LatestHealth returns the newest stored check for instanceID.
func (s *Store) LatestHealth(ctx context.Context, instanceID string) (fleet.HealthCheck, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, instance_id, status, detail, checked_at
		FROM health_checks
		WHERE instance_id = $1
		ORDER BY checked_at DESC, id DESC
		LIMIT 1
	`, instanceID)
	var check fleet.HealthCheck
	err := row.Scan(&check.ID, &check.InstanceID, &check.Status, &check.Detail, &check.CheckedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fleet.HealthCheck{}, fleet.ErrNotFound
	}
	if err != nil {
		return fleet.HealthCheck{}, fmt.Errorf("latest health: %w", err)
	}
	return check, nil
}
