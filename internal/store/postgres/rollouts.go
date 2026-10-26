package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

// CreateRollout inserts a running rollout.
// A second running rollout returns fleet.ErrConflict.
func (s *Store) CreateRollout(ctx context.Context, spec fleet.Spec, names []string, at time.Time) (fleet.Rollout, error) {
	id, err := fleet.NewID("roll_")
	if err != nil {
		return fleet.Rollout{}, err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)
	copied := make([]string, len(names))
	copy(copied, names)
	roll := fleet.Rollout{
		ID:        id,
		Spec:      spec,
		Status:    fleet.RolloutRunning,
		Names:     copied,
		Phase:     fleet.PhaseIdle,
		CreatedAt: at,
		UpdatedAt: at,
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO rollouts (
			id, image, size, region, status, pause_reason, instance_names,
			next_index, phase, job_id, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, '', $6,
			0, $7, '', $8, $8
		)
	`, roll.ID, roll.Spec.Image, roll.Spec.Size, roll.Spec.Region, roll.Status, roll.Names, roll.Phase, roll.CreatedAt)
	if err != nil {
		if isRunningRolloutConflict(err) {
			return fleet.Rollout{}, fleet.ErrConflict
		}
		return fleet.Rollout{}, fmt.Errorf("insert rollout: %w", err)
	}
	return roll, nil
}

// GetRollout returns one rollout by id.
func (s *Store) GetRollout(ctx context.Context, id string) (fleet.Rollout, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, image, size, region, status, pause_reason, instance_names,
		       next_index, phase, job_id, created_at, updated_at
		FROM rollouts
		WHERE id = $1
	`, id)
	roll, err := scanRollout(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return fleet.Rollout{}, fleet.ErrNotFound
	}
	if err != nil {
		return fleet.Rollout{}, fmt.Errorf("get rollout: %w", err)
	}
	return roll, nil
}

// SaveRollout writes status, progress, and the pause reason.
func (s *Store) SaveRollout(ctx context.Context, rollout fleet.Rollout) error {
	now := time.Now().UTC().Truncate(time.Microsecond)
	names := rollout.Names
	if names == nil {
		names = []string{}
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE rollouts
		SET status = $2, pause_reason = $3, instance_names = $4, next_index = $5,
		    phase = $6, job_id = $7, updated_at = $8
		WHERE id = $1
	`, rollout.ID, rollout.Status, rollout.PauseReason, names, rollout.Next, rollout.Phase, rollout.JobID, now)
	if err != nil {
		return fmt.Errorf("save rollout: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fleet.ErrNotFound
	}
	return nil
}

// ListRunningRollouts returns rollouts the roller should advance.
func (s *Store) ListRunningRollouts(ctx context.Context) ([]fleet.Rollout, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, image, size, region, status, pause_reason, instance_names,
		       next_index, phase, job_id, created_at, updated_at
		FROM rollouts
		WHERE status = $1
		ORDER BY created_at, id
	`, fleet.RolloutRunning)
	if err != nil {
		return nil, fmt.Errorf("list rollouts: %w", err)
	}
	defer rows.Close()
	out := make([]fleet.Rollout, 0)
	for rows.Next() {
		roll, err := scanRollout(rows)
		if err != nil {
			return nil, fmt.Errorf("scan rollout: %w", err)
		}
		out = append(out, roll)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rollouts: %w", err)
	}
	return out, nil
}

func scanRollout(row scannable) (fleet.Rollout, error) {
	var roll fleet.Rollout
	err := row.Scan(
		&roll.ID,
		&roll.Spec.Image,
		&roll.Spec.Size,
		&roll.Spec.Region,
		&roll.Status,
		&roll.PauseReason,
		&roll.Names,
		&roll.Next,
		&roll.Phase,
		&roll.JobID,
		&roll.CreatedAt,
		&roll.UpdatedAt,
	)
	if err != nil {
		return fleet.Rollout{}, err
	}
	if roll.Names == nil {
		roll.Names = []string{}
	}
	return roll, nil
}

func isRunningRolloutConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "rollouts_one_running"
}
