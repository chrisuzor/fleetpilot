package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

var _ work.Queue = (*Store)(nil)

const jobColumns = `id, instance_name, op, image, size, region, provider_id, attempts, status, run_after, last_error, created_at, updated_at`

// Enqueue inserts a job or refreshes the pending job for that instance.
func (s *Store) Enqueue(ctx context.Context, in work.Job) (work.Job, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return work.Job{}, false, fmt.Errorf("begin enqueue: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, found, err := lockActiveJob(ctx, tx, in.Name)
	if err != nil {
		return work.Job{}, false, err
	}
	now := in.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if found {
		if existing.Status == work.StatusRunning || (existing.Op == in.Op && existing.Spec == in.Spec) {
			if err := tx.Commit(ctx); err != nil {
				return work.Job{}, false, fmt.Errorf("commit enqueue: %w", err)
			}
			return existing, false, nil
		}
		row := tx.QueryRow(ctx, `
			UPDATE jobs
			SET op = $2, image = $3, size = $4, region = $5, provider_id = $6,
			    attempts = 0, last_error = '', run_after = $7, updated_at = $7
			WHERE id = $1 AND status = 'pending'
			RETURNING `+jobColumns, existing.ID, in.Op, in.Spec.Image, in.Spec.Size, in.Spec.Region, in.ProviderID, now)
		updated, err := scanJob(row)
		if err != nil {
			return work.Job{}, false, fmt.Errorf("update job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return work.Job{}, false, fmt.Errorf("commit enqueue: %w", err)
		}
		return updated, true, nil
	}

	if in.ID == "" {
		in.ID, err = fleet.NewID("job_")
		if err != nil {
			return work.Job{}, false, err
		}
	}
	if in.RunAfter.IsZero() {
		in.RunAfter = now
	}
	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, instance_name, op, image, size, region, provider_id,
			status, attempts, run_after, last_error, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			'pending', 0, $8, '', $9, $9
		)
		ON CONFLICT (instance_name) WHERE status IN ('pending', 'running') DO NOTHING
		RETURNING `+jobColumns,
		in.ID, in.Name, in.Op, in.Spec.Image, in.Spec.Size, in.Spec.Region, in.ProviderID,
		in.RunAfter, now)
	inserted, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, found, err = lockActiveJob(ctx, tx, in.Name)
		if err != nil {
			return work.Job{}, false, err
		}
		if !found {
			return work.Job{}, false, errors.New("enqueue lost the job row")
		}
		if err := tx.Commit(ctx); err != nil {
			return work.Job{}, false, fmt.Errorf("commit enqueue: %w", err)
		}
		return existing, false, nil
	}
	if err != nil {
		return work.Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return work.Job{}, false, fmt.Errorf("commit enqueue: %w", err)
	}
	return inserted, true, nil
}

// Claim marks one ready job running.
func (s *Store) Claim(ctx context.Context, now time.Time) (work.Job, bool, error) {
	staleBefore := now.Add(-work.StaleAfter)
	row := s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id
			FROM jobs
			WHERE (status = 'pending' AND run_after <= $1)
			   OR (status = 'running' AND updated_at <= $2)
			ORDER BY created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET status = 'running', updated_at = $1
		FROM next
		WHERE jobs.id = next.id
		RETURNING jobs.id, jobs.instance_name, jobs.op, jobs.image, jobs.size, jobs.region,
		          jobs.provider_id, jobs.attempts, jobs.status, jobs.run_after, jobs.last_error,
		          jobs.created_at, jobs.updated_at`, now, staleBefore)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return work.Job{}, false, nil
	}
	if err != nil {
		return work.Job{}, false, fmt.Errorf("claim job: %w", err)
	}
	return job, true, nil
}

// GetJob returns one job by id.
func (s *Store) GetJob(ctx context.Context, id string) (work.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return work.Job{}, work.ErrNotFound
	}
	if err != nil {
		return work.Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

// Succeed marks a running job succeeded.
func (s *Store) Succeed(ctx context.Context, id string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', last_error = '', updated_at = $2
		WHERE id = $1 AND status = 'running'
	`, id, now)
	if err != nil {
		return fmt.Errorf("succeed job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.allowTerminal(ctx, id, work.StatusSucceeded)
	}
	return nil
}

// Retry schedules a running job again.
func (s *Store) Retry(ctx context.Context, id string, now, runAfter time.Time, attempts int, lastError string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending', attempts = $2, run_after = $3, last_error = $4, updated_at = $5
		WHERE id = $1 AND status = 'running'
	`, id, attempts, runAfter, lastError, now)
	if err != nil {
		return fmt.Errorf("retry job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	return nil
}

// Dead stops retries for a running job.
func (s *Store) Dead(ctx context.Context, id string, now time.Time, attempts int, lastError string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead', attempts = $2, last_error = $3, updated_at = $4
		WHERE id = $1 AND status = 'running'
	`, id, attempts, lastError, now)
	if err != nil {
		return fmt.Errorf("dead job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.allowTerminal(ctx, id, work.StatusDead)
	}
	return nil
}

// Release returns a running job to pending without changing attempts.
func (s *Store) Release(ctx context.Context, id string, now, runAfter time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending', run_after = $2, updated_at = $3
		WHERE id = $1 AND status = 'running'
	`, id, runAfter, now)
	if err != nil {
		return fmt.Errorf("release job: %w", err)
	}
	return nil
}

func (s *Store) allowTerminal(ctx context.Context, id, status string) error {
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if job.Status == status {
		return nil
	}
	return nil
}

func lockActiveJob(ctx context.Context, tx pgx.Tx, name string) (work.Job, bool, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE instance_name = $1 AND status IN ('pending', 'running')
		FOR UPDATE
	`, name)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return work.Job{}, false, nil
	}
	if err != nil {
		return work.Job{}, false, fmt.Errorf("lock active job: %w", err)
	}
	return job, true, nil
}

func scanJob(row scannable) (work.Job, error) {
	var job work.Job
	err := row.Scan(
		&job.ID,
		&job.Name,
		&job.Op,
		&job.Spec.Image,
		&job.Spec.Size,
		&job.Spec.Region,
		&job.ProviderID,
		&job.Attempts,
		&job.Status,
		&job.RunAfter,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	return job, err
}
