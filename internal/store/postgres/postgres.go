// Package postgres stores desired instance state.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	_ "embed"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

//go:embed schema.sql
var schemaSQL string

// Store is the PostgreSQL desired-state store.
type Store struct {
	pool *pgxpool.Pool
}

var _ fleet.Store = (*Store)(nil)

// Connect opens a pool, retrying until PostgreSQL accepts connections.
// The database URL is not included in errors.
func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	var last error
	for attempt := 1; attempt <= 40; attempt++ {
		cfg, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			return nil, fmt.Errorf("parse database url: %w", err)
		}
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'")
			return err
		}
		// Each in-flight job holds one pool connection for its instance lock.
		if cfg.MaxConns < 16 {
			cfg.MaxConns = 16
		}
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return &Store{pool: pool}, nil
			}
			pool.Close()
		}
		last = err
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("connect database: %w", last)
}

// Migrate creates the desired-state table if it does not exist.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// Close closes the pool.
func (s *Store) Close() {
	s.pool.Close()
}

// Create inserts a desired instance.
func (s *Store) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Desired, error) {
	id, err := fleet.NewID("inst_")
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
	_, err = s.pool.Exec(ctx, `
		INSERT INTO instances (id, name, image, size, region, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, item.ID, item.Name, item.Spec.Image, item.Spec.Size, item.Spec.Region, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		if isNameConflict(err) {
			return fleet.Desired{}, fleet.ErrConflict
		}
		return fleet.Desired{}, fmt.Errorf("insert instance: %w", err)
	}
	return item, nil
}

// Get returns one desired instance by id.
func (s *Store) Get(ctx context.Context, id string) (fleet.Desired, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, image, size, region, created_at, updated_at
		FROM instances
		WHERE id = $1
	`, id)
	item, err := scanDesired(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return fleet.Desired{}, fleet.ErrNotFound
	}
	if err != nil {
		return fleet.Desired{}, fmt.Errorf("get instance: %w", err)
	}
	return item, nil
}

// List returns desired instances sorted by name.
func (s *Store) List(ctx context.Context) ([]fleet.Desired, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, image, size, region, created_at, updated_at
		FROM instances
		ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("list instances: %w", err)
	}
	defer rows.Close()
	out := make([]fleet.Desired, 0)
	for rows.Next() {
		item, err := scanDesired(rows)
		if err != nil {
			return nil, fmt.Errorf("scan instance: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list instances: %w", err)
	}
	return out, nil
}

// UpdateSpec replaces the spec of an existing desired instance.
func (s *Store) UpdateSpec(ctx context.Context, id string, spec fleet.Spec) (fleet.Desired, error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	row := s.pool.QueryRow(ctx, `
		UPDATE instances
		SET image = $2, size = $3, region = $4, updated_at = $5
		WHERE id = $1
		RETURNING id, name, image, size, region, created_at, updated_at
	`, id, spec.Image, spec.Size, spec.Region, now)
	item, err := scanDesired(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return fleet.Desired{}, fleet.ErrNotFound
	}
	if err != nil {
		return fleet.Desired{}, fmt.Errorf("update instance: %w", err)
	}
	return item, nil
}

// Delete removes a desired instance.
func (s *Store) Delete(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM instances WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete instance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fleet.ErrNotFound
	}
	return nil
}

// Ping checks the database connection.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type scannable interface {
	Scan(dest ...any) error
}

func scanDesired(row scannable) (fleet.Desired, error) {
	var item fleet.Desired
	err := row.Scan(
		&item.ID,
		&item.Name,
		&item.Spec.Image,
		&item.Spec.Size,
		&item.Spec.Region,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	return item, err
}

func isNameConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "instances_name_key"
}
