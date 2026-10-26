package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/chrisuzor/fleetpilot/internal/work"
)

const insertInstanceLockSQL = `
INSERT INTO instance_locks (name) VALUES ($1)
ON CONFLICT (name) DO NOTHING
`

// lockInstanceSQL locks the instance row when this transaction can take it.
// SKIP LOCKED makes a second applier get no row instead of waiting.
const lockInstanceSQL = `
SELECT name
FROM instance_locks
WHERE name = $1
FOR UPDATE SKIP LOCKED
`

var _ work.Locker = (*Store)(nil)

// WithLock runs fn while this transaction holds the row lock for instanceName.
// Another transaction holding that row gets work.ErrLocked, and fn does not run.
// The lock lasts until the transaction commits or rolls back, which is when fn returns.
func (s *Store) WithLock(ctx context.Context, instanceName string, fn func(context.Context) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin instance lock: %w", err)
	}
	defer func() {
		err := tx.Rollback(ctx)
		if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.Error("rollback instance lock", "err", err)
		}
	}()

	if _, err = tx.Exec(ctx, insertInstanceLockSQL, instanceName); err != nil {
		return fmt.Errorf("insert instance lock: %w", err)
	}
	var name string
	err = tx.QueryRow(ctx, lockInstanceSQL, instanceName).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return work.ErrLocked
	}
	if err != nil {
		return fmt.Errorf("lock instance: %w", err)
	}
	if err := fn(ctx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit instance lock: %w", err)
	}
	return nil
}
