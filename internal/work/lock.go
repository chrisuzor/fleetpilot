package work

import (
	"context"
	"sync"
)

// MemoryLock is the in-memory try-lock used by tests.
// It has the same result as the Postgres lock: the second caller for a name
// gets ErrLocked and its function does not run.
type MemoryLock struct {
	mu   sync.Mutex
	held map[string]struct{}
}

var _ Locker = (*MemoryLock)(nil)

// NewLock returns a lock with nothing held.
func NewLock() *MemoryLock {
	return &MemoryLock{held: make(map[string]struct{})}
}

// WithLock runs fn while name is held.
// A name that is already held returns ErrLocked without calling fn.
func (l *MemoryLock) WithLock(ctx context.Context, instanceName string, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if _, ok := l.held[instanceName]; ok {
		l.mu.Unlock()
		return ErrLocked
	}
	l.held[instanceName] = struct{}{}
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		delete(l.held, instanceName)
		l.mu.Unlock()
	}()
	return fn(ctx)
}
