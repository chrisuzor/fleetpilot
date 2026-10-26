// Package audit is the append-only record of control-plane actions.
package audit

import (
	"context"
	"errors"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

const (
	// ActorWorker and ActorRoller are process actors, not caller roles.
	ActorWorker = "worker"
	ActorRoller = "roller"

	ActionInstanceCreate = "instance.create"
	ActionInstanceUpdate = "instance.update"
	ActionInstanceDelete = "instance.delete"
	ActionReconcile      = "reconcile"
	ActionJobSucceeded   = "job.succeeded"
	ActionJobDead        = "job.dead"
	ActionRolloutPause   = "rollout.pause"
)

// Event is one stored audit row.
type Event struct {
	ID       string
	At       time.Time
	Actor    string
	Action   string
	Resource string
	Detail   string
}

// Log stores audit events and returns the newest ones first.
type Log interface {
	AppendAudit(ctx context.Context, event Event) (Event, error)
	ListAudit(ctx context.Context, limit int) ([]Event, error)
}

// Prepare fills the id and time and checks the required fields.
func Prepare(event Event) (Event, error) {
	if event.Actor == "" || event.Action == "" || event.Resource == "" {
		return Event{}, errors.New("audit event requires actor, action, and resource")
	}
	if len(event.Detail) > 256 {
		event.Detail = event.Detail[:256]
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	event.At = event.At.UTC().Truncate(time.Microsecond)
	if event.ID == "" {
		id, err := fleet.NewID("aud_")
		if err != nil {
			return Event{}, err
		}
		event.ID = id
	}
	return event, nil
}

// Limit clamps a list size to the API's page size.
func Limit(n int) int {
	if n < 1 {
		return 50
	}
	if n > 100 {
		return 100
	}
	return n
}
