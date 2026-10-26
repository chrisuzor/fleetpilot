// Package fleet is the desired-state model and the provider contract.
package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	StatusRunning = "running"

	OpCreate = "create"
	OpUpdate = "update"
	OpDelete = "delete"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")

	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	valuePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// Spec is the desired shape of a virtual machine.
// The fields are labels. This slice does not interpret them as a cloud catalog.
type Spec struct {
	Image  string
	Size   string
	Region string
}

// Equal reports whether two specs are the same.
func (s Spec) Equal(other Spec) bool {
	return s == other
}

// Validate checks the labels the API accepts.
func (s Spec) Validate() error {
	if err := validateValue("image", s.Image); err != nil {
		return err
	}
	if err := validateValue("size", s.Size); err != nil {
		return err
	}
	return validateValue("region", s.Region)
}

// ValidateName checks an instance name. Names are the match key between
// desired and actual state, so they are immutable after create.
func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return errors.New("name must start with a lowercase letter and contain only lowercase letters, digits, and hyphens (max 63 characters)")
	}
	return nil
}

func validateValue(field, value string) error {
	if !valuePattern.MatchString(value) {
		return fmt.Errorf("%s must be 1-128 characters and use only letters, digits, and . _ : -", field)
	}
	return nil
}

// Desired is an instance the control plane wants to exist.
type Desired struct {
	ID        string
	Name      string
	Spec      Spec
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Actual is an instance the provider currently has.
type Actual struct {
	ID     string
	Name   string
	Spec   Spec
	Status string
}

// Store is the desired-state database.
// Reconcile only lists it. Creates, updates, and deletes of desired state
// happen through the API.
type Store interface {
	Create(ctx context.Context, name string, spec Spec) (Desired, error)
	Get(ctx context.Context, id string) (Desired, error)
	List(ctx context.Context) ([]Desired, error)
	UpdateSpec(ctx context.Context, id string, spec Spec) (Desired, error)
	Delete(ctx context.Context, id string) error
	Ping(ctx context.Context) error
}

// Provider is the actual-state source for VM instances.
// The first slice ships an in-process fake. A later provider implements
// this same interface.
type Provider interface {
	Create(ctx context.Context, name string, spec Spec) (Actual, error)
	Update(ctx context.Context, id string, spec Spec) (Actual, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]Actual, error)
}

// DesiredByName indexes desired instances by name.
func DesiredByName(items []Desired) (map[string]Desired, error) {
	out := make(map[string]Desired, len(items))
	for _, item := range items {
		if _, exists := out[item.Name]; exists {
			return nil, fmt.Errorf("duplicate desired instance name %q", item.Name)
		}
		out[item.Name] = item
	}
	return out, nil
}

// ActualByName indexes provider instances by name.
func ActualByName(items []Actual) (map[string]Actual, error) {
	out := make(map[string]Actual, len(items))
	for _, item := range items {
		if _, exists := out[item.Name]; exists {
			return nil, fmt.Errorf("duplicate provider instance name %q", item.Name)
		}
		out[item.Name] = item
	}
	return out, nil
}

// NewID returns prefix plus 16 random hex characters.
func NewID(prefix string) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("new id: %w", err)
	}
	return prefix + hex.EncodeToString(buf[:]), nil
}
