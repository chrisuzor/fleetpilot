package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
)

func TestProvider_CreateUpdateDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := fake.New()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}

	created, err := p.Create(ctx, "web-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != fleet.StatusRunning || created.Name != "web-1" {
		t.Fatalf("created = %+v", created)
	}
	created.Spec.Size = "mutated"

	if _, err := p.Create(ctx, "api-1", spec); err != nil {
		t.Fatal(err)
	}
	list, err := p.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "api-1" || list[1].Name != "web-1" {
		t.Fatalf("list = %+v", list)
	}
	if list[1].Spec.Size != "small" {
		t.Fatal("caller mutated provider state")
	}

	updated, err := p.Update(ctx, created.ID, fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if updated.ID != created.ID || updated.Spec.Size != "medium" || updated.Status != fleet.StatusRunning {
		t.Fatalf("updated = %+v", updated)
	}

	if _, err := p.Create(ctx, "web-1", spec); !errors.Is(err, fleet.ErrConflict) {
		t.Fatalf("duplicate create err = %v", err)
	}
	if _, err := p.Update(ctx, "vm_missing", spec); !errors.Is(err, fleet.ErrNotFound) {
		t.Fatalf("missing update err = %v", err)
	}
	if err := p.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, created.ID); !errors.Is(err, fleet.ErrNotFound) {
		t.Fatalf("second delete err = %v", err)
	}
	list, err = p.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "api-1" {
		t.Fatalf("list after delete = %+v", list)
	}
}

func TestProvider_FailNextDoesNotPersistAndUnhealthyCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := fake.New()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	p.FailNext(1)
	if _, err := p.Create(ctx, "web-1", spec); !errors.Is(err, fake.ErrInjected) {
		t.Fatalf("create err = %v", err)
	}
	list, err := p.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("list after injected failure = %+v", list)
	}
	created, err := p.Create(ctx, "web-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	check, err := p.Check(ctx, created.ID)
	if err != nil || check.Status != fleet.HealthHealthy {
		t.Fatalf("check = %+v err %v", check, err)
	}
	p.SetUnhealthy("web-1", "injected failure")
	check, err = p.Check(ctx, created.ID)
	if err != nil || check.Status != fleet.HealthUnhealthy || check.Detail != "injected failure" {
		t.Fatalf("unhealthy check = %+v err %v", check, err)
	}
}
