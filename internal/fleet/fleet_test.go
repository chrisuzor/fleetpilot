package fleet_test

import (
	"strings"
	"testing"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
)

func TestValidate_ExampleSpec(t *testing.T) {
	t.Parallel()
	if err := fleet.ValidateName("web-1"); err != nil {
		t.Fatal(err)
	}
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_RejectsBadNameAndSpec(t *testing.T) {
	t.Parallel()
	if err := fleet.ValidateName("Web-1"); err == nil {
		t.Fatal("expected uppercase name to fail")
	}
	spec := fleet.Spec{Image: "", Size: "small", Region: "local"}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected empty image to fail")
	}
}

func TestDesiredByName_RejectsDuplicates(t *testing.T) {
	t.Parallel()
	_, err := fleet.DesiredByName([]fleet.Desired{{Name: "web"}, {Name: "web"}})
	if err == nil || !strings.Contains(err.Error(), `duplicate desired instance name "web"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestActualByName_RejectsDuplicates(t *testing.T) {
	t.Parallel()
	_, err := fleet.ActualByName([]fleet.Actual{{Name: "web"}, {Name: "web"}})
	if err == nil || !strings.Contains(err.Error(), `duplicate provider instance name "web"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewID_PrefixAndLength(t *testing.T) {
	t.Parallel()
	id, err := fleet.NewID("inst_")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "inst_") || len(id) != len("inst_")+16 {
		t.Fatalf("id = %q", id)
	}
}
