package postgres

import (
	"strings"
	"testing"
)

func TestInstanceLockSQL_SkipsAHeldRow(t *testing.T) {
	if !strings.Contains(lockInstanceSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatalf("lock sql = %s", lockInstanceSQL)
	}
	if !strings.Contains(insertInstanceLockSQL, "INSERT INTO instance_locks") {
		t.Fatalf("insert sql = %s", insertInstanceLockSQL)
	}
}
