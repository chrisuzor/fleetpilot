package work

import (
	"testing"
	"time"
)

func TestBackoff_DoublesUntilCap(t *testing.T) {
	t.Parallel()
	base := 100 * time.Millisecond
	max := 5 * time.Second
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		5 * time.Second,
		5 * time.Second,
	}
	for attempt, delay := range want {
		got := Backoff(attempt+1, base, max)
		if got != delay {
			t.Fatalf("attempt %d delay = %s, want %s", attempt+1, got, delay)
		}
	}
}
