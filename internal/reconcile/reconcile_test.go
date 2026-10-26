package reconcile_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/reconcile"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
)

func TestReconcile_SecondRunWithNoSpecChangeIsNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := newCountingProvider()
	rec := reconcile.New(store, provider)
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if _, err := store.Create(ctx, "web-1", spec); err != nil {
		t.Fatal(err)
	}

	first, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := actionSummary(first.Actions), []string{"create web-1"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want %v", got, want)
	}
	if provider.counts() != (opCounts{creates: 1}) {
		t.Fatalf("counts = %+v", provider.counts())
	}
	if first.Actions[0].ProviderID == "" {
		t.Fatal("create action missing provider id")
	}

	desiredBefore, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actualBefore, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	countsBefore := provider.counts()

	second, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Actions) != 0 {
		t.Fatalf("second run actions = %+v, want none", second.Actions)
	}
	desiredAfter, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actualAfter, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(desiredBefore, desiredAfter) {
		t.Fatalf("desired state changed\nbefore %#v\nafter %#v", desiredBefore, desiredAfter)
	}
	if !reflect.DeepEqual(actualBefore, actualAfter) {
		t.Fatalf("actual state changed\nbefore %#v\nafter %#v", actualBefore, actualAfter)
	}
	if provider.counts() != countsBefore {
		t.Fatalf("provider calls on second run: before %+v after %+v", countsBefore, provider.counts())
	}
}

func TestReconcile_UpdateThenNoop(t *testing.T) {
	t.Parallel()
	base := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	cases := []struct {
		name string
		next fleet.Spec
	}{
		{name: "image", next: fleet.Spec{Image: "debian-12", Size: "small", Region: "local"}},
		{name: "size", next: fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"}},
		{name: "region", next: fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "lab"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store, provider, rec := synced(t, ctx, "web-1", base)

			updated, err := store.UpdateSpec(ctx, mustOne(t, store).ID, tc.next)
			if err != nil {
				t.Fatal(err)
			}
			if updated.Spec != tc.next {
				t.Fatalf("stored spec = %+v", updated.Spec)
			}
			before, err := store.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			countsBefore := provider.counts()

			res, err := rec.Reconcile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := actionSummary(res.Actions), []string{"update web-1"}; !slices.Equal(got, want) {
				t.Fatalf("actions = %v, want %v", got, want)
			}
			after, err := store.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("reconcile wrote desired state during update")
			}
			list, err := provider.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 || list[0].Spec != tc.next || list[0].ID != res.Actions[0].ProviderID {
				t.Fatalf("actual = %+v", list)
			}

			again, err := rec.Reconcile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(again.Actions) != 0 {
				t.Fatalf("second run actions = %+v", again.Actions)
			}
			if provider.counts() != (opCounts{creates: countsBefore.creates, updates: countsBefore.updates + 1}) {
				t.Fatalf("counts = %+v", provider.counts())
			}
		})
	}
}

func TestReconcile_DeleteThenNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	store, provider, rec := synced(t, ctx, "web-1", spec)
	id := mustOne(t, store).ID
	if err := store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}

	res, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := actionSummary(res.Actions), []string{"delete web-1"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want %v", got, want)
	}
	list, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("actual after delete = %+v", list)
	}
	desired, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired) != 0 {
		t.Fatalf("desired = %+v", desired)
	}

	again, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Actions) != 0 {
		t.Fatalf("second run actions = %+v", again.Actions)
	}
	if provider.counts().deletes != 1 {
		t.Fatalf("counts = %+v", provider.counts())
	}
}

func TestReconcile_MixedCreateUpdateDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	small := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	medium := fleet.Spec{Image: "ubuntu-24.04", Size: "medium", Region: "local"}
	store := memory.New()
	provider := newCountingProvider()
	if _, err := provider.Create(ctx, "keep", small); err != nil {
		t.Fatal(err)
	}
	orphan, err := provider.Create(ctx, "orphan", small)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "keep", medium); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "new", small); err != nil {
		t.Fatal(err)
	}

	res, err := reconcile.New(store, provider).Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := actionSummary(res.Actions), []string{"update keep", "create new", "delete orphan"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want %v", got, want)
	}
	if res.Actions[2].ProviderID != orphan.ID {
		t.Fatalf("deleted id = %s, want %s", res.Actions[2].ProviderID, orphan.ID)
	}

	again, err := reconcile.New(store, provider).Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Actions) != 0 {
		t.Fatalf("second run actions = %+v", again.Actions)
	}
	list, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "keep" || list[0].Spec != medium || list[1].Name != "new" {
		t.Fatalf("actual = %+v", list)
	}
}

func TestReconcile_PartialCreateThenRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	store := memory.New()
	provider := newCountingProvider()
	provider.failOnCreate = 2
	if _, err := store.Create(ctx, "a", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, "b", spec); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider)

	res, err := rec.Reconcile(ctx)
	if err == nil || !slices.Equal(actionSummary(res.Actions), []string{"create a"}) {
		t.Fatalf("err = %v actions = %+v", err, res.Actions)
	}
	desired, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired) != 2 {
		t.Fatalf("desired rows = %d", len(desired))
	}

	provider.failOnCreate = 0
	res, err = rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := actionSummary(res.Actions), []string{"create b"}; !slices.Equal(got, want) {
		t.Fatalf("retry actions = %v, want %v", got, want)
	}
	again, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Actions) != 0 {
		t.Fatalf("third run actions = %+v", again.Actions)
	}
	if provider.counts() != (opCounts{creates: 2}) {
		t.Fatalf("counts = %+v", provider.counts())
	}
}

func TestReconcile_ConcurrentCreatesOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.New()
	provider := fake.New()
	spec := fleet.Spec{Image: "ubuntu-24.04", Size: "small", Region: "local"}
	if _, err := store.Create(ctx, "web-1", spec); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider)

	const n = 8
	out := make(chan outcome, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rec.Reconcile(ctx)
			out <- outcome{actions: actionSummary(res.Actions), err: err}
		}()
	}
	wg.Wait()
	close(out)

	creates := 0
	for result := range out {
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, action := range result.actions {
			if action != "create web-1" {
				t.Fatalf("unexpected action %q", action)
			}
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("creates = %d, want 1", creates)
	}
	list, err := provider.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("provider instances = %d", len(list))
	}
	follow, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(follow.Actions) != 0 {
		t.Fatalf("follow-up actions = %+v", follow.Actions)
	}
}

func TestReconcile_DuplicateNamesDoNotMutate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("desired", func(t *testing.T) {
		t.Parallel()
		provider := newCountingProvider()
		store := listStore{items: []fleet.Desired{{ID: "1", Name: "web"}, {ID: "2", Name: "web"}}}
		_, err := reconcile.New(store, provider).Reconcile(ctx)
		if err == nil || err.Error() != `duplicate desired instance name "web"` {
			t.Fatalf("err = %v", err)
		}
		if provider.counts() != (opCounts{}) {
			t.Fatalf("counts = %+v", provider.counts())
		}
	})

	t.Run("provider", func(t *testing.T) {
		t.Parallel()
		provider := listProvider{items: []fleet.Actual{{ID: "vm_1", Name: "web"}, {ID: "vm_2", Name: "web"}}}
		_, err := reconcile.New(memory.New(), provider).Reconcile(ctx)
		if err == nil || err.Error() != `duplicate provider instance name "web"` {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestReconcile_EmptyIsNoop(t *testing.T) {
	t.Parallel()
	res, err := reconcile.New(memory.New(), fake.New()).Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Actions) != 0 {
		t.Fatalf("actions = %+v", res.Actions)
	}
}

func TestReconcile_CancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := reconcile.New(memory.New(), fake.New()).Reconcile(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func synced(t *testing.T, ctx context.Context, name string, spec fleet.Spec) (*memory.Store, *countingProvider, *reconcile.Reconciler) {
	t.Helper()
	store := memory.New()
	provider := newCountingProvider()
	if _, err := store.Create(ctx, name, spec); err != nil {
		t.Fatal(err)
	}
	rec := reconcile.New(store, provider)
	res, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := actionSummary(res.Actions), []string{"create " + name}; !slices.Equal(got, want) {
		t.Fatalf("setup actions = %v", got)
	}
	return store, provider, rec
}

func mustOne(t *testing.T, store *memory.Store) fleet.Desired {
	t.Helper()
	list, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("desired rows = %d", len(list))
	}
	return list[0]
}

func actionSummary(actions []fleet.Action) []string {
	out := make([]string, 0, len(actions))
	for _, action := range actions {
		out = append(out, action.Op+" "+action.Name)
	}
	return out
}

type outcome struct {
	actions []string
	err     error
}

type opCounts struct {
	creates int
	updates int
	deletes int
}

type countingProvider struct {
	inner        *fake.Provider
	mu           sync.Mutex
	creates      int
	updates      int
	deletes      int
	createCalls  int
	failOnCreate int
}

func newCountingProvider() *countingProvider {
	return &countingProvider{inner: fake.New()}
}

func (p *countingProvider) counts() opCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return opCounts{creates: p.creates, updates: p.updates, deletes: p.deletes}
}

func (p *countingProvider) Create(ctx context.Context, name string, spec fleet.Spec) (fleet.Actual, error) {
	p.mu.Lock()
	p.createCalls++
	fail := p.failOnCreate > 0 && p.createCalls == p.failOnCreate
	p.mu.Unlock()
	if fail {
		return fleet.Actual{}, errors.New("provider create failed")
	}
	created, err := p.inner.Create(ctx, name, spec)
	if err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	p.creates++
	p.mu.Unlock()
	return created, nil
}

func (p *countingProvider) Update(ctx context.Context, id string, spec fleet.Spec) (fleet.Actual, error) {
	updated, err := p.inner.Update(ctx, id, spec)
	if err != nil {
		return fleet.Actual{}, err
	}
	p.mu.Lock()
	p.updates++
	p.mu.Unlock()
	return updated, nil
}

func (p *countingProvider) Delete(ctx context.Context, id string) error {
	if err := p.inner.Delete(ctx, id); err != nil {
		return err
	}
	p.mu.Lock()
	p.deletes++
	p.mu.Unlock()
	return nil
}

func (p *countingProvider) List(ctx context.Context) ([]fleet.Actual, error) {
	return p.inner.List(ctx)
}

type listStore struct {
	items []fleet.Desired
}

func (s listStore) Create(context.Context, string, fleet.Spec) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected create")
}

func (s listStore) Get(context.Context, string) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected get")
}

func (s listStore) List(context.Context) ([]fleet.Desired, error) {
	return s.items, nil
}

func (s listStore) UpdateSpec(context.Context, string, fleet.Spec) (fleet.Desired, error) {
	return fleet.Desired{}, errors.New("unexpected update")
}

func (s listStore) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}

func (s listStore) Ping(context.Context) error { return nil }

type listProvider struct {
	items []fleet.Actual
}

func (p listProvider) Create(context.Context, string, fleet.Spec) (fleet.Actual, error) {
	return fleet.Actual{}, errors.New("unexpected create")
}

func (p listProvider) Update(context.Context, string, fleet.Spec) (fleet.Actual, error) {
	return fleet.Actual{}, errors.New("unexpected update")
}

func (p listProvider) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}

func (p listProvider) List(context.Context) ([]fleet.Actual, error) {
	return p.items, nil
}
