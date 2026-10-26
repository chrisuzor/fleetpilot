package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/api"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

func TestAPI_CreateGetReconcileNoop(t *testing.T) {
	t.Parallel()
	ts := testServer(t)

	code, raw := do(t, ts, http.MethodPost, "/v1/instances", `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	created := decodeInstance(t, raw)
	if created.Actual != nil || created.InSync || created.Name != "web-1" || !strings.HasPrefix(created.ID, "inst_") {
		t.Fatalf("created = %+v", created)
	}
	if created.Desired.Image != "ubuntu-24.04" || created.Desired.Size != "small" || created.Desired.Region != "local" {
		t.Fatalf("desired = %+v", created.Desired)
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID, "")
	if code != http.StatusOK {
		t.Fatalf("get status %d body %s", code, raw)
	}
	got := decodeInstance(t, raw)
	if got.Actual != nil || got.InSync {
		t.Fatalf("get before reconcile = %+v", got)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	first := decodeReconcile(t, raw)
	if len(first.Jobs) != 1 || first.Jobs[0].Op != "create" || first.Jobs[0].Name != "web-1" || first.Jobs[0].Status != "pending" || !strings.HasPrefix(first.Jobs[0].ID, "job_") {
		t.Fatalf("reconcile = %+v", first)
	}
	if waitJob(t, ts, first.Jobs[0].ID).Status != "succeeded" {
		t.Fatal("create job did not succeed")
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID, "")
	if code != http.StatusOK {
		t.Fatalf("get after reconcile status %d body %s", code, raw)
	}
	synced := decodeInstance(t, raw)
	if !synced.InSync || synced.Actual == nil || synced.Actual.Status != "running" || synced.Actual.Size != "small" || !strings.HasPrefix(synced.Actual.ID, "vm_") {
		t.Fatalf("synced = %+v", synced)
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances", "")
	if code != http.StatusOK {
		t.Fatalf("list status %d body %s", code, raw)
	}
	var listed struct {
		Instances []instancePayload `json:"instances"`
	}
	decode(t, raw, &listed)
	if len(listed.Instances) != 1 || listed.Instances[0].ID != created.ID || !listed.Instances[0].InSync {
		t.Fatalf("list = %+v", listed.Instances)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("second reconcile status %d body %s", code, raw)
	}
	if strings.TrimSpace(string(raw)) != `{"jobs":[]}` {
		t.Fatalf("second reconcile body = %s", raw)
	}
}

func TestAPI_UpdateAndDelete(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	id := createAndSync(t, ts)

	code, raw := do(t, ts, http.MethodPatch, "/v1/instances/"+id, `{"image":"ubuntu-24.04","size":"medium","region":"local"}`)
	if code != http.StatusOK {
		t.Fatalf("patch status %d body %s", code, raw)
	}
	patched := decodeInstance(t, raw)
	if patched.InSync || patched.Desired.Size != "medium" || patched.Actual == nil || patched.Actual.Size != "small" {
		t.Fatalf("patched = %+v actual %+v", patched, patched.Actual)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("update reconcile status %d body %s", code, raw)
	}
	updated := decodeReconcile(t, raw)
	if len(updated.Jobs) != 1 || updated.Jobs[0].Op != "update" || updated.Jobs[0].Name != "web-1" {
		t.Fatalf("update reconcile = %+v", updated)
	}
	if waitJob(t, ts, updated.Jobs[0].ID).Status != "succeeded" {
		t.Fatal("update job did not succeed")
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+id, "")
	got := decodeInstance(t, raw)
	if code != http.StatusOK || !got.InSync || got.Actual.Size != "medium" {
		t.Fatalf("after update status %d instance %+v", code, got)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"jobs":[]}` {
		t.Fatalf("noop after update status %d body %s", code, raw)
	}

	code, raw = do(t, ts, http.MethodDelete, "/v1/instances/"+id, "")
	if code != http.StatusNoContent || len(strings.TrimSpace(string(raw))) != 0 {
		t.Fatalf("delete status %d body %q", code, raw)
	}
	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+id, "")
	if code != http.StatusNotFound {
		t.Fatalf("get after delete status %d body %s", code, raw)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("delete reconcile status %d body %s", code, raw)
	}
	deleted := decodeReconcile(t, raw)
	if len(deleted.Jobs) != 1 || deleted.Jobs[0].Op != "delete" || deleted.Jobs[0].Name != "web-1" {
		t.Fatalf("delete reconcile = %+v", deleted)
	}
	if waitJob(t, ts, deleted.Jobs[0].ID).Status != "succeeded" {
		t.Fatal("delete job did not succeed")
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"jobs":[]}` {
		t.Fatalf("noop after delete status %d body %s", code, raw)
	}
}

func TestAPI_RejectsBadRequests(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	cases := []struct {
		name string
		body string
	}{
		{name: "uppercase name", body: `{"name":"Web-1","image":"ubuntu-24.04","size":"small","region":"local"}`},
		{name: "empty image", body: `{"name":"web-1","image":"","size":"small","region":"local"}`},
		{name: "unknown field", body: `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local","status":"running"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, raw := do(t, ts, http.MethodPost, "/v1/instances", tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status %d body %s", code, raw)
			}
		})
	}
}

func TestAPI_DuplicateNameAndMissingInstance(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	body := `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`
	code, raw := do(t, ts, http.MethodPost, "/v1/instances", body)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/instances", body)
	if code != http.StatusConflict {
		t.Fatalf("duplicate status %d body %s", code, raw)
	}
	code, raw = do(t, ts, http.MethodGet, "/v1/instances/inst_missing", "")
	if code != http.StatusNotFound {
		t.Fatalf("missing status %d body %s", code, raw)
	}
}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	ts, _ := testServerWithProvider(t)
	return ts
}

func testServerWithProvider(t *testing.T) (*httptest.Server, *fake.Provider) {
	t.Helper()
	queue := work.NewQueue()
	provider := fake.New()
	store := memory.New()
	cfg := work.DefaultConfig()
	cfg.Poll = time.Millisecond
	cfg.BaseDelay = time.Millisecond
	cfg.MaxDelay = 5 * time.Millisecond
	server := api.New(store, provider, queue, work.NewLock(), provider, store, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go server.RunWorker(ctx)
	go server.RunRoller(ctx)
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	t.Cleanup(cancel)
	return ts, provider
}

func createAndSync(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, raw := do(t, ts, http.MethodPost, "/v1/instances", `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	created := decodeInstance(t, raw)
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	jobs := decodeReconcile(t, raw).Jobs
	if len(jobs) != 1 || jobs[0].Op != "create" {
		t.Fatalf("setup reconcile = %+v", jobs)
	}
	if waitJob(t, ts, jobs[0].ID).Status != "succeeded" {
		t.Fatal("setup job did not succeed")
	}
	return created.ID
}

func waitJob(t *testing.T, ts *httptest.Server, id string) jobPayload {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last jobPayload
	for time.Now().Before(deadline) {
		code, raw := do(t, ts, http.MethodGet, "/v1/jobs/"+id, "")
		if code != http.StatusOK {
			t.Fatalf("job status %d body %s", code, raw)
		}
		last = decodeJob(t, raw)
		if last.Status == "succeeded" || last.Status == "dead" {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish: %+v", id, last)
	return last
}

func do(t *testing.T, ts *httptest.Server, method, path, body string) (int, []byte) {
	t.Helper()
	return doAs(t, ts, "operator", method, path, body)
}

func doAs(t *testing.T, ts *httptest.Server, role, method, path, body string) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if role != "" {
		req.Header.Set("X-FleetPilot-Role", role)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

type instancePayload struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Desired struct {
		Image  string `json:"image"`
		Size   string `json:"size"`
		Region string `json:"region"`
	} `json:"desired"`
	Actual *struct {
		ID     string `json:"id"`
		Image  string `json:"image"`
		Size   string `json:"size"`
		Region string `json:"region"`
		Status string `json:"status"`
	} `json:"actual"`
	InSync bool `json:"in_sync"`
	Health *struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Detail string `json:"detail"`
	} `json:"health"`
}

type reconcilePayload struct {
	Jobs []jobPayload `json:"jobs"`
}

type jobPayload struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Op       string `json:"op"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}

func decodeInstance(t *testing.T, raw []byte) instancePayload {
	t.Helper()
	var payload instancePayload
	decode(t, raw, &payload)
	return payload
}

func decodeReconcile(t *testing.T, raw []byte) reconcilePayload {
	t.Helper()
	var payload reconcilePayload
	decode(t, raw, &payload)
	return payload
}

func decodeJob(t *testing.T, raw []byte) jobPayload {
	t.Helper()
	var payload jobPayload
	decode(t, raw, &payload)
	return payload
}

func TestAPI_HealthCheckIsStored(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	code, raw := do(t, ts, http.MethodPost, "/v1/instances", `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	created := decodeInstance(t, raw)
	if created.Health != nil {
		t.Fatalf("health before a check = %+v", created.Health)
	}
	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID+"/health", "")
	if code != http.StatusNotFound {
		t.Fatalf("health before check status %d body %s", code, raw)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/instances/"+created.ID+"/health", "")
	if code != http.StatusOK {
		t.Fatalf("check status %d body %s", code, raw)
	}
	var checked healthPayload
	decode(t, raw, &checked)
	if checked.Status != "unhealthy" || checked.Detail != "not running" || !strings.HasPrefix(checked.ID, "chk_") {
		t.Fatalf("check = %+v", checked)
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID, "")
	got := decodeInstance(t, raw)
	if code != http.StatusOK || got.Health == nil || got.Health.Status != "unhealthy" || got.Health.ID != checked.ID {
		t.Fatalf("instance health status %d %+v", code, got.Health)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	if waitJob(t, ts, decodeReconcile(t, raw).Jobs[0].ID).Status != "succeeded" {
		t.Fatal("create job did not succeed")
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/instances/"+created.ID+"/health", "")
	if code != http.StatusOK {
		t.Fatalf("second check status %d body %s", code, raw)
	}
	decode(t, raw, &checked)
	if checked.Status != "healthy" || checked.Detail != "running" {
		t.Fatalf("running check = %+v", checked)
	}
	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID+"/health", "")
	var latest healthPayload
	decode(t, raw, &latest)
	if code != http.StatusOK || latest.ID != checked.ID || latest.Status != "healthy" {
		t.Fatalf("latest status %d %+v", code, latest)
	}
}

func TestAPI_RollingUpdateStopsOnFailedHealthCheck(t *testing.T) {
	t.Parallel()
	ts, provider := testServerWithProvider(t)
	for _, name := range []string{"web-1", "web-2", "web-3"} {
		body := `{"name":"` + name + `","image":"ubuntu-24.04","size":"small","region":"local"}`
		code, raw := do(t, ts, http.MethodPost, "/v1/instances", body)
		if code != http.StatusCreated {
			t.Fatalf("create %s status %d body %s", name, code, raw)
		}
	}
	code, raw := do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	for _, job := range decodeReconcile(t, raw).Jobs {
		if waitJob(t, ts, job.ID).Status != "succeeded" {
			t.Fatalf("job %+v did not succeed", job)
		}
	}
	provider.SetUnhealthy("web-2", "injected failure")

	code, raw = do(t, ts, http.MethodPost, "/v1/rollouts", `{"image":"ubuntu-24.04","size":"medium","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("rollout status %d body %s", code, raw)
	}
	started := decodeRollout(t, raw)
	if started.Status != "running" || len(started.Instances) != 3 || !strings.HasPrefix(started.ID, "roll_") {
		t.Fatalf("started = %+v", started)
	}
	roll := waitRollout(t, ts, started.ID)
	if roll.Status != "paused" || roll.PauseReason != "health check failed" {
		t.Fatalf("rollout = %+v", roll)
	}
	if roll.Instances[0].Name != "web-1" || roll.Instances[0].State != "updated" ||
		roll.Instances[1].Name != "web-2" || roll.Instances[1].State != "failed" ||
		roll.Instances[2].Name != "web-3" || roll.Instances[2].State != "pending" {
		t.Fatalf("instances = %+v", roll.Instances)
	}

	byName := map[string]instancePayload{}
	code, raw = do(t, ts, http.MethodGet, "/v1/instances", "")
	if code != http.StatusOK {
		t.Fatalf("list status %d body %s", code, raw)
	}
	var listed struct {
		Instances []instancePayload `json:"instances"`
	}
	decode(t, raw, &listed)
	for _, item := range listed.Instances {
		byName[item.Name] = item
	}
	if byName["web-1"].Desired.Size != "medium" || byName["web-1"].Actual == nil || byName["web-1"].Actual.Size != "medium" || byName["web-1"].Health == nil || byName["web-1"].Health.Status != "healthy" {
		t.Fatalf("web-1 = %+v health %+v", byName["web-1"], byName["web-1"].Health)
	}
	if byName["web-2"].Desired.Size != "medium" || byName["web-2"].Actual == nil || byName["web-2"].Actual.Size != "medium" || byName["web-2"].Health == nil || byName["web-2"].Health.Status != "unhealthy" || byName["web-2"].Health.Detail != "injected failure" {
		t.Fatalf("web-2 = %+v health %+v", byName["web-2"], byName["web-2"].Health)
	}
	if byName["web-3"].Desired.Size != "small" || byName["web-3"].Actual == nil || byName["web-3"].Actual.Size != "small" || byName["web-3"].Health != nil {
		t.Fatalf("web-3 = %+v health %+v", byName["web-3"], byName["web-3"].Health)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"jobs":[]}` {
		t.Fatalf("reconcile after pause status %d body %s", code, raw)
	}
	events := listAudit(t, ts, "reader")
	if !hasAudit(events, "rollout.pause", started.ID, "roller") {
		t.Fatalf("audit = %+v", events)
	}
}

func TestAPI_OperatorCreateReaderForbiddenAndAudited(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	body := `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`

	code, raw := doAs(t, ts, "", http.MethodPost, "/v1/instances", body)
	if code != http.StatusUnauthorized {
		t.Fatalf("missing role status %d body %s", code, raw)
	}
	code, raw = doAs(t, ts, "reader", http.MethodPost, "/v1/instances", body)
	if code != http.StatusForbidden {
		t.Fatalf("reader create status %d body %s", code, raw)
	}
	code, raw = doAs(t, ts, "reader", http.MethodGet, "/v1/instances", "")
	if code != http.StatusOK {
		t.Fatalf("reader list status %d body %s", code, raw)
	}
	code, raw = doAs(t, ts, "reader", http.MethodGet, "/v1/audit", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"events":[]}` {
		t.Fatalf("empty audit status %d body %s", code, raw)
	}

	code, raw = do(t, ts, http.MethodPost, "/v1/instances", body)
	if code != http.StatusCreated {
		t.Fatalf("operator create status %d body %s", code, raw)
	}
	created := decodeInstance(t, raw)
	events := listAudit(t, ts, "reader")
	var found bool
	for _, event := range events {
		if event.Action == "instance.create" && event.Resource == created.ID && event.Actor == "operator" && event.Detail == "web-1" && strings.HasPrefix(event.ID, "aud_") {
			found = true
		}
	}
	if !found {
		t.Fatalf("audit = %+v", events)
	}
}

func waitRollout(t *testing.T, ts *httptest.Server, id string) rolloutPayload {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last rolloutPayload
	for time.Now().Before(deadline) {
		code, raw := do(t, ts, http.MethodGet, "/v1/rollouts/"+id, "")
		if code != http.StatusOK {
			t.Fatalf("rollout status %d body %s", code, raw)
		}
		last = decodeRollout(t, raw)
		if last.Status == "paused" || last.Status == "succeeded" {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("rollout %s did not finish: %+v", id, last)
	return last
}

type healthPayload struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type rolloutPayload struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	PauseReason string `json:"pause_reason"`
	Instances   []struct {
		Name  string `json:"name"`
		State string `json:"state"`
	} `json:"instances"`
}

type auditPayload struct {
	ID       string `json:"id"`
	Actor    string `json:"actor"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Detail   string `json:"detail"`
}

func listAudit(t *testing.T, ts *httptest.Server, role string) []auditPayload {
	t.Helper()
	code, raw := doAs(t, ts, role, http.MethodGet, "/v1/audit", "")
	if code != http.StatusOK {
		t.Fatalf("audit status %d body %s", code, raw)
	}
	var payload struct {
		Events []auditPayload `json:"events"`
	}
	decode(t, raw, &payload)
	return payload.Events
}

func hasAudit(events []auditPayload, action, resource, actor string) bool {
	for _, event := range events {
		if event.Action == action && event.Resource == resource && event.Actor == actor {
			return true
		}
	}
	return false
}

func decodeRollout(t *testing.T, raw []byte) rolloutPayload {
	t.Helper()
	var payload rolloutPayload
	decode(t, raw, &payload)
	return payload
}

func decode(t *testing.T, raw []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}
