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
	queue := work.NewQueue()
	provider := fake.New()
	cfg := work.DefaultConfig()
	cfg.Poll = time.Millisecond
	cfg.BaseDelay = time.Millisecond
	cfg.MaxDelay = 5 * time.Millisecond
	server := api.New(memory.New(), provider, queue, work.NewLock(), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go server.RunWorker(ctx)
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	t.Cleanup(cancel)
	return ts
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
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, reader)
	if err != nil {
		t.Fatal(err)
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

func decode(t *testing.T, raw []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}
