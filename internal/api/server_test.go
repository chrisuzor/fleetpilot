package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chrisuzor/fleetpilot/internal/api"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
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
	if len(first.Actions) != 1 || first.Actions[0].Op != "create" || first.Actions[0].Name != "web-1" || !strings.HasPrefix(first.Actions[0].ProviderID, "vm_") {
		t.Fatalf("reconcile = %+v", first)
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+created.ID, "")
	if code != http.StatusOK {
		t.Fatalf("get after reconcile status %d body %s", code, raw)
	}
	synced := decodeInstance(t, raw)
	if !synced.InSync || synced.Actual == nil || synced.Actual.Status != "running" || synced.Actual.Size != "small" {
		t.Fatalf("synced = %+v", synced)
	}
	if synced.Actual.ID != first.Actions[0].ProviderID {
		t.Fatalf("actual id = %s, want %s", synced.Actual.ID, first.Actions[0].ProviderID)
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
	if strings.TrimSpace(string(raw)) != `{"actions":[]}` {
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
	if len(updated.Actions) != 1 || updated.Actions[0].Op != "update" || updated.Actions[0].Name != "web-1" {
		t.Fatalf("update reconcile = %+v", updated)
	}

	code, raw = do(t, ts, http.MethodGet, "/v1/instances/"+id, "")
	got := decodeInstance(t, raw)
	if code != http.StatusOK || !got.InSync || got.Actual.Size != "medium" {
		t.Fatalf("after update status %d instance %+v", code, got)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"actions":[]}` {
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
	if len(deleted.Actions) != 1 || deleted.Actions[0].Op != "delete" || deleted.Actions[0].Name != "web-1" {
		t.Fatalf("delete reconcile = %+v", deleted)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK || strings.TrimSpace(string(raw)) != `{"actions":[]}` {
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
	ts := httptest.NewServer(api.New(memory.New(), fake.New()).Handler())
	t.Cleanup(ts.Close)
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
	if actions := decodeReconcile(t, raw).Actions; len(actions) != 1 || actions[0].Op != "create" {
		t.Fatalf("setup reconcile = %+v", actions)
	}
	return created.ID
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
	Actions []struct {
		Op         string `json:"op"`
		Name       string `json:"name"`
		ProviderID string `json:"provider_id"`
	} `json:"actions"`
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

func decode(t *testing.T, raw []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}
