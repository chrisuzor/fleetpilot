package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/chrisuzor/fleetpilot/internal/api"
	"github.com/chrisuzor/fleetpilot/internal/provider/fake"
	"github.com/chrisuzor/fleetpilot/internal/store/memory"
	"github.com/chrisuzor/fleetpilot/internal/telemetry"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

func TestAPI_JobSuccessMovesOutcomeMetric(t *testing.T) {
	t.Parallel()
	ts := testServer(t)
	const series = `fleetpilot_job_outcomes_total{outcome="succeeded"}`
	before := seriesValue(scrapeMetrics(t, ts), series)

	code, raw := do(t, ts, http.MethodPost, "/v1/instances", `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	jobs := decodeReconcile(t, raw).Jobs
	if len(jobs) != 1 {
		t.Fatalf("jobs = %+v", jobs)
	}
	if waitJob(t, ts, jobs[0].ID).Status != "succeeded" {
		t.Fatal("job did not succeed")
	}

	after := seriesValue(scrapeMetrics(t, ts), series)
	if after < before+1 {
		t.Fatalf("succeeded outcomes %v, want at least %v", after, before+1)
	}
}

func TestAPI_ReconcileProducesSpan(t *testing.T) {
	t.Parallel()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
	)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	queue := work.NewQueue()
	store := memory.New()
	cfg := work.DefaultConfig()
	cfg.Poll = time.Millisecond
	server := api.New(store, fake.New(), queue, work.NewLock(), fake.New(), store, cfg)
	server.SetTracer(provider.Tracer(telemetry.TracerName))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go server.RunWorker(ctx)
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)

	code, raw := do(t, ts, http.MethodPost, "/v1/instances", `{"name":"web-1","image":"ubuntu-24.04","size":"small","region":"local"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status %d body %s", code, raw)
	}
	code, raw = do(t, ts, http.MethodPost, "/v1/reconcile", "")
	if code != http.StatusOK {
		t.Fatalf("reconcile status %d body %s", code, raw)
	}
	parent, ok := findSpan(exporter, "reconcile")
	if !ok {
		t.Fatal("reconcile span was not recorded")
	}
	jobs := decodeReconcile(t, raw).Jobs
	if len(jobs) != 1 {
		t.Fatalf("jobs = %+v", jobs)
	}
	if waitJob(t, ts, jobs[0].ID).Status != "succeeded" {
		t.Fatal("job did not succeed")
	}

	deadline := time.Now().Add(2 * time.Second)
	var child tracetest.SpanStub
	var found bool
	for time.Now().Before(deadline) {
		for _, span := range exporter.GetSpans() {
			if span.Name != "job.apply" {
				continue
			}
			if span.SpanContext.TraceID() != parent.SpanContext.TraceID() {
				continue
			}
			child = span
			found = true
		}
		if found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !found {
		t.Fatal("job.apply span was not recorded on the reconcile trace")
	}
	if child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Fatalf("job.apply parent %s, reconcile span %s", child.Parent.SpanID(), parent.SpanContext.SpanID())
	}
}

func scrapeMetrics(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, raw := doAs(t, ts, "", http.MethodGet, "/metrics", "")
	if code != http.StatusOK {
		t.Fatalf("metrics status %d body %s", code, raw)
	}
	return string(raw)
}

func seriesValue(body, series string) float64 {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != series {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		return value
	}
	return 0
}

func findSpan(exporter *tracetest.InMemoryExporter, name string) (tracetest.SpanStub, bool) {
	var found tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			found = span
		}
	}
	if found.Name == "" {
		return tracetest.SpanStub{}, false
	}
	return found, true
}
