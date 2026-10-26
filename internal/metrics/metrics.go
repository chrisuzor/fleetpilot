// Package metrics is the Prometheus registry for one API process.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the counters and the in-flight gauge.
// A nil *Metrics ignores every call so tests can omit it.
type Metrics struct {
	reg       *prometheus.Registry
	http      *prometheus.CounterVec
	reconcile *prometheus.CounterVec
	outcomes  *prometheus.CounterVec
	inFlight  prometheus.Gauge
}

// New registers the process metrics on a private registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		http: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpilot_http_requests_total",
			Help: "HTTP requests by method, route, and status code.",
		}, []string{"method", "path", "code"}),
		reconcile: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpilot_reconcile_runs_total",
			Help: "Reconcile runs by result.",
		}, []string{"result"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpilot_job_outcomes_total",
			Help: "Provisioning jobs that succeeded or became dead.",
		}, []string{"outcome"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetpilot_jobs_in_flight",
			Help: "Jobs currently applying a provider call.",
		}),
	}
	reg.MustRegister(m.http, m.reconcile, m.outcomes, m.inFlight)
	return m
}

// Handler serves the registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveHTTP counts one completed request.
func (m *Metrics) ObserveHTTP(method, path string, code int) {
	if m == nil {
		return
	}
	if path == "" {
		path = "unmatched"
	}
	m.http.WithLabelValues(method, path, strconv.Itoa(code)).Inc()
}

// ObserveReconcile counts one reconcile run. result is ok or error.
func (m *Metrics) ObserveReconcile(result string) {
	if m == nil {
		return
	}
	m.reconcile.WithLabelValues(result).Inc()
}

// JobOutcome counts a terminal job. outcome is succeeded or dead.
func (m *Metrics) JobOutcome(outcome string) {
	if m == nil {
		return
	}
	m.outcomes.WithLabelValues(outcome).Inc()
}

// JobStarted marks one job inside the provider call.
func (m *Metrics) JobStarted() {
	if m == nil {
		return
	}
	m.inFlight.Inc()
}

// JobFinished marks that provider call done.
func (m *Metrics) JobFinished() {
	if m == nil {
		return
	}
	m.inFlight.Dec()
}
