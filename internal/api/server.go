// Package api is the HTTP control plane.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/chrisuzor/fleetpilot/internal/fleet"
	"github.com/chrisuzor/fleetpilot/internal/reconcile"
	"github.com/chrisuzor/fleetpilot/internal/rollout"
	"github.com/chrisuzor/fleetpilot/internal/work"
)

// Server serves desired-state writes, health checks, and rolling updates.
type Server struct {
	store      fleet.Store
	provider   fleet.Provider
	queue      work.Queue
	checker    fleet.Checker
	reconciler *reconcile.Reconciler
	worker     *work.Worker
	roller     *rollout.Roller
	poll       time.Duration
}

// New wires the API to desired state, a provider, the job worker, and the roller.
func New(store fleet.Store, provider fleet.Provider, queue work.Queue, locker work.Locker, checker fleet.Checker, cfg work.Config) *Server {
	poll := cfg.Poll
	if poll <= 0 {
		poll = work.DefaultPoll
	}
	return &Server{
		store:      store,
		provider:   provider,
		queue:      queue,
		checker:    checker,
		reconciler: reconcile.New(store, provider, queue),
		worker:     work.NewWorker(queue, provider, locker, cfg),
		roller:     rollout.New(store, provider, queue, checker),
		poll:       poll,
	}
}

// RunWorker applies queued jobs until ctx is cancelled.
func (s *Server) RunWorker(ctx context.Context) {
	s.worker.Run(ctx)
}

// RunRoller advances rolling updates until ctx is cancelled.
func (s *Server) RunRoller(ctx context.Context) {
	s.roller.Run(ctx, s.poll)
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/instances", s.create)
	mux.HandleFunc("GET /v1/instances", s.list)
	mux.HandleFunc("GET /v1/instances/{id}", s.get)
	mux.HandleFunc("PATCH /v1/instances/{id}", s.patch)
	mux.HandleFunc("DELETE /v1/instances/{id}", s.delete)
	mux.HandleFunc("POST /v1/instances/{id}/health", s.checkHealth)
	mux.HandleFunc("GET /v1/instances/{id}/health", s.getHealth)
	mux.HandleFunc("POST /v1/reconcile", s.reconcile)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /v1/rollouts", s.startRollout)
	mux.HandleFunc("GET /v1/rollouts/{id}", s.getRollout)
	return logRequests(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		slog.Error("health", "err", err)
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Image  string `json:"image"`
		Size   string `json:"size"`
		Region string `json:"region"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if err := fleet.ValidateName(req.Name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	spec := fleet.Spec{Image: req.Image, Size: req.Size, Region: req.Region}
	if err := spec.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	created, err := s.store.Create(r.Context(), req.Name, spec)
	if err != nil {
		writeStoreError(w, "create instance", err)
		return
	}
	view, err := s.view(r.Context(), created)
	if err != nil {
		slog.Error("read actual", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Location", "/v1/instances/"+created.ID)
	writeJSON(w, http.StatusCreated, view)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	desired, err := s.store.List(r.Context())
	if err != nil {
		writeStoreError(w, "list instances", err)
		return
	}
	actual, err := s.actualByName(r.Context())
	if err != nil {
		slog.Error("list actual", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	out := make([]instanceResponse, 0, len(desired))
	for _, item := range desired {
		var got *fleet.Actual
		if inst, ok := actual[item.Name]; ok {
			got = &inst
		}
		view, err := s.withHealth(r.Context(), item, toInstance(item, got))
		if err != nil {
			slog.Error("list health", "err", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		out = append(out, view)
	}
	writeJSON(w, http.StatusOK, listResponse{Instances: out})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, "get instance", err)
		return
	}
	view, err := s.view(r.Context(), item)
	if err != nil {
		slog.Error("read actual", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) patch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Image  string `json:"image"`
		Size   string `json:"size"`
		Region string `json:"region"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	spec := fleet.Spec{Image: req.Image, Size: req.Size, Region: req.Region}
	if err := spec.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.store.UpdateSpec(r.Context(), r.PathValue("id"), spec)
	if err != nil {
		writeStoreError(w, "update instance", err)
		return
	}
	view, err := s.view(r.Context(), updated)
	if err != nil {
		slog.Error("read actual", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, "delete instance", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	result, err := s.reconciler.Reconcile(r.Context())
	if err != nil {
		slog.Error("reconcile", "err", err, "enqueued", len(result.Jobs))
		writeError(w, http.StatusInternalServerError, "reconcile failed")
		return
	}
	writeJSON(w, http.StatusOK, reconcileResponse{Jobs: toJobs(result.Jobs)})
}

func (s *Server) checkHealth(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, "get instance", err)
		return
	}
	status, detail, err := s.probe(r.Context(), item.Name)
	if err != nil {
		slog.Error("health check", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	saved, err := s.store.RecordHealth(r.Context(), item.ID, status, detail, time.Now().UTC())
	if err != nil {
		slog.Error("record health", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toHealth(saved))
}

func (s *Server) getHealth(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, "get instance", err)
		return
	}
	check, err := s.store.LatestHealth(r.Context(), item.ID)
	if err != nil {
		if errors.Is(err, fleet.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		slog.Error("get health", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toHealth(check))
}

func (s *Server) startRollout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Image  string `json:"image"`
		Size   string `json:"size"`
		Region string `json:"region"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	spec := fleet.Spec{Image: req.Image, Size: req.Size, Region: req.Region}
	if err := spec.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	roll, err := s.roller.Start(r.Context(), spec)
	if err != nil {
		if errors.Is(err, fleet.ErrConflict) {
			writeError(w, http.StatusConflict, "a rollout is already running")
			return
		}
		slog.Error("start rollout", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Location", "/v1/rollouts/"+roll.ID)
	writeJSON(w, http.StatusCreated, toRollout(roll))
}

func (s *Server) getRollout(w http.ResponseWriter, r *http.Request) {
	roll, err := s.store.GetRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, "get rollout", err)
		return
	}
	writeJSON(w, http.StatusOK, toRollout(roll))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.queue.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, work.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		slog.Error("get job", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toJob(job))
}

func (s *Server) view(ctx context.Context, item fleet.Desired) (instanceResponse, error) {
	actual, err := s.actualByName(ctx)
	if err != nil {
		return instanceResponse{}, err
	}
	var got *fleet.Actual
	if inst, ok := actual[item.Name]; ok {
		got = &inst
	}
	return s.withHealth(ctx, item, toInstance(item, got))
}

func (s *Server) probe(ctx context.Context, name string) (string, string, error) {
	actual, err := s.actualByName(ctx)
	if err != nil {
		return "", "", err
	}
	inst, ok := actual[name]
	if !ok {
		return fleet.HealthUnhealthy, "not running", nil
	}
	result, err := s.checker.Check(ctx, inst.ID)
	if errors.Is(err, fleet.ErrNotFound) {
		return fleet.HealthUnhealthy, "not running", nil
	}
	if err != nil {
		return "", "", err
	}
	return result.Status, result.Detail, nil
}

func (s *Server) withHealth(ctx context.Context, item fleet.Desired, resp instanceResponse) (instanceResponse, error) {
	check, err := s.store.LatestHealth(ctx, item.ID)
	if errors.Is(err, fleet.ErrNotFound) {
		return resp, nil
	}
	if err != nil {
		return instanceResponse{}, err
	}
	resp.Health = toHealth(check)
	return resp, nil
}

func (s *Server) actualByName(ctx context.Context) (map[string]fleet.Actual, error) {
	list, err := s.provider.List(ctx)
	if err != nil {
		return nil, err
	}
	return fleet.ActualByName(list)
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

type listResponse struct {
	Instances []instanceResponse `json:"instances"`
}

type instanceResponse struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Desired   specResponse    `json:"desired"`
	Actual    *actualResponse `json:"actual"`
	InSync    bool            `json:"in_sync"`
	Health    *healthResponse `json:"health"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type healthResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
	CheckedAt time.Time `json:"checked_at"`
}

type rolloutResponse struct {
	ID          string                    `json:"id"`
	Status      string                    `json:"status"`
	PauseReason string                    `json:"pause_reason"`
	Spec        specResponse              `json:"spec"`
	Instances   []rolloutInstanceResponse `json:"instances"`
}

type rolloutInstanceResponse struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type specResponse struct {
	Image  string `json:"image"`
	Size   string `json:"size"`
	Region string `json:"region"`
}

type actualResponse struct {
	ID     string `json:"id"`
	Image  string `json:"image"`
	Size   string `json:"size"`
	Region string `json:"region"`
	Status string `json:"status"`
}

type reconcileResponse struct {
	Jobs []jobResponse `json:"jobs"`
}

type jobResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Op        string    `json:"op"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	RunAfter  time.Time `json:"run_after"`
}

func toInstance(item fleet.Desired, actual *fleet.Actual) instanceResponse {
	resp := instanceResponse{
		ID:   item.ID,
		Name: item.Name,
		Desired: specResponse{
			Image:  item.Spec.Image,
			Size:   item.Spec.Size,
			Region: item.Spec.Region,
		},
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}
	if actual != nil {
		resp.Actual = &actualResponse{
			ID:     actual.ID,
			Image:  actual.Spec.Image,
			Size:   actual.Spec.Size,
			Region: actual.Spec.Region,
			Status: actual.Status,
		}
		resp.InSync = actual.Spec.Equal(item.Spec)
	}
	return resp
}

func toJobs(jobs []work.Job) []jobResponse {
	out := make([]jobResponse, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, toJob(job))
	}
	return out
}

func toHealth(check fleet.HealthCheck) *healthResponse {
	return &healthResponse{
		ID:        check.ID,
		Status:    check.Status,
		Detail:    check.Detail,
		CheckedAt: check.CheckedAt,
	}
}

func toRollout(roll fleet.Rollout) rolloutResponse {
	instances := make([]rolloutInstanceResponse, 0, len(roll.Names))
	for i, name := range roll.Names {
		instances = append(instances, rolloutInstanceResponse{Name: name, State: rolloutState(roll, i)})
	}
	return rolloutResponse{
		ID:          roll.ID,
		Status:      roll.Status,
		PauseReason: roll.PauseReason,
		Spec: specResponse{
			Image:  roll.Spec.Image,
			Size:   roll.Spec.Size,
			Region: roll.Spec.Region,
		},
		Instances: instances,
	}
}

func rolloutState(roll fleet.Rollout, index int) string {
	switch {
	case index < roll.Next:
		return "updated"
	case roll.Status == fleet.RolloutPaused && index == roll.Next:
		return "failed"
	case roll.Status == fleet.RolloutRunning && index == roll.Next && roll.Phase != fleet.PhaseIdle:
		return "updating"
	default:
		return "pending"
	}
}

func toJob(job work.Job) jobResponse {
	return jobResponse{
		ID:        job.ID,
		Name:      job.Name,
		Op:        job.Op,
		Status:    job.Status,
		Attempts:  job.Attempts,
		LastError: job.LastError,
		RunAfter:  job.RunAfter,
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	media := r.Header.Get("Content-Type")
	if media != "application/json" && !strings.HasPrefix(media, "application/json;") {
		writeError(w, http.StatusBadRequest, "content-type must be application/json")
		return false
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

func writeStoreError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, fleet.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, fleet.ErrConflict):
		writeError(w, http.StatusConflict, "name already exists")
	default:
		slog.Error(op, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write json", "err", err)
	}
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.code)
	})
}
