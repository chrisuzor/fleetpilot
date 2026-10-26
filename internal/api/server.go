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
)

// Server serves desired-state writes and reconcile runs.
type Server struct {
	store      fleet.Store
	provider   fleet.Provider
	reconciler *reconcile.Reconciler
}

// New wires the API to a desired-state store and a provider.
func New(store fleet.Store, provider fleet.Provider) *Server {
	return &Server{
		store:      store,
		provider:   provider,
		reconciler: reconcile.New(store, provider),
	}
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
	mux.HandleFunc("POST /v1/reconcile", s.reconcile)
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
		out = append(out, toInstance(item, got))
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
		slog.Error("reconcile", "err", err, "applied", len(result.Actions))
		writeError(w, http.StatusInternalServerError, "reconcile failed")
		return
	}
	writeJSON(w, http.StatusOK, reconcileResponse{Actions: toActions(result.Actions)})
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
	return toInstance(item, got), nil
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
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
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
	Actions []actionResponse `json:"actions"`
}

type actionResponse struct {
	Op         string `json:"op"`
	Name       string `json:"name"`
	ProviderID string `json:"provider_id"`
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

func toActions(actions []fleet.Action) []actionResponse {
	out := make([]actionResponse, 0, len(actions))
	for _, action := range actions {
		out = append(out, actionResponse{
			Op:         action.Op,
			Name:       action.Name,
			ProviderID: action.ProviderID,
		})
	}
	return out
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
