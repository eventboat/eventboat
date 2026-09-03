package engine

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// AdminHandler serves hot-reload and pipeline admin APIs (§6.6).
type AdminHandler struct {
	eng *Engine
}

func NewAdminHandler(eng *Engine) *AdminHandler {
	return &AdminHandler{eng: eng}
}

func (h *AdminHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/admin/reload", h.handleReload)
	mux.HandleFunc("/admin/reload/", h.handleReloadPath)
	mux.HandleFunc("/admin/pipelines", h.handlePipelines)
	mux.HandleFunc("/admin/pipelines/", h.handlePipelinePath)
}

func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	h.Register(mux)
	mux.ServeHTTP(w, r)
}

func (h *AdminHandler) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/reload" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		adminMethodNotAllowed(w)
		return
	}
	taskID, err := h.eng.BeginReloadAll(r.Context())
	if err != nil {
		writeReloadError(w, err)
		return
	}
	writeAccepted(w, taskID)
}

func (h *AdminHandler) handleReloadPath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/admin/reload/")
	if rest == "" {
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(rest, "status/") {
		if r.Method != http.MethodGet {
			adminMethodNotAllowed(w)
			return
		}
		taskID := strings.TrimPrefix(rest, "status/")
		task, ok := h.eng.ReloadTask(taskID)
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeAdminJSON(w, http.StatusOK, task)
		return
	}
	if r.Method != http.MethodPost {
		adminMethodNotAllowed(w)
		return
	}
	taskID, err := h.eng.BeginReload(r.Context(), rest)
	if err != nil {
		writeReloadError(w, err)
		return
	}
	writeAccepted(w, taskID)
}

func (h *AdminHandler) handlePipelines(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/pipelines" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		adminMethodNotAllowed(w)
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"pipelines": h.eng.ListPipelines(),
	})
}

func (h *AdminHandler) handlePipelinePath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/admin/pipelines/")
	if rest == "" || !strings.HasSuffix(rest, "/status") {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		adminMethodNotAllowed(w)
		return
	}
	name := strings.TrimSuffix(rest, "/status")
	info, ok := h.eng.PipelineInfo(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeAdminJSON(w, http.StatusOK, info)
}

func writeReloadError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrReloadInProgress) {
		writeAdminJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func writeAccepted(w http.ResponseWriter, taskID string) {
	writeAdminJSON(w, http.StatusAccepted, map[string]string{
		"task_id": taskID,
		"status":  "accepted",
	})
}

func writeAdminJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func adminMethodNotAllowed(w http.ResponseWriter) {
	writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}
