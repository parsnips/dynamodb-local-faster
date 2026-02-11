package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type State struct {
	startedAt time.Time
	ready     atomic.Bool
}

func NewState() *State {
	return &State{
		startedAt: time.Now().UTC(),
	}
}

func (s *State) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *State) HealthHandler(w http.ResponseWriter, r *http.Request) {
	_ = r

	uptimeSeconds := int(time.Since(s.startedAt).Seconds())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","uptime_seconds":%d}`+"\n", uptimeSeconds)
}

func (s *State) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "application/json")

	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not-ready"}` + "\n"))
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
}

func (s *State) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	_ = r

	readyValue := 0
	if s.ready.Load() {
		readyValue = 1
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte("# HELP dynolocalfaster_ready Whether the server is ready.\n"))
	_, _ = w.Write([]byte("# TYPE dynolocalfaster_ready gauge\n"))
	_, _ = fmt.Fprintf(w, "dynolocalfaster_ready %d\n", readyValue)
	_, _ = w.Write([]byte("# HELP dynolocalfaster_uptime_seconds Process uptime.\n"))
	_, _ = w.Write([]byte("# TYPE dynolocalfaster_uptime_seconds gauge\n"))
	_, _ = fmt.Fprintf(w, "dynolocalfaster_uptime_seconds %.3f\n", time.Since(s.startedAt).Seconds())
}
