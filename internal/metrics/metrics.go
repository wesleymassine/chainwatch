// Package metrics serves a liveness check and the service's counters.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server exposes two endpoints and no more. A service a scheduler can restart
// needs to say it is alive, and one that must keep pace with a chain needs to
// say how far behind it is. Anything past that is a feature nobody asked for.
type Server struct {
	listener net.Listener
	http     *http.Server
}

// Listen binds addr and prepares the endpoints. snapshot is called per request
// and returns whatever should appear at /metrics.
//
// Binding here rather than inside Serve means a port that cannot be opened is a
// startup failure, not something noticed hours later when someone looks.
func Listen(addr string, snapshot func() any, log *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics endpoint: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(snapshot()); err != nil {
			log.Error("writing metrics", "err", err)
		}
	})

	return &Server{
		listener: listener,
		http:     &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
	}, nil
}

// Addr is what was actually bound, which matters when addr asked for port 0.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve runs until ctx is done.
func (s *Server) Serve(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- s.http.Serve(s.listener) }()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// Its own deadline: the caller's context is already cancelled, which is
		// exactly when Shutdown needs a live one.
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(stop)
	}
}
