package metrics

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

type stats struct {
	Blocks uint64 `json:"blocksProcessed"`
	Lag    uint64 `json:"blocksBehind"`
}

func serve(t *testing.T, snapshot func() any) string {
	t.Helper()
	// Port 0 so tests never collide with each other or with a running service.
	s, err := Listen("127.0.0.1:0", snapshot, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return "http://" + s.Addr()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestHealthz(t *testing.T) {
	base := serve(t, func() any { return stats{} })
	code, body := get(t, base+"/healthz")
	if code != http.StatusOK {
		t.Errorf("status = %d, want 200", code)
	}
	if strings.TrimSpace(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
}

// The snapshot is taken per request, so what /metrics reports is current rather
// than whatever was true when the server started.
func TestMetricsReflectsTheCurrentSnapshot(t *testing.T) {
	current := stats{Blocks: 1, Lag: 10}
	base := serve(t, func() any { return current })

	code, body := get(t, base+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	var got stats
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	if got.Blocks != 1 || got.Lag != 10 {
		t.Fatalf("got %+v, want the first snapshot", got)
	}

	current = stats{Blocks: 500, Lag: 0}
	_, body = get(t, base+"/metrics")
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Blocks != 500 || got.Lag != 0 {
		t.Errorf("got %+v, want the second snapshot", got)
	}
}

// A port that cannot be opened has to stop the service at startup, not leave it
// running and unobservable.
func TestListenReportsABadAddress(t *testing.T) {
	if _, err := Listen("127.0.0.1:99999", func() any { return nil },
		slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("want an error for an impossible port")
	}
}

func TestServeStopsWithTheContext(t *testing.T) {
	s, err := Listen("127.0.0.1:0", func() any { return nil },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve() = %v, want a clean stop", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the context was cancelled")
	}
}
