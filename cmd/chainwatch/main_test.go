package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesleymassine/chainwatch/internal/ethrpc"
)

func TestEnvFallsBackWhenUnset(t *testing.T) {
	if got := env("CHAINWATCH_DEFINITELY_UNSET", "fallback"); got != "fallback" {
		t.Errorf("env() = %q, want the fallback", got)
	}
	t.Setenv("CHAINWATCH_TEST_KEY", "set")
	if got := env("CHAINWATCH_TEST_KEY", "fallback"); got != "set" {
		t.Errorf("env() = %q, want the value that was set", got)
	}
	// An empty variable is treated as unset, so an accidental `FOO=` in a .env
	// file does not silently blank a setting.
	t.Setenv("CHAINWATCH_TEST_KEY", "")
	if got := env("CHAINWATCH_TEST_KEY", "fallback"); got != "fallback" {
		t.Errorf("env() = %q, want the fallback for an empty value", got)
	}
}

func TestLogLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"":      slog.LevelInfo,
		"loud":  slog.LevelInfo, // unreadable settings must not stop the service
	}
	for value, want := range tests {
		t.Setenv("LOG_LEVEL", value)
		if got := logLevel(); got != want {
			t.Errorf("LOG_LEVEL=%q gave %v, want %v", value, got, want)
		}
	}
}

// A dataset that loads but holds nothing gives a service that watches nothing
// and still reports itself healthy. It has to refuse instead.
func TestLoadWatchedRefusesAnEmptyDataset(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "empty file", content: "", wantErr: "no addresses"},
		{name: "header only", content: "userId,address\n", wantErr: "no addresses"},
		{name: "one address is enough", content: "1,0x28c6c06298d514db089934071355e5743bf21d60\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "addresses.csv")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			set, err := loadWatched(path)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if set.Len() != 1 {
				t.Errorf("Len() = %d, want 1", set.Len())
			}
		})
	}
}

func TestLoadWatchedReportsAMissingFile(t *testing.T) {
	_, err := loadWatched(filepath.Join(t.TempDir(), "absent.csv"))
	if err == nil || !strings.Contains(err.Error(), "make dataset") {
		t.Fatalf("err = %v, want it to point at the fix", err)
	}
}

func TestStartBlock(t *testing.T) {
	const head = 25854432
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"id":0,"result":"0x%x"}]`, head)
	}))
	defer srv.Close()
	client := ethrpc.New(srv.URL, slog.New(slog.NewTextHandler(nopWriter{}, nil)))

	tests := []struct {
		name    string
		value   string
		want    uint64
		wantErr string
	}{
		{name: "latest asks the node", value: "latest", want: head},
		{name: "a number is taken as given", value: "1000000", want: 1_000_000},
		{name: "anything else is refused", value: "yesterday", wantErr: "START_BLOCK"},
		// Otherwise the service starts, waits for a block years away, and says
		// nothing at all.
		{name: "a block past the head is refused", value: "99999999999", wantErr: "ahead of the chain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := startBlock(context.Background(), client, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to name START_BLOCK", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("startBlock(%q) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
