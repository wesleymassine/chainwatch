package config

import (
	"strings"
	"testing"
	"time"
)

// env builds a getenv from "KEY=value" pairs.
func env(pairs ...string) func(string) string {
	values := map[string]string{}
	for _, p := range pairs {
		k, v, _ := strings.Cut(p, "=")
		values[k] = v
	}
	return func(key string) string { return values[key] }
}

func none(string) string { return "" }

func TestProfilesMatchWhatWasMeasured(t *testing.T) {
	tests := []struct {
		chainID      uint64
		name         string
		poll         time.Duration
		batch        int
		confirmDepth uint64
	}{
		{chainID: 1, name: "ethereum", poll: 3 * time.Second, batch: 20, confirmDepth: 2},
		{chainID: 42161, name: "arbitrum", poll: 250 * time.Millisecond, batch: 100, confirmDepth: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(tt.chainID, none)
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != tt.name {
				t.Errorf("Name = %q, want %q", got.Name, tt.name)
			}
			if got.Poll != tt.poll {
				t.Errorf("Poll = %v, want %v", got.Poll, tt.poll)
			}
			if got.BatchSize != tt.batch {
				t.Errorf("BatchSize = %d, want %d", got.BatchSize, tt.batch)
			}
			// The one that comes from how the chain agrees, not how fast it runs.
			if got.ConfirmDepth != tt.confirmDepth {
				t.Errorf("ConfirmDepth = %d, want %d", got.ConfirmDepth, tt.confirmDepth)
			}
		})
	}
}

func TestUnmeasuredChainGetsTheCautiousProfile(t *testing.T) {
	got, err := Load(8453, none) // Base, which we have not measured
	if err != nil {
		t.Fatal(err)
	}
	if Known(8453) {
		t.Fatal("8453 should not be reported as measured")
	}
	if got.Name != "chain-8453" {
		t.Errorf("Name = %q, want it to name the unknown chain", got.Name)
	}
	// Small batches and a confirmation delay: the safe assumptions.
	mainnet, _ := Load(1, none)
	if got.BatchSize != mainnet.BatchSize || got.ConfirmDepth != mainnet.ConfirmDepth {
		t.Errorf("got %+v, want mainnet's caution", got.Options)
	}
}

func TestEnvironmentOverridesTheProfile(t *testing.T) {
	got, err := Load(42161, env("WORKERS=2", "BATCH_SIZE=25", "POLL_INTERVAL=1s", "CONFIRM_DEPTH=5"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Workers != 2 || got.BatchSize != 25 || got.Poll != time.Second || got.ConfirmDepth != 5 {
		t.Errorf("overrides did not apply: %+v", got.Options)
	}
	if got.Name != "arbitrum" {
		t.Errorf("Name = %q, want the profile it started from", got.Name)
	}
}

func TestBadOverridesAreRefused(t *testing.T) {
	tests := []struct {
		name string
		env  func(string) string
		want string
	}{
		{name: "zero workers", env: env("WORKERS=0"), want: "WORKERS"},
		{name: "negative batch", env: env("BATCH_SIZE=-1"), want: "BATCH_SIZE"},
		{name: "batch not a number", env: env("BATCH_SIZE=lots"), want: "BATCH_SIZE"},
		{name: "poll not a duration", env: env("POLL_INTERVAL=soon"), want: "POLL_INTERVAL"},
		{name: "negative confirm depth", env: env("CONFIRM_DEPTH=-2"), want: "CONFIRM_DEPTH"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(1, tt.env)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}
