// Package config picks the settings that differ between chains.
package config

import (
	"fmt"
	"strconv"
	"time"

	"github.com/wesleymassine/chainwatch/internal/pipeline"
)

// Profile is the tuning for one chain.
type Profile struct {
	Name string
	pipeline.Options
}

// Chains differ in ways no single setting can serve. Mainnet produces a block
// every 12-13 seconds and each one is around 620 KB, so batches stay small and
// the poll is slow. Arbitrum produces up to four a second and its blocks are
// roughly a hundred times smaller, so it polls fast and batches large.
//
// ConfirmDepth is the one that comes from how the chains reach agreement rather
// than from their throughput. Mainnet can replace a block that two miners found
// at once, so we stay two behind. Arbitrum has a single sequencer deciding
// order, nothing competes, and waiting would buy nothing.
//
// The numbers are measured, not guessed: notes/03-rpc-probe.md.
var profiles = map[uint64]Profile{
	1: {
		Name: "ethereum",
		Options: pipeline.Options{
			Poll: 3 * time.Second, Workers: 8, BatchSize: 20, ConfirmDepth: 2,
		},
	},
	42161: {
		Name: "arbitrum",
		Options: pipeline.Options{
			Poll: 250 * time.Millisecond, Workers: 8, BatchSize: 100, ConfirmDepth: 0,
		},
	},
}

// Load returns the profile for a chain, with any environment overrides applied.
//
// The chain identifies itself: a CHAIN setting could disagree with the endpoint
// it is pointed at, and resuming mainnet's block numbers against another chain
// would be worse than any tuning mistake.
func Load(chainID uint64, getenv func(string) string) (Profile, error) {
	profile, known := profiles[chainID]
	if !known {
		// Mainnet's settings are the cautious choice for a chain we have not
		// measured: small batches and a confirmation delay.
		profile = profiles[1]
		profile.Name = fmt.Sprintf("chain-%d", chainID)
	}

	var err error
	if profile.Poll, err = duration(getenv, "POLL_INTERVAL", profile.Poll); err != nil {
		return Profile{}, err
	}
	if profile.Workers, err = positive(getenv, "WORKERS", profile.Workers); err != nil {
		return Profile{}, err
	}
	if profile.BatchSize, err = positive(getenv, "BATCH_SIZE", profile.BatchSize); err != nil {
		return Profile{}, err
	}
	if profile.ConfirmDepth, err = count(getenv, "CONFIRM_DEPTH", profile.ConfirmDepth); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

// Known reports whether the chain has a measured profile of its own.
func Known(chainID uint64) bool {
	_, ok := profiles[chainID]
	return ok
}

func duration(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func positive(getenv func(string) string, key string, fallback int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive number, got %q", key, raw)
	}
	return value, nil
}

func count(getenv func(string) string, key string, fallback uint64) (uint64, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number, got %q", key, raw)
	}
	return value, nil
}
