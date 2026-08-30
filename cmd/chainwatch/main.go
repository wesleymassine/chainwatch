// Command chainwatch monitors Ethereum and compatible L2 chains for transactions
// involving a known set of addresses, and publishes the matches to Kafka.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/checkpoint"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/pipeline"
	"github.com/wesleymassine/chainwatch/internal/publisher"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("chainwatch stopped", "err", err)
		os.Exit(1)
	}
	log.Info("chainwatch stopped")
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		rpcURL      = env("RPC_URL", "https://ethereum-rpc.publicnode.com")
		brokers     = strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
		topic       = env("KAFKA_TOPIC", "tx-events")
		checkpoints = env("KAFKA_CHECKPOINT_TOPIC", "tx-checkpoints")
		dataset     = env("ADDRESSES_FILE", "testdata/addresses.csv")
	)
	opts, err := options()
	if err != nil {
		return err
	}

	file, err := os.Open(dataset)
	if err != nil {
		return fmt.Errorf("opening the address dataset (run `make dataset`): %w", err)
	}
	watched, err := addresses.Load(file)
	file.Close()
	if err != nil {
		return fmt.Errorf("loading %s: %w", dataset, err)
	}
	log.Info("loaded watched addresses", "count", watched.Len(), "file", dataset)

	// Created here rather than by a setup step, so that starting the service is
	// the only thing anyone has to do. The checkpoint topic must be compacted:
	// under retention its one meaningful record would eventually be deleted.
	err = publisher.EnsureTopics(ctx, brokers,
		publisher.TopicSpec{Name: topic, Partitions: 6},
		publisher.TopicSpec{Name: checkpoints, Partitions: 1,
			Configs: map[string]string{"cleanup.policy": "compact"}},
	)
	if err != nil {
		return err
	}
	pub, err := publisher.NewKafka(brokers, topic)
	if err != nil {
		return err
	}
	defer pub.Close()

	client := ethrpc.New(rpcURL, log)
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("identifying the chain: %w", err)
	}
	store, err := checkpoint.NewKafka(brokers, checkpoints, chainID)
	if err != nil {
		return err
	}
	defer store.Close()

	from, err := startBlock(ctx, client, env("START_BLOCK", "latest"))
	if err != nil {
		return err
	}
	log.Info("starting", "rpc", rpcURL, "chainId", chainID, "topic", topic, "fallback", from,
		"poll", opts.Poll.String(), "workers", opts.Workers, "batch", opts.BatchSize,
		"confirmDepth", opts.ConfirmDepth)

	return pipeline.New(client, matcher.New(watched), pub, store, opts, log).Run(ctx, from)
}

// options reads the tuning knobs. The defaults are the mainnet numbers measured
// in notes/03-rpc-probe.md: a batch of 20 stays well under the endpoint's 24 MB
// response cap, and eight workers reach roughly 100 blocks/s during catch-up.
func options() (pipeline.Options, error) {
	poll, err := time.ParseDuration(env("POLL_INTERVAL", "3s"))
	if err != nil {
		return pipeline.Options{}, fmt.Errorf("POLL_INTERVAL: %w", err)
	}
	workers, err := strconv.Atoi(env("WORKERS", "8"))
	if err != nil || workers < 1 {
		return pipeline.Options{}, fmt.Errorf("WORKERS must be a positive number, got %q", env("WORKERS", "8"))
	}
	batch, err := strconv.Atoi(env("BATCH_SIZE", "20"))
	if err != nil || batch < 1 {
		return pipeline.Options{}, fmt.Errorf("BATCH_SIZE must be a positive number, got %q", env("BATCH_SIZE", "20"))
	}
	confirm, err := strconv.ParseUint(env("CONFIRM_DEPTH", "2"), 10, 64)
	if err != nil {
		return pipeline.Options{}, fmt.Errorf("CONFIRM_DEPTH must be a number, got %q", env("CONFIRM_DEPTH", "2"))
	}
	return pipeline.Options{Poll: poll, Workers: workers, BatchSize: batch, ConfirmDepth: confirm}, nil
}

// startBlock resolves START_BLOCK, which is either "latest" or a block number.
// It is only a fallback: a stored checkpoint takes precedence over it.
func startBlock(ctx context.Context, client *ethrpc.Client, value string) (uint64, error) {
	if value == "latest" {
		head, err := client.BlockNumber(ctx)
		if err != nil {
			return 0, fmt.Errorf("reading head: %w", err)
		}
		return head, nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("START_BLOCK must be \"latest\" or a block number: %w", err)
	}
	return n, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func logLevel() slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return slog.LevelInfo
	}
	return level
}
