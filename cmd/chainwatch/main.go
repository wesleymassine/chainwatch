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

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/checkpoint"
	"github.com/wesleymassine/chainwatch/internal/config"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/metrics"
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
		metricsAddr = env("METRICS_ADDR", ":9090")
		dataset     = env("ADDRESSES_FILE", "testdata/addresses.csv")
	)

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
	profile, err := config.Load(chainID, os.Getenv)
	if err != nil {
		return err
	}
	if !config.Known(chainID) {
		log.Warn("no measured profile for this chain, using mainnet settings",
			"chainId", chainID, "profile", profile.Name)
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
	p := pipeline.New(client, matcher.New(watched), pub, store, profile.Options, log)

	// Bound before anything starts, so a port that cannot be opened stops the
	// service now rather than leaving it running and unobservable.
	server, err := metrics.Listen(metricsAddr, func() any { return p.Stats() }, log)
	if err != nil {
		return err
	}

	log.Info("starting", "chain", profile.Name, "chainId", chainID, "rpc", rpcURL,
		"topic", topic, "fallback", from, "poll", profile.Poll.String(),
		"workers", profile.Workers, "batch", profile.BatchSize,
		"confirmDepth", profile.ConfirmDepth, "metrics", server.Addr())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(runCtx) }()

	err = p.Run(runCtx, from)
	cancel()
	if serveErr := <-served; err == nil {
		err = serveErr
	}
	return err
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
