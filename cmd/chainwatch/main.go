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
		rpcURL  = env("RPC_URL", "https://ethereum-rpc.publicnode.com")
		brokers = strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
		topic   = env("KAFKA_TOPIC", "tx-events")
		dataset = env("ADDRESSES_FILE", "testdata/addresses.csv")
	)
	poll, err := time.ParseDuration(env("POLL_INTERVAL", "3s"))
	if err != nil {
		return fmt.Errorf("POLL_INTERVAL: %w", err)
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
	// the only thing anyone has to do.
	if err := publisher.EnsureTopics(ctx, brokers, publisher.TopicSpec{Name: topic, Partitions: 6}); err != nil {
		return err
	}
	pub, err := publisher.NewKafka(brokers, topic)
	if err != nil {
		return err
	}
	defer pub.Close()

	client := ethrpc.New(rpcURL, log)
	from, err := startBlock(ctx, client, env("START_BLOCK", "latest"))
	if err != nil {
		return err
	}
	log.Info("starting", "rpc", rpcURL, "topic", topic, "from", from, "poll", poll.String())

	return pipeline.New(client, matcher.New(watched), pub, poll, log).Run(ctx, from)
}

// startBlock resolves START_BLOCK, which is either "latest" or a block number.
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
