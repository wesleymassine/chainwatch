package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/wesleymassine/chainwatch/internal/matcher"
)

// Kafka publishes events to one topic, keyed by userId.
type Kafka struct {
	client *kgo.Client
	topic  string
}

// NewKafka connects a producer configured for durability over speed.
//
// Idempotent writes are on (franz-go's default) and acks are required from all
// in-sync replicas. Together they mean a retry inside the client cannot turn
// into a duplicate on the broker, and an acknowledged write survives the loss of
// a broker. Neither is free, and both are the point: this service exists to not
// lose transactions.
func NewKafka(brokers []string, topic string) (*Kafka, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Kafka{client: client, topic: topic}, nil
}

// Publish sends every event and waits for the broker to acknowledge all of them.
func (k *Kafka) Publish(ctx context.Context, events []matcher.Event) error {
	if len(events) == 0 {
		return nil
	}
	records := make([]*kgo.Record, len(events))
	for i, e := range events {
		value, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("encoding event %s: %w", e.Hash, err)
		}
		// Keying by userId keeps one user's events on one partition, so a
		// consumer sees them in the order they were produced.
		records[i] = &kgo.Record{
			Topic: k.topic,
			Key:   strconv.AppendUint(nil, e.UserID, 10),
			Value: value,
		}
	}
	// ProduceSync blocks until every record is acknowledged or has failed. The
	// synchronous wait is deliberate: it is what makes "published" mean
	// "durable" to the caller, and what the checkpoint depends on.
	//
	// The broker is never asked to create the topic. EnsureTopics already made it
	// with the configuration this service needs. A topic created on first produce
	// gets server defaults instead, which on the checkpoint topic means retention
	// rather than compaction, and retention is free to delete it. So a missing
	// topic here means something removed it, and that is worth saying.
	if err := k.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("topic %q vanished after startup: %w", k.topic, err)
		}
		return err
	}
	return nil
}

func (k *Kafka) Close() error {
	k.client.Close()
	return nil
}
