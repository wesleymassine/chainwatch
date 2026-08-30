package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka stores the checkpoint as a record in a compacted topic, keyed by chain.
//
// Compaction is what makes this work: the topic keeps only the latest value per
// key, so it stays one record per chain however long the service runs. It is also
// why the topic must be created deliberately. A topic the broker makes on first
// produce gets retention instead, and retention is free to delete the one record
// we depend on.
type Kafka struct {
	brokers  []string
	topic    string
	key      []byte
	producer *kgo.Client
}

func NewKafka(brokers []string, topic string, chainID uint64) (*Kafka, error) {
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint producer: %w", err)
	}
	return &Kafka{
		brokers:  brokers,
		topic:    topic,
		key:      strconv.AppendUint(nil, chainID, 10),
		producer: producer,
	}, nil
}

func (k *Kafka) Save(ctx context.Context, cp Checkpoint) error {
	value, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: k.topic, Key: k.key, Value: value}
	if err := k.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("saving checkpoint at block %d: %w", cp.Block, err)
	}
	return nil
}

// Load reads the topic and keeps the last value stored under our key.
//
// The subtle part is deciding that there is nothing to find. Concluding it
// wrongly would send the service back to the chain head and silently skip every
// block in between — the exact data loss this type exists to prevent. So the end
// of the log is established first: an empty topic is proved empty rather than
// inferred from a read that returned nothing yet.
func (k *Kafka) Load(ctx context.Context) (Checkpoint, bool, error) {
	ranges, err := k.logRanges(ctx)
	if err != nil {
		return Checkpoint{}, false, err
	}
	records := int64(0)
	for _, r := range ranges {
		records += r.end - r.start
	}
	if records == 0 {
		return Checkpoint{}, false, nil
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumeTopics(k.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("checkpoint consumer: %w", err)
	}
	defer consumer.Close()

	var (
		found Checkpoint
		ok    bool
	)
	// Seeded from the log start, not from zero: on a compacted topic the early
	// offsets are gone, and waiting for records that were removed would hang.
	consumed := make(map[int32]int64, len(ranges))
	for partition, r := range ranges {
		consumed[partition] = r.start
	}
	for {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return Checkpoint{}, false, fmt.Errorf("reading checkpoints: %w", err)
		}
		var decodeErr error
		fetches.EachRecord(func(r *kgo.Record) {
			consumed[r.Partition] = r.Offset + 1
			if string(r.Key) != string(k.key) {
				return
			}
			var cp Checkpoint
			if err := json.Unmarshal(r.Value, &cp); err != nil {
				decodeErr = fmt.Errorf("checkpoint at offset %d: %w", r.Offset, err)
				return
			}
			// Later records win: compaction keeps the newest per key, but until
			// it has run the log may still hold older ones ahead of it.
			found, ok = cp, true
		})
		if decodeErr != nil {
			return Checkpoint{}, false, decodeErr
		}

		done := true
		for partition, r := range ranges {
			if consumed[partition] < r.end {
				done = false
				break
			}
		}
		if done {
			return found, ok, nil
		}
	}
}

// partitionRange is where a partition's log currently begins and ends. Both
// matter: on a compacted topic the beginning moves forward as old records are
// removed, so an end offset alone does not say whether anything is left to read.
type partitionRange struct{ start, end int64 }

// logRanges asks the broker what the log actually holds right now.
//
// Partitions are discovered, not assumed. We create the checkpoint topic with
// one. But point the service at a topic with more, read only the first, and the
// checkpoint is quietly missed. That is the failure these lines rule out.
func (k *Kafka) logRanges(ctx context.Context) (map[int32]partitionRange, error) {
	meta := kmsg.NewPtrMetadataRequest()
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = &k.topic
	meta.Topics = append(meta.Topics, topic)

	mresp, err := meta.RequestWith(ctx, k.producer)
	if err != nil {
		return nil, fmt.Errorf("describing %q: %w", k.topic, err)
	}
	var ids []int32
	for _, mt := range mresp.Topics {
		if err := kerr.ErrorForCode(mt.ErrorCode); err != nil {
			return nil, fmt.Errorf("describing %q: %w", k.topic, err)
		}
		for _, mp := range mt.Partitions {
			if err := kerr.ErrorForCode(mp.ErrorCode); err != nil {
				return nil, fmt.Errorf("%s partition %d: %w", k.topic, mp.Partition, err)
			}
			ids = append(ids, mp.Partition)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions", k.topic)
	}

	const earliest, latest = -2, -1
	starts, err := k.listOffsets(ctx, ids, earliest)
	if err != nil {
		return nil, err
	}
	ends, err := k.listOffsets(ctx, ids, latest)
	if err != nil {
		return nil, err
	}
	ranges := make(map[int32]partitionRange, len(ids))
	for _, id := range ids {
		ranges[id] = partitionRange{start: starts[id], end: ends[id]}
	}
	return ranges, nil
}

func (k *Kafka) listOffsets(ctx context.Context, ids []int32, timestamp int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = k.topic
	for _, id := range ids {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = id
		p.Timestamp = timestamp
		lt.Partitions = append(lt.Partitions, p)
	}
	req.Topics = append(req.Topics, lt)

	resp, err := req.RequestWith(ctx, k.producer)
	if err != nil {
		return nil, fmt.Errorf("listing offsets for %q: %w", k.topic, err)
	}
	out := make(map[int32]int64, len(ids))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("%s partition %d: %w", k.topic, p.Partition, err)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out, nil
}

func (k *Kafka) Close() error {
	k.producer.Close()
	return nil
}
