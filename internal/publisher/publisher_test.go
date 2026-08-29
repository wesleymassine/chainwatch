package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/wesleymassine/chainwatch/internal/matcher"
)

// Fake and Kafka must stay interchangeable, or the pipeline tests prove nothing
// about the real thing.
var (
	_ Publisher = (*Fake)(nil)
	_ Publisher = (*Kafka)(nil)
)

func events(n int) []matcher.Event {
	out := make([]matcher.Event, n)
	for i := range out {
		to := "0x05ff6964d21e5dae3b1010d5ae0465b3c450f381"
		out[i] = matcher.Event{
			UserID:      uint64(i + 1),
			From:        "0x28c6c06298d514db089934071355e5743bf21d60",
			To:          &to,
			Amount:      "10000000000000000000",
			Hash:        fmt.Sprintf("0x%064x", i),
			BlockNumber: 25854432,
		}
	}
	return out
}

func TestFakeRecordsWhatWasPublished(t *testing.T) {
	f := NewFake()
	if err := f.Publish(context.Background(), events(3)); err != nil {
		t.Fatal(err)
	}
	if err := f.Publish(context.Background(), events(2)); err != nil {
		t.Fatal(err)
	}
	if got := len(f.Events()); got != 5 {
		t.Fatalf("recorded %d events, want 5", got)
	}
}

func TestFakePublishingNothingIsNotAnError(t *testing.T) {
	if err := NewFake().Publish(context.Background(), nil); err != nil {
		t.Fatalf("an empty block must not fail: %v", err)
	}
}

func TestFakeFailAfterRecordsNothingOnFailure(t *testing.T) {
	want := errors.New("broker gone")
	f := NewFake()
	f.FailAfter(3, want)

	if err := f.Publish(context.Background(), events(3)); err != nil {
		t.Fatal(err)
	}
	err := f.Publish(context.Background(), events(2))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	// The failed batch must leave no trace, so a test can assert that the
	// pipeline republished it rather than skipped it.
	if got := len(f.Events()); got != 3 {
		t.Fatalf("recorded %d events, want the 3 from before the failure", got)
	}
}

func TestFakeHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewFake().Publish(ctx, events(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestFakeRejectsPublishAfterClose(t *testing.T) {
	f := NewFake()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Publish(context.Background(), events(1)); err == nil {
		t.Fatal("publishing after Close must fail")
	}
}

// TestKafka runs against a real broker. It is skipped unless
// CHAINWATCH_KAFKA_BROKERS is set, so the normal test run stays offline.
func TestKafka(t *testing.T) {
	brokers := os.Getenv("CHAINWATCH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set CHAINWATCH_KAFKA_BROKERS to run against a broker")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("chainwatch-test-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := EnsureTopics(ctx, seeds, TopicSpec{Name: topic, Partitions: 3}); err != nil {
		t.Fatal(err)
	}

	p, err := NewKafka(seeds, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	want := events(5)
	if err := p.Publish(ctx, want); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	got := make(map[uint64]matcher.Event, len(want))
	for len(got) < len(want) {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			// A consumer that starts before the topic metadata has propagated
			// sees UNKNOWN_TOPIC_OR_PARTITION for a moment. The context
			// deadline, not a retry count, is what bounds this.
			if ctx.Err() != nil {
				t.Fatalf("consuming: %v (got %d of %d)", err, len(got), len(want))
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var e matcher.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Errorf("decoding record: %v", err)
				return
			}
			// Partitioning depends on the key being the userId.
			if key := string(r.Key); key != strconv.FormatUint(e.UserID, 10) {
				t.Errorf("record key = %q, want %d", key, e.UserID)
			}
			got[e.UserID] = e
		})
	}

	for _, w := range want {
		g, ok := got[w.UserID]
		if !ok {
			t.Fatalf("user %d never arrived", w.UserID)
		}
		// The value that a JSON number would have corrupted.
		if g.Amount != w.Amount {
			t.Errorf("user %d amount = %s, want %s", w.UserID, g.Amount, w.Amount)
		}
		if g.Hash != w.Hash || g.BlockNumber != w.BlockNumber {
			t.Errorf("user %d round-tripped as %+v, want %+v", w.UserID, g, w)
		}
	}
}

// The service creates its own topics, so a missing one means something removed
// it underneath us. The error has to say that rather than silently recreating
// it with the wrong configuration.
func TestKafkaRefusesToCreateMissingTopic(t *testing.T) {
	brokers := os.Getenv("CHAINWATCH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set CHAINWATCH_KAFKA_BROKERS to run against a broker")
	}
	topic := fmt.Sprintf("chainwatch-absent-%d", time.Now().UnixNano())

	p, err := NewKafka(strings.Split(brokers, ","), topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err = p.Publish(ctx, events(1))
	if err == nil {
		t.Fatal("publishing to a topic that does not exist must fail, not create it")
	}
	if !strings.Contains(err.Error(), topic) {
		t.Errorf("error = %v, want it to name the missing topic", err)
	}
}

func TestEnsureTopicsIsIdempotentAndAppliesConfig(t *testing.T) {
	brokers := os.Getenv("CHAINWATCH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set CHAINWATCH_KAFKA_BROKERS to run against a broker")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("chainwatch-ensure-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := TopicSpec{Name: topic, Partitions: 1, Configs: map[string]string{"cleanup.policy": "compact"}}
	// Called twice on purpose: every restart of the service calls it again.
	for i := 0; i < 2; i++ {
		if err := EnsureTopics(ctx, seeds, spec); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = topic
	req.Resources = append(req.Resources, resource)

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	var policy string
	for _, r := range resp.Resources {
		for _, c := range r.Configs {
			if c.Name == "cleanup.policy" && c.Value != nil {
				policy = *c.Value
			}
		}
	}
	// The whole reason the service provisions topics itself: a compacted
	// checkpoint topic is a correctness requirement, not a preference.
	if policy != "compact" {
		t.Errorf("cleanup.policy = %q, want compact", policy)
	}
}

// Per-user ordering is the guarantee this service actually makes, so it is worth
// proving against a broker rather than asserting in a README.
//
// It rests on three things: the record key is the userId, so one user's events
// land on one partition; the idempotent producer keeps a partition's writes in
// sequence even with several requests in flight; and the pipeline publishes
// blocks strictly in order, waiting for each ack.
func TestKafkaPreservesPerUserOrder(t *testing.T) {
	brokers := os.Getenv("CHAINWATCH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set CHAINWATCH_KAFKA_BROKERS to run against a broker")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("chainwatch-order-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Several partitions on purpose: if the key were wrong, one user's events
	// would scatter and the order below would not survive.
	if err := EnsureTopics(ctx, seeds, TopicSpec{Name: topic, Partitions: 6}); err != nil {
		t.Fatal(err)
	}

	p, err := NewKafka(seeds, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// One user, 200 blocks, published the way the pipeline publishes: one call
	// per block, each awaiting its ack.
	const blocks = 200
	to := "0x05ff6964d21e5dae3b1010d5ae0465b3c450f381"
	for i := 0; i < blocks; i++ {
		e := matcher.Event{
			UserID: 42, From: "0x28c6c06298d514db089934071355e5743bf21d60", To: &to,
			Amount: "1", Hash: fmt.Sprintf("0x%064x", i), BlockNumber: uint64(1000 + i),
		}
		if err := p.Publish(ctx, []matcher.Event{e}); err != nil {
			t.Fatalf("block %d: %v", 1000+i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var got []uint64
	partitions := map[int32]bool{}
	for len(got) < blocks {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				t.Fatalf("consuming: %v (got %d of %d)", err, len(got), blocks)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var e matcher.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Errorf("decoding record: %v", err)
				return
			}
			partitions[r.Partition] = true
			got = append(got, e.BlockNumber)
		})
	}

	if len(partitions) != 1 {
		t.Errorf("one user's events landed on %d partitions, want 1", len(partitions))
	}
	for i, n := range got {
		if want := uint64(1000 + i); n != want {
			t.Fatalf("event %d is for block %d, want %d - ordering broke", i, n, want)
		}
	}
}
