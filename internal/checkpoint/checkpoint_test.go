package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wesleymassine/chainwatch/internal/publisher"
)

var _ Store = (*Fake)(nil)
var _ Store = (*Kafka)(nil)

func TestFakeReportsNothingBeforeAnythingIsSaved(t *testing.T) {
	_, found, err := NewFake().Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("a fresh store reported a checkpoint")
	}
}

func TestFakeReturnsTheLatestSave(t *testing.T) {
	f := NewFake()
	for _, b := range []uint64{10, 20, 30} {
		if err := f.Save(context.Background(), Checkpoint{Block: b}); err != nil {
			t.Fatal(err)
		}
	}
	cp, found, err := f.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("Load() = %v, %v, %v", cp, found, err)
	}
	if cp.Block != 30 {
		t.Errorf("Load() = block %d, want 30", cp.Block)
	}
}

func TestFakeFailAfterStopsRecording(t *testing.T) {
	want := errors.New("broker gone")
	f := NewFake()
	f.FailAfter(1, want)
	if err := f.Save(context.Background(), Checkpoint{Block: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.Save(context.Background(), Checkpoint{Block: 2}); !errors.Is(err, want) {
		t.Fatalf("Save() = %v, want %v", err, want)
	}
	if got := f.Saved(); len(got) != 1 {
		t.Errorf("recorded %d checkpoints, want 1", len(got))
	}
}

func kafkaStore(t *testing.T, chainID uint64) (*Kafka, string) {
	t.Helper()
	brokers := os.Getenv("CHAINWATCH_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set CHAINWATCH_KAFKA_BROKERS to run against a broker")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("chainwatch-cp-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec := publisher.TopicSpec{Name: topic, Partitions: 1,
		Configs: map[string]string{"cleanup.policy": "compact"}}
	if err := publisher.EnsureTopics(ctx, seeds, spec); err != nil {
		t.Fatal(err)
	}
	store, err := NewKafka(seeds, topic, chainID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, topic
}

// An empty topic must be reported as "no checkpoint" promptly. Concluding it
// slowly would make every cold start wait; concluding it wrongly would send the
// service to the chain head and skip everything in between.
func TestKafkaLoadOnAnEmptyTopic(t *testing.T) {
	store, _ := kafkaStore(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, found, err := store.Load(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("an empty topic reported a checkpoint")
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v to decide the topic is empty; that is on every cold start", elapsed)
	}
}

func TestKafkaRoundTripsTheLatestCheckpoint(t *testing.T) {
	store, _ := kafkaStore(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, b := range []uint64{25854432, 25854433, 25854434} {
		if err := store.Save(ctx, Checkpoint{Block: b, Hash: fmt.Sprintf("0x%x", b)}); err != nil {
			t.Fatal(err)
		}
	}
	cp, found, err := store.Load(ctx)
	if err != nil || !found {
		t.Fatalf("Load() = %+v, %v, %v", cp, found, err)
	}
	if cp.Block != 25854434 {
		t.Errorf("Load() = block %d, want the newest, 25854434", cp.Block)
	}
	if want := fmt.Sprintf("0x%x", 25854434); cp.Hash != want {
		t.Errorf("Load() hash = %s, want %s — the hash saved alongside the number", cp.Hash, want)
	}
}

// One topic can serve several chains, so a checkpoint must never be read across
// them: mainnet block 25,000,000 means nothing on Arbitrum.
func TestKafkaKeepsChainsApart(t *testing.T) {
	mainnet, topic := kafkaStore(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := mainnet.Save(ctx, Checkpoint{Block: 25854432, Hash: "0xaa"}); err != nil {
		t.Fatal(err)
	}

	arbitrum, err := NewKafka(strings.Split(os.Getenv("CHAINWATCH_KAFKA_BROKERS"), ","), topic, 42161)
	if err != nil {
		t.Fatal(err)
	}
	defer arbitrum.Close()

	if _, found, err := arbitrum.Load(ctx); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("Arbitrum read mainnet's checkpoint")
	}

	if err := arbitrum.Save(ctx, Checkpoint{Block: 499572049, Hash: "0xbb"}); err != nil {
		t.Fatal(err)
	}
	cp, found, err := mainnet.Load(ctx)
	if err != nil || !found {
		t.Fatalf("Load() = %+v, %v, %v", cp, found, err)
	}
	if cp.Block != 25854432 {
		t.Errorf("mainnet checkpoint = %d, want its own 25854432", cp.Block)
	}
}
