package publisher

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicSpec is a topic this service requires, and the configuration it requires
// it to have.
type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]string
}

// EnsureTopics creates any of the given topics that does not exist yet.
//
// The service provisions its own topics instead of leaving it to a setup step.
// Starting it, or a pod being rescheduled, needs nothing else to have run first.
// It is safe on every start: a topic that already exists is left as it is.
//
// What it deliberately does not do is let the broker create topics on first
// produce. That path ignores the configuration below and applies server defaults.
// For the checkpoint topic that means retention instead of compaction, and a
// checkpoint that retention can delete is not a checkpoint. Creating them here is
// what keeps the configuration in the code instead of in someone's shell
// history.
func EnsureTopics(ctx context.Context, brokers []string, specs ...TopicSpec) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("connecting to kafka: %w", err)
	}
	defer client.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, spec := range specs {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = spec.Name
		topic.NumPartitions = spec.Partitions
		// -1 asks the broker for its default, which is 1 on a local single node
		// and 3 on a real cluster. Hardcoding either would be wrong somewhere.
		topic.ReplicationFactor = -1
		for name, value := range spec.Configs {
			config := kmsg.NewCreateTopicsRequestTopicConfig()
			config.Name = name
			config.Value = &value
			topic.Configs = append(topic.Configs, config)
		}
		req.Topics = append(req.Topics, topic)
	}

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("creating topics: %w", err)
	}
	for _, topic := range resp.Topics {
		err := kerr.ErrorForCode(topic.ErrorCode)
		// Already existing is the normal case on every start after the first.
		if err == nil || errors.Is(err, kerr.TopicAlreadyExists) {
			continue
		}
		return fmt.Errorf("creating topic %q: %w", topic.Topic, err)
	}
	return nil
}
