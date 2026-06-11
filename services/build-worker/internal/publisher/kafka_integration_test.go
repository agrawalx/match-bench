// Package publisher defines tests for kafka integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package publisher

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestKafkaPublisherIntegrationPublishesSubmissionStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestKafkaPublisherIntegrationPublishesSubmissionStatus(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicSubmissionStatusUpdated, 3)

	pub := NewKafkaPublisher(strings.Join(brokers, ","), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	t.Cleanup(func() { _ = pub.Close() })

	submissionID := "itest-status-" + integrationSuffix()
	if err := pub.PublishStatus(context.Background(), submissionID, topics.StatusReady, "integration ready"); err != nil {
		t.Fatalf("publish status: %v", err)
	}

	var status topics.SubmissionStatusUpdated
	consumeJSONByKey(t, brokers, topics.TopicSubmissionStatusUpdated, submissionID, &status)
	if status.SubmissionID != submissionID || status.Status != topics.StatusReady || status.Message != "integration ready" {
		t.Fatalf("unexpected status update: %+v", status)
	}
}

// integrationBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func integrationBrokers(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("KAFKA_BROKERS")
	if strings.TrimSpace(raw) == "" {
		t.Skip("KAFKA_BROKERS is not set; skipping real Kafka integration test")
	}
	var brokers []string
	for _, part := range strings.Split(raw, ",") {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("KAFKA_BROKERS contains no usable brokers")
	}
	return brokers
}

// integrationSuffix performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func integrationSuffix() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
}

// ensureTopic performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ensureTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka admin: %v", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 3,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "min.insync.replicas", ConfigValue: "2"},
			{ConfigName: "max.message.bytes", ConfigValue: "1048576"},
		},
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

// consumeJSONByKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func consumeJSONByKey[T any](t *testing.T, brokers []string, topic, key string, dst *T) {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        "itest-" + topic + "-" + integrationSuffix(),
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("fetch %s key %s: %v", topic, key, err)
		}
		if string(msg.Key) == key {
			if err := json.Unmarshal(msg.Value, dst); err != nil {
				t.Fatalf("decode %s key %s: %v", topic, key, err)
			}
			_ = reader.CommitMessages(context.Background(), msg)
			return
		}
		_ = reader.CommitMessages(context.Background(), msg)
	}
}
