// Package consumer defines tests for kafka integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestKafkaConsumerIntegrationConsumesBuildRequestAndCommits performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestKafkaConsumerIntegrationConsumesBuildRequestAndCommits(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicSubmissionBuildRequested, 3)

	suffix := integrationSuffix()
	msg := topics.SubmissionBuildRequested{
		SubmissionID: "itest-build-consume-" + suffix,
		ArtifactPath: "submissions/itest/artifact.zip",
		Language:     "go",
		Protocol:     "REST",
		Port:         8080,
		BuildType:    "go",
		BuildTarget:  "main",
		TeamName:     "integration",
		SHA256:       "sha-integration",
		RequestedAt:  time.Now().UTC(),
	}

	handler := &recordingHandler{
		wantSubmissionID: msg.SubmissionID,
		seen:             make(chan topics.SubmissionBuildRequested, 1),
	}
	groupID := "itest-build-consumer-" + suffix
	consumer := NewKafkaConsumer(strings.Join(brokers, ","), groupID, handler, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
	})
	go consumer.Start(ctx)
	time.Sleep(500 * time.Millisecond)

	publishJSON(t, brokers, topics.TopicSubmissionBuildRequested, msg.SubmissionID, msg)

	select {
	case got := <-handler.seen:
		if got.SubmissionID != msg.SubmissionID || got.Protocol != msg.Protocol || got.Port != msg.Port {
			t.Fatalf("unexpected consumed build request: %+v", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for build request consumer")
	}
}

// recordingHandler groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type recordingHandler struct {
	mu               sync.Mutex
	wantSubmissionID string
	seen             chan topics.SubmissionBuildRequested
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *recordingHandler) Run(_ context.Context, msg topics.SubmissionBuildRequested) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if msg.SubmissionID != h.wantSubmissionID {
		return
	}
	select {
	case h.seen <- msg:
	default:
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

// publishJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func publishJSON(t *testing.T, brokers []string, topic, key string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test payload: %v", err)
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           5 * time.Second,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatalf("write test message: %v", err)
	}
}
