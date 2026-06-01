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

func TestBenchmarkStatusConsumerIntegrationConsumesStatusFromKafka(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicBenchmarkStatusUpdated, 3)

	suffix := integrationSuffix()
	sessionID := "itest-submission-consumer-" + suffix
	store := newRecordingRunStatusStore(sessionID)
	consumer := NewBenchmarkStatusConsumer(
		strings.Join(brokers, ","),
		"itest-submission-api-benchmark-status-"+suffix,
		store,
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
	})
	go consumer.Start(ctx)

	event := topics.BenchmarkStatusUpdated{
		SessionID:    sessionID,
		SubmissionID: "sub-" + suffix,
		RunGroupID:   "group-" + suffix,
		Status:       topics.RunStatusRunning,
		Message:      "integration running",
		UpdatedAt:    time.Now().UTC(),
	}
	publishJSON(t, brokers, topics.TopicBenchmarkStatusUpdated, event.SessionID, event)

	select {
	case got := <-store.updates:
		if got.sessionID != event.SessionID || got.status != event.Status || got.message != event.Message {
			t.Fatalf("unexpected store update from Kafka event: %+v", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for benchmark.status.updated consumer to process Kafka message")
	}
}

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

func integrationSuffix() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
}

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

func publishJSON[T any](t *testing.T, brokers []string, topic, key string, value T) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s key %s: %v", topic, key, err)
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatalf("publish %s key %s: %v", topic, key, err)
	}
}

type runStatusUpdate struct {
	sessionID string
	status    string
	message   string
}

type recordingRunStatusStore struct {
	targetSessionID string
	updates         chan runStatusUpdate
	mu              sync.Mutex
	rollups         []string
}

func newRecordingRunStatusStore(targetSessionID string) *recordingRunStatusStore {
	return &recordingRunStatusStore{
		targetSessionID: targetSessionID,
		updates:         make(chan runStatusUpdate, 4),
	}
}

func (s *recordingRunStatusStore) UpdateRunStatus(_ context.Context, sessionID, status, message string) error {
	if sessionID == s.targetSessionID {
		s.updates <- runStatusUpdate{sessionID: sessionID, status: status, message: message}
	}
	return nil
}

func (s *recordingRunStatusStore) RecomputeRunGroupStatus(_ context.Context, runGroupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollups = append(s.rollups, runGroupID)
	return nil
}
