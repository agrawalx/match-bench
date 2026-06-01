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

func TestKafkaConsumerIntegrationConsumesBuildRequestAndCommits(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicSubmissionBuildRequested, 3)

	handler := &recordingHandler{seen: make(chan topics.SubmissionBuildRequested, 1)}
	groupID := "itest-build-consumer-" + integrationSuffix()
	consumer := NewKafkaConsumer(strings.Join(brokers, ","), groupID, handler, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
	})
	go consumer.Start(ctx)

	msg := topics.SubmissionBuildRequested{
		SubmissionID: "itest-build-consume-" + integrationSuffix(),
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
	publishJSON(t, brokers, topics.TopicSubmissionBuildRequested, msg.SubmissionID, msg)

	select {
	case got := <-handler.seen:
		if got.SubmissionID != msg.SubmissionID || got.Protocol != "REST" || got.Port != 8080 {
			t.Fatalf("unexpected consumed build request: %+v", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for build request consumer")
	}
}

type recordingHandler struct {
	mu   sync.Mutex
	seen chan topics.SubmissionBuildRequested
}

func (h *recordingHandler) Run(_ context.Context, msg topics.SubmissionBuildRequested) {
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case h.seen <- msg:
	default:
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
