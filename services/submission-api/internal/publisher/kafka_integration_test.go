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

func TestKafkaPublisherIntegrationPublishesBuildAndBenchmarkRequests(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicSubmissionBuildRequested, 3)
	ensureTopic(t, brokers, topics.TopicBenchmarkRequested, 3)

	pub := NewKafkaPublisher(strings.Join(brokers, ","), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	t.Cleanup(func() { _ = pub.Close() })

	suffix := integrationSuffix()
	buildID := "itest-build-" + suffix
	benchID := "itest-benchmark-" + suffix
	now := time.Now().UTC().Truncate(time.Millisecond)

	if err := pub.PublishBuildRequested(context.Background(), PublishMeta{
		SubmissionID: buildID,
		ContestantID: "contestant-" + suffix,
		SHA256:       "sha-" + suffix,
		Language:     "rust",
		Protocol:     "FIX",
		Port:         9898,
		BuildType:    "cargo",
		BuildTarget:  "algo",
		TeamName:     "integration",
		ArtifactPath: "submissions/" + buildID + "/artifact.zip",
		RequestedAt:  now,
	}); err != nil {
		t.Fatalf("publish build request: %v", err)
	}

	if err := pub.PublishBenchmarkRequested(context.Background(), BenchmarkMeta{
		SessionID:    benchID,
		SubmissionID: buildID,
		ContestantID: "contestant-" + suffix,
		RunGroupID:   "group-" + suffix,
		ScenarioID:   "scenario-" + suffix,
		RequestedAt:  now,
	}); err != nil {
		t.Fatalf("publish benchmark request: %v", err)
	}

	var build topics.SubmissionBuildRequested
	consumeJSONByKey(t, brokers, topics.TopicSubmissionBuildRequested, buildID, &build)
	if build.SubmissionID != buildID || build.ArtifactPath == "" || build.Port != 9898 {
		t.Fatalf("unexpected build request: %+v", build)
	}

	var benchmark topics.BenchmarkRequested
	consumeJSONByKey(t, brokers, topics.TopicBenchmarkRequested, benchID, &benchmark)
	if benchmark.SessionID != benchID || benchmark.RunGroupID != "group-"+suffix {
		t.Fatalf("unexpected benchmark request: %+v", benchmark)
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
