// Package metrics wraps the official Prometheus Go client with small project
// helpers so services can share metric names, labels, and HTTP wiring.
package metrics

import (
	"errors"
	"math"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "iicpc"

var defaultBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900,
}

var byteBuckets = []float64{
	1024, 10 * 1024, 100 * 1024, 1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024, 25 * 1024 * 1024, 50 * 1024 * 1024, 100 * 1024 * 1024,
}

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

type registry struct {
	mu      sync.RWMutex
	prom    *prometheus.Registry
	metrics map[string]*metric
	errors  *prometheus.CounterVec
}

type metric struct {
	name      string
	kind      metricKind
	buckets   []float64
	labelKeys []string
	counter   *prometheus.CounterVec
	gauge     *prometheus.GaugeVec
	histogram *prometheus.HistogramVec
}

type metricSpec struct {
	name      string
	help      string
	kind      metricKind
	labelKeys []string
	buckets   []float64
}

var global = newRegistry()

func newRegistry() *registry {
	promRegistry := prometheus.NewRegistry()
	errs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: normalizeName("metrics_registry_errors_total"),
			Help: "Metrics samples dropped by the in-process registry.",
		},
		[]string{"reason"},
	)
	promRegistry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	promRegistry.MustRegister(errs)
	r := &registry{
		prom:    promRegistry,
		metrics: make(map[string]*metric),
		errors:  errs,
	}
	r.registerCatalog(projectMetricCatalog())
	return r
}

func Counter(name, help string, labels map[string]string, delta float64) {
	if delta < 0 || !isFinite(delta) {
		global.recordError("invalid_counter_delta")
		return
	}
	if delta == 0 {
		return
	}
	m, values := global.metric(name, help, kindCounter, nil, labels)
	if m == nil {
		return
	}
	m.counter.WithLabelValues(values...).Add(delta)
}

func Gauge(name, help string, labels map[string]string, value float64) {
	if !isFinite(value) {
		global.recordError("invalid_gauge_value")
		return
	}
	m, values := global.metric(name, help, kindGauge, nil, labels)
	if m == nil {
		return
	}
	m.gauge.WithLabelValues(values...).Set(value)
}

func Histogram(name, help string, labels map[string]string, value float64) {
	HistogramWithBuckets(name, help, labels, value, bucketsForMetric(name))
}

func HistogramWithBuckets(name, help string, labels map[string]string, value float64, buckets []float64) {
	if !isFinite(value) {
		global.recordError("invalid_histogram_observation")
		return
	}
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	m, values := global.metric(name, help, kindHistogram, normalizeBuckets(buckets), labels)
	if m == nil {
		return
	}
	m.histogram.WithLabelValues(values...).Observe(value)
}

func Handler() http.Handler {
	return promhttp.HandlerFor(global.prom, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}

func StartServer(addr string) (*http.Server, error) {
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			global.recordError("metrics_server_serve_error")
		}
	}()
	return srv, nil
}

func SinceSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func Labels(values ...string) map[string]string {
	labels := make(map[string]string, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		labels[values[i]] = values[i+1]
	}
	return labels
}

func (r *registry) metric(name, help string, kind metricKind, buckets []float64, labels map[string]string) (*metric, []string) {
	name = normalizeName(name)
	sanitized, ok := sanitizedLabels(labels, kind == kindHistogram)
	if !ok {
		r.recordError("label_name_collision")
		return nil, nil
	}
	labels = sanitized
	labelKeys, labelValues := splitLabels(labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.metrics[name]; ok {
		if existing.kind != kind || !sameBuckets(existing.buckets, buckets) || !sameStrings(existing.labelKeys, labelKeys) {
			r.recordErrorLocked("metric_definition_conflict")
			return nil, nil
		}
		return existing, labelValues
	}
	
	r.recordErrorLocked("unregistered_metric")
	return nil, nil
}

func (r *registry) registerCatalog(specs []metricSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, spec := range specs {
		spec.name = normalizeName(spec.name)
		spec.labelKeys = sanitizeCatalogLabelKeys(spec.labelKeys, spec.kind == kindHistogram)
		if spec.kind == kindHistogram {
			spec.buckets = normalizeBuckets(spec.buckets)
		}
		if _, ok := r.metrics[spec.name]; ok {
			r.recordErrorLocked("catalog_duplicate_metric")
			continue
		}
		m := &metric{name: spec.name, kind: spec.kind, buckets: spec.buckets, labelKeys: spec.labelKeys}
		if r.registerMetricLocked(m, spec.help) {
			r.metrics[spec.name] = m
		}
	}
}

func (r *registry) registerMetricLocked(m *metric, help string) bool {
	switch m.kind {
	case kindCounter:
		m.counter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: m.name, Help: help}, m.labelKeys)
		if !r.register(m.counter) {
			return false
		}
	case kindGauge:
		m.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: m.name, Help: help}, m.labelKeys)
		if !r.register(m.gauge) {
			return false
		}
	case kindHistogram:
		m.histogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: m.name, Help: help, Buckets: m.buckets}, m.labelKeys)
		if !r.register(m.histogram) {
			return false
		}
	}
	return true
}

func projectMetricCatalog() []metricSpec {
	return []metricSpec{
		counterSpec("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil),
		counterSpec("benchmark_publish_failures_total", "Benchmark publish failures by topic.", []string{"topic"}),
		counterSpec("benchmark_requests_total", "Benchmark requests by result.", []string{"result"}),
		counterSpec("build_jobs_created_total", "Build jobs created by mode.", []string{"mode"}),
		counterSpec("build_jobs_failed_total", "Build jobs failed by mode.", []string{"mode"}),
		counterSpec("build_phase_total", "Build phases by phase and result.", []string{"phase", "result"}),
		histogramSpec("build_phase_duration_seconds", "Build phase duration in seconds.", []string{"phase", "result"}, defaultBuckets),
		histogramSpec("build_request_duration_seconds", "End-to-end build request duration in seconds.", []string{"result"}, defaultBuckets),
		counterSpec("build_requests_total", "Build requests by result.", []string{"result"}),
		counterSpec("controller_duplicate_benchmark_requested_total", "Duplicate benchmark.requested messages ignored by controller.", nil),
		gaugeSpec("controller_active_sessions", "Active sessions tracked by the controller.", nil),
		counterSpec("controller_unknown_ready_signal_total", "Ready signals for unknown sessions.", nil),
		histogramSpec("db_query_duration_seconds", "PostgreSQL query duration in seconds.", []string{"operation", "service"}, defaultBuckets),
		counterSpec("db_query_total", "PostgreSQL queries by operation and result.", []string{"operation", "result", "service"}),
		counterSpec("harbor_promote_total", "Harbor image promotions by result.", []string{"result"}),
		histogramSpec("http_request_duration_seconds", "HTTP request duration in seconds.", []string{"method", "path", "service", "status"}, defaultBuckets),
		counterSpec("http_requests_total", "HTTP requests by service, method, path, and status.", []string{"method", "path", "service", "status"}),
		counterSpec("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", []string{"result", "service", "topic"}),
		histogramSpec("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", []string{"result", "service", "topic"}, defaultBuckets),
		counterSpec("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", []string{"result", "service", "topic"}),
		counterSpec("kafka_messages_produced_total", "Kafka messages produced by topic and result.", []string{"result", "service", "topic"}),
		histogramSpec("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", []string{"result", "service", "topic"}, defaultBuckets),
		histogramSpec("minio_operation_duration_seconds", "MinIO operation duration in seconds.", []string{"operation"}, defaultBuckets),
		gaugeSpec("pgxpool_acquired_conns", "Acquired pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_canceled_acquire_total", "pgxpool canceled acquire count.", []string{"service"}),
		gaugeSpec("pgxpool_empty_acquire_total", "pgxpool empty acquire count.", []string{"service"}),
		gaugeSpec("pgxpool_idle_conns", "Idle pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_max_conns", "Maximum pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_total_conns", "Total pgxpool connections.", []string{"service"}),
		counterSpec("ready_none_total", "Sessions with no ready signals before deadline.", nil),
		counterSpec("ready_partial_total", "Sessions with partial ready fan-in.", nil),
		counterSpec("ready_signals_total", "Ready fan-in completions by result.", []string{"result"}),
		counterSpec("recovery_inflight_runs_total", "In-flight runs recovered on controller startup.", nil),
		counterSpec("run_group_children_created_total", "Child runs created by scenario.", []string{"scenario_name"}),
		counterSpec("run_groups_created_total", "Run-groups created by submission-api.", nil),
		counterSpec("run_status_updates_total", "Run status updates applied by submission-api.", []string{"status"}),
		histogramSpec("session_duration_seconds", "Benchmark session duration in seconds.", []string{"result", "scenario_name"}, defaultBuckets),
		histogramSpec("session_stage_duration_seconds", "Benchmark session stage duration in seconds.", []string{"result", "stage"}, defaultBuckets),
		counterSpec("session_stage_total", "Benchmark session stages by result.", []string{"result", "stage"}),
		counterSpec("session_transitions_total", "Session transitions published by status.", []string{"status"}),
		counterSpec("sessions_completed_total", "Benchmark sessions completed by scenario and result.", []string{"result", "scenario_name"}),
		counterSpec("sessions_started_total", "Benchmark sessions started by scenario.", []string{"scenario_name"}),
		histogramSpec("slot_create_duration_seconds", "Sandbox slot create duration in seconds.", []string{"result"}, defaultBuckets),
		counterSpec("slot_refresh_total", "Sandbox slot refreshes by state and result.", []string{"result", "state"}),
		counterSpec("slots_operations_total", "Sandbox slot operations by operation and result.", []string{"operation", "result"}),
		counterSpec("submission_duplicate_total", "Duplicate submissions detected by sha256.", nil),
		counterSpec("submission_status_transition_total", "Submission status transitions by status/result.", []string{"result", "status"}),
		histogramSpec("submission_upload_bytes", "Submission upload size in bytes.", nil, byteBuckets),
		counterSpec("submission_validation_failures_total", "Submission validation failures.", []string{"reason"}),
		counterSpec("submissions_accepted_total", "Accepted submissions by language and protocol.", []string{"language", "protocol"}),
	}
}

func counterSpec(name, help string, labelKeys []string) metricSpec {
	return metricSpec{name: name, help: help, kind: kindCounter, labelKeys: labelKeys}
}

func gaugeSpec(name, help string, labelKeys []string) metricSpec {
	return metricSpec{name: name, help: help, kind: kindGauge, labelKeys: labelKeys}
}

func histogramSpec(name, help string, labelKeys []string, buckets []float64) metricSpec {
	return metricSpec{name: name, help: help, kind: kindHistogram, labelKeys: labelKeys, buckets: buckets}
}

func bucketsForMetric(name string) []float64 {
	name = normalizeName(name)
	for _, spec := range projectMetricCatalog() {
		if normalizeName(spec.name) == name && spec.kind == kindHistogram {
			return spec.buckets
		}
	}
	return defaultBuckets
}

func (r *registry) register(c prometheus.Collector) bool {
	if err := r.prom.Register(c); err != nil {
		r.recordErrorLocked("collector_registration_failed")
		return false
	}
	return true
}

func normalizeName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, namespace+"_") {
		return sanitizeMetricName(name)
	}
	return sanitizeMetricName(namespace + "_" + name)
}

func normalizeBuckets(buckets []float64) []float64 {
	cp := append([]float64(nil), buckets...)
	sort.Float64s(cp)
	out := cp[:0]
	last := math.NaN()
	for _, v := range cp {
		if v <= 0 || v == last {
			continue
		}
		out = append(out, v)
		last = v
	}
	return out
}

func sanitizedLabels(labels map[string]string, histogram bool) (map[string]string, bool) {
	if len(labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		name := sanitizeLabelName(k)
		if histogram && name == "le" {
			name = "label_le"
		}
		if _, exists := out[name]; exists {
			return nil, false
		}
		out[name] = v
	}
	return out, true
}

func sanitizeCatalogLabelKeys(keys []string, histogram bool) []string {
	if len(keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		name := sanitizeLabelName(key)
		if histogram && name == "le" {
			name = "label_le"
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func splitLabels(labels map[string]string) ([]string, []string) {
	if len(labels) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, orderedValues(labels, keys)
}

func orderedValues(labels map[string]string, keys []string) []string {
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		values = append(values, labels[k])
	}
	return values
}

func sanitizeLabelName(name string) string {
	var b strings.Builder
	for i, r := range name {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || (i > 0 && r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

func sanitizeMetricName(name string) string {
	var b strings.Builder
	for i, r := range name {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || (i > 0 && r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return namespace + "_invalid_metric"
	}
	return b.String()
}

func sameBuckets(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func (r *registry) recordError(reason string) {
	r.errors.WithLabelValues(reason).Inc()
}

func (r *registry) recordErrorLocked(reason string) {
	r.errors.WithLabelValues(reason).Inc()
}
