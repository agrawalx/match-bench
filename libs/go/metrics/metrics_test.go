package metrics

import (
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRejectsNonFiniteSamples(t *testing.T) {
	// Problem: registry edge cases regress silently until Prometheus scrapes
	// break. Fix: pin the production-safety behavior with focused tests.
	resetGlobalForTest()

	Gauge("bad_gauge", "Bad gauge.", nil, math.NaN())
	Histogram("bad_histogram", "Bad histogram.", nil, math.Inf(1))
	Counter("bad_counter", "Bad counter.", nil, math.Inf(1))

	out := renderForTest(t)
	for _, name := range []string{"iicpc_bad_gauge", "iicpc_bad_histogram", "iicpc_bad_counter"} {
		if strings.Contains(out, name) {
			t.Fatalf("rendered invalid metric %s:\n%s", name, out)
		}
	}
	for _, reason := range []string{"invalid_gauge_value", "invalid_histogram_observation", "invalid_counter_delta"} {
		if !strings.Contains(out, `reason="`+reason+`"`) {
			t.Fatalf("missing registry error reason %q:\n%s", reason, out)
		}
	}
}

func TestMetricDefinitionConflictIsDropped(t *testing.T) {
	resetGlobalForTest()

	Counter("http_requests_total", "Counter.", Labels("service", "svc", "method", "GET", "path", "/x", "status", "200"), 1)
	Histogram("http_requests_total", "Histogram.", Labels("service", "svc", "method", "GET", "path", "/x", "status", "200"), 0.2)

	out := renderForTest(t)
	if strings.Contains(out, "iicpc_http_requests_total_bucket") {
		t.Fatalf("rendered conflicting histogram for existing counter:\n%s", out)
	}
	if !strings.Contains(out, `reason="metric_definition_conflict"`) {
		t.Fatalf("missing definition conflict error:\n%s", out)
	}
}

func TestSanitizesMetricAndHistogramLabels(t *testing.T) {
	resetGlobalForTest()
	global.registerCatalog([]metricSpec{
		histogramSpec("latency.seconds", "Latency.", []string{"label_le", "bad_label"}, defaultBuckets),
	})

	Histogram("latency.seconds", "Latency.", Labels("le", "caller", "bad-label", "value"), 0.1)

	out := renderForTest(t)
	if !strings.Contains(out, "iicpc_latency_seconds_bucket") {
		t.Fatalf("metric name was not sanitized:\n%s", out)
	}
	if strings.Contains(out, `{bad-label=`) || strings.Contains(out, `{le="caller"`) {
		t.Fatalf("labels were not sanitized or reserved le leaked:\n%s", out)
	}
	if !strings.Contains(out, `bad_label="value"`) || !strings.Contains(out, `label_le="caller"`) {
		t.Fatalf("sanitized labels missing:\n%s", out)
	}
}

func TestLabelNameCollisionIsDropped(t *testing.T) {
	resetGlobalForTest()
	global.registerCatalog([]metricSpec{
		counterSpec("colliding_labels_total", "Counter.", []string{"bad_label"}),
	})

	Counter("colliding_labels_total", "Counter.", Labels("bad-label", "one", "bad_label", "two"), 1)

	out := renderForTest(t)
	if strings.Contains(out, "iicpc_colliding_labels_total") {
		t.Fatalf("rendered metric with colliding sanitized labels:\n%s", out)
	}
	if !strings.Contains(out, `reason="label_name_collision"`) {
		t.Fatalf("missing label collision error:\n%s", out)
	}
}

func TestUnregisteredMetricIsDropped(t *testing.T) {
	resetGlobalForTest()

	Counter("surprise_total", "Surprise.", nil, 1)

	out := renderForTest(t)
	if strings.Contains(out, "iicpc_surprise_total") {
		t.Fatalf("rendered unregistered metric:\n%s", out)
	}
	if !strings.Contains(out, `reason="unregistered_metric"`) {
		t.Fatalf("missing unregistered metric error:\n%s", out)
	}
}

func TestIncludesOfficialRuntimeCollectors(t *testing.T) {
	resetGlobalForTest()

	out := renderForTest(t)
	for _, name := range []string{"go_goroutines", "process_cpu_seconds_total"} {
		if !strings.Contains(out, name) {
			t.Fatalf("missing official collector metric %q:\n%s", name, out)
		}
	}
}

func TestUploadBytesUsesByteBuckets(t *testing.T) {
	resetGlobalForTest()

	Histogram("submission_upload_bytes", "Submission upload size in bytes.", nil, 2*1024*1024)

	out := renderForTest(t)
	if !strings.Contains(out, `iicpc_submission_upload_bytes_bucket{le="1.048576e+06"} 0`) {
		t.Fatalf("missing byte histogram bucket below sample:\n%s", out)
	}
	if !strings.Contains(out, `iicpc_submission_upload_bytes_bucket{le="5.24288e+06"} 1`) {
		t.Fatalf("missing byte histogram bucket above sample:\n%s", out)
	}
}

func TestStartServerReportsBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("socket bind unavailable in this environment: %v", err)
	}
	defer ln.Close()

	if srv, err := StartServer(ln.Addr().String()); err == nil {
		_ = srv.Close()
		t.Fatal("StartServer succeeded on an occupied address")
	}
}

func TestHTTPMiddlewareRecordsFirstStatus(t *testing.T) {
	resetGlobalForTest()

	handler := HTTPMiddleware("svc", func(*http.Request) string { return "/items/{id}" })(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			w.WriteHeader(http.StatusInternalServerError)
		}),
	)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/items/123", nil))

	if rr.Code != http.StatusCreated {
		t.Fatalf("response status = %d, want %d", rr.Code, http.StatusCreated)
	}
	out := renderForTest(t)
	if !strings.Contains(out, `status="201"`) {
		t.Fatalf("metrics did not record first status:\n%s", out)
	}
	if strings.Contains(out, `status="500"`) {
		t.Fatalf("metrics recorded second WriteHeader status:\n%s", out)
	}
}

func TestHTTPMiddlewareRecordsImplicitOKAfterWrite(t *testing.T) {
	resetGlobalForTest()

	handler := HTTPMiddleware("svc", nil)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
			w.WriteHeader(http.StatusInternalServerError)
		}),
	)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("response status = %d, want %d", rr.Code, http.StatusOK)
	}
	out := renderForTest(t)
	if !strings.Contains(out, `status="200"`) {
		t.Fatalf("metrics did not record implicit OK:\n%s", out)
	}
	if strings.Contains(out, `status="500"`) {
		t.Fatalf("metrics recorded late WriteHeader status:\n%s", out)
	}
}

func resetGlobalForTest() {
	global = newRegistry()
}

func renderForTest(t *testing.T) string {
	t.Helper()
	rr := httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want %d", rr.Code, http.StatusOK)
	}
	return rr.Body.String()
}
