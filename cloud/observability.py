"""Low-cardinality production metrics for API and worker processes."""

from prometheus_client import Counter, Gauge, Histogram


HTTP_REQUESTS = Counter(
    "kirakira_http_requests_total",
    "Cloud API requests",
    ("method", "route", "status"),
)
HTTP_DURATION = Histogram(
    "kirakira_http_request_duration_seconds",
    "Cloud API request latency",
    ("method", "route"),
)
RATE_LIMITED = Counter(
    "kirakira_rate_limited_total", "Rejected requests", ("scope",)
)
RUNS = Counter(
    "kirakira_runs_total", "Run terminal transitions", ("status",)
)
RUN_DURATION = Histogram(
    "kirakira_run_duration_seconds", "Worker Run execution latency", ("status",)
)
WORKER_ACTIVE_RUNS = Gauge(
    "kirakira_worker_active_runs", "Runs currently executing in this worker process"
)
