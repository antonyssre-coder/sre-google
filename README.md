# GKE Reporting and Correlation Engine

This read-only Python tool brings together GKE metrics, Cloud Monitoring alert incidents, and Cloud Logging entries for one configured cluster and time window. It correlates signals in bounded time windows, ranks explainable root-cause hypotheses, writes JSON and Markdown reports, and can send a digest by SMTP email and/or an incoming webhook.

## Monarch integration

Monarch is Google's internal time-series service. The script does not call an undocumented direct Monarch endpoint. It queries Google Cloud Managed Service for Prometheus through the documented Cloud Monitoring Prometheus API; Google documents that this interface retrieves Prometheus data from Monarch. Alert incidents come from the Cloud Monitoring alerts API, and log records come from Cloud Logging.

## Files

- `reporting_correlation_engine.py` — collector, correlation, RCA, reporting, and optional notifications.
- `reporting_input.yaml` — separate environment inputs, PromQL queries, thresholds, filters, and notification configuration. Keep credentials out of this file.
- `requirements.txt` — Python dependencies.
- `reports/` — created at runtime; contains timestamped `.json` and `.md` results.

## Setup

1. Edit `reporting_input.yaml`: set the scoping project ID, GKE cluster name, location, log filter, application metric labels, and output directory. CPU and memory thresholds are 80% of each container's configured limit, evaluated as a five-minute mean. The sample p95 latency (1000 ms), 5xx rate (5%), and 1 request/second minimum traffic are starter values; set them from your service SLO. The request-rate query is monitored without a fixed alert threshold because request volume has no universal healthy value. Container restarts alert at 3 in 10 minutes, and any unschedulable pod is surfaced. A custom application metric query is included disabled until you provide its PromQL and threshold.
2. Use an identity with read access to Cloud Monitoring/Managed Service for Prometheus and Cloud Logging. Typical predefined roles are `roles/monitoring.viewer` and `roles/logging.viewer`. Enable the Cloud Monitoring and Cloud Logging APIs in the project, and ensure the scoping project's metrics scope can see the cluster's Prometheus data.
3. Authenticate with Application Default Credentials. For local development, run `gcloud auth application-default login`; in production, prefer an attached service account or Workload Identity.
4. Install and run from this folder:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   python -m pip install -r requirements.txt
   python reporting_correlation_engine.py --input reporting_input.yaml
   ```

Before making Google Cloud API calls, run the local input checks:

```powershell
python reporting_correlation_engine.py --input reporting_input.yaml --validate-only
```

This checks YAML structure and threshold values only; it does not validate PromQL or GCP access. For a live check, run the normal command with `--verbose`, then inspect the JSON report's `signal_counts`, `collection_errors`, `incidents`, and `notifications` fields, together with the Markdown summary in `reports/`.

The tool uses `monitoring.read` and `logging.read` OAuth scopes. It makes read-only API requests and writes reports locally.

## Monitored signals and starter thresholds

| Signal | Query / input | Alert behavior |
|---|---|---|
| CPU utilization | GKE `container/cpu/limit_utilization`; five-minute mean | Notify at **80% of the container CPU limit**. |
| Memory usage | GKE `container/memory/limit_utilization`; five-minute mean | Notify at **80% of the container memory limit**. |
| Request count | OpenTelemetry HTTP server request histogram count, converted to requests/second | Reported; no universal static threshold is enabled. Set one from expected traffic or a learned baseline if you want request-volume alerts. |
| Latency | OpenTelemetry HTTP server request-duration histogram, p95 | Starter alert at **1,000 ms**, only when traffic is at least 1 request/second. Tune both values to the service SLO. |
| Error rate | HTTP 5xx request rate divided by total request rate | Starter alert at **5%**, only when traffic is at least 1 request/second. Tune to the service SLO. |
| Application metrics | `custom_application_metric` PromQL slot | Disabled until you supply an application-specific series and threshold (for example queue depth, DB pool saturation, or a business KPI). |
| Infrastructure health | Container restart count and kube-state unschedulable-pod metric | Notify at **3 restarts in 10 minutes** or **1+ unschedulable pods**. Unschedulable pods require GKE kube-state metrics collection. |
| Pod/container failures | kube-state-metrics last exit code and waiting/phase/reason metrics, plus matching Cloud Logging text | Notify for exit codes **1, 126, 127, 137, 139, 143** and states **CrashLoopBackOff, ImagePullBackOff, Pending, OOMKilled, Evicted**. Each configured PromQL query is actionable at **one matching container/pod**. |
| Existing Cloud Monitoring alerts and error-level logs | Cloud Monitoring incidents and Cloud Logging entries | Correlated and included in notifications. Logs are retrieved at WARNING or higher; ERROR and above count as actionable RCA evidence. |

CPU and memory percentages in this input are per-container limit utilization, matching GKE's documented limit-utilization metrics; they are not node-wide percentages of allocatable capacity. Containers without configured limits might not produce limit-utilization series. If the requirement means node-wide 80% utilization, change the configured PromQL to node-level usage divided by node allocatable capacity and set the denominator explicitly.

Request volume, latency, error rate, and application metrics depend on application instrumentation being collected by Managed Service for Prometheus. The example HTTP queries use OpenTelemetry's HTTP server request-duration histogram. Check the metric names and labels in Metrics Explorer for your exporter before using the sample thresholds.

`--validate-only` checks the YAML and threshold syntax but does not parse PromQL. To validate the actual queries, run each PromQL expression in Cloud Monitoring Metrics Explorer against the target cluster, confirm it returns data, then run the collector and compare `metric_query_counts` with the expected signals.

## Notifications

Notifications are disabled by default. Set `notifications.enabled: true`, then enable `notifications.email` and/or `notifications.webhook` in the YAML. SMTP username and password and the webhook URL are read from the named environment variables, never from the input file. The webhook payload is `{"text": "..."}`; confirm that the receiving endpoint accepts this format. A notification is sent for a Cloud Monitoring alert, configured metric threshold breach, recognized pod failure, or error-level log. Normal metric samples do not trigger notifications. A notification is skipped when there are no actionable signals unless `notify_on_empty` is enabled. There is no cross-run deduplication, so a continuing breach can notify again on each scheduled run.

### Pod failure monitoring

The configured `pod_failure_*` PromQL queries look for exit codes 1 (application error), 126 (not executable/permission issue), 127 (command not found), 137 (SIGKILL, commonly OOM), 139 (SIGSEGV), and 143 (SIGTERM), and the listed Kubernetes waiting, phase, and termination reasons. The exit-code metric is the **last terminated container exit code**; it remains visible until another termination updates it, so an alert can recur on later runs while that value is retained. The tool also classifies these reasons and explicit exit-code phrases found in returned log messages. Log text alone may not contain Kubernetes status or exit-code details.

These pod-state PromQL queries require the corresponding kube-state-metrics series to be scraped and queryable in Managed Service for Prometheus. The sample selectors assume `cluster`, `namespace`, `pod`, and `container` labels. Confirm metric availability and actual label names in Metrics Explorer; adapt the selectors or disable unavailable queries to avoid collection errors. A Pending pod is immediately surfaced, which can include newly created pods during normal scheduling. Exit 143 often accompanies a planned rollout or graceful shutdown and should be interpreted with deployment events. Pod conditions are evaluated at one matching series (>= 1); tune or add a persistence window if transient notifications are too noisy.

For example, configure the webhook in PowerShell without putting its secret into YAML:

```powershell
$env:SRE_NOTIFICATION_WEBHOOK_URL = "<your incoming webhook URL>"
```

## Correlation and RCA

Signals are assigned to fixed time windows using the configured correlation window. The report preserves metric labels, alert metadata, log severity/message, and shared entity labels when available. Alerts with explicit labels for a different cluster are excluded; alerts without cluster labels are excluded unless `alerts.include_unscoped` is enabled. PromQL inputs should scope the query to the selected cluster whenever the metric has a cluster label. The RCA rules rank evidence such as configured metric threshold breaches, Cloud Monitoring alerts, error-level logs, OOM messages, timeouts, connection failures, and application exceptions. Each hypothesis includes its evidence and a simple confidence label based on repeated matching evidence.

This is an explainable triage aid, not causal proof. It does not use an LLM, automatically remediate the cluster, or change monitoring policies. Missing labels, absent telemetry, bad metric names, or an inaccessible metrics scope can limit correlation; failures are included in the report's `collection_errors` field.

## Output

Each run produces:

- `<cluster>-correlation-<UTC timestamp>.json` — machine-readable signal groups, hypotheses, notifications, and collection errors.
- `<cluster>-correlation-<UTC timestamp>.md` — concise incident and RCA summary for review or handoff.

## Google Cloud references

- [Managed Service for Prometheus query API and Monarch-backed queries](https://docs.cloud.google.com/stackdriver/docs/managed-prometheus/query-api-ui)
- [Prometheus `query_range` API](https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v1/projects.location.prometheus.api.v1/query_range)
- [Cloud Monitoring alert incidents API](https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alerts/list)
- [Cloud Logging `entries.list` API](https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list)
- [GKE system metrics and example 80% CPU limit alert](https://docs.cloud.google.com/kubernetes-engine/docs/troubleshooting/introduction-monitoring)
- [GKE cAdvisor/Kubelet metrics](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/cadvisor-kubelet-metrics)
- [GKE kube-state metrics](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/kube-state-metrics)
- [GKE application performance metrics](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/app-performance-metrics)
- [OpenTelemetry HTTP metric conventions](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)
- [Google SRE's four golden signals](https://sre.google/sre-book/monitoring-distributed-systems/)
