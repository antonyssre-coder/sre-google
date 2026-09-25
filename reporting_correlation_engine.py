#!/usr/bin/env python3
"""Correlate GKE metrics, Cloud Monitoring alerts, and Cloud Logging entries.

Metrics use the Managed Service for Prometheus API backed by Monarch. The
engine produces explainable, rule-based RCA hypotheses; it does not assert
causation. Input values and notification destinations live in a separate YAML.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import ssl
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests
import yaml

LOG = logging.getLogger("gke-reporting-correlation")
SCOPES = ["https://www.googleapis.com/auth/monitoring.read",
          "https://www.googleapis.com/auth/logging.read"]
MONITORING_ROOT = "https://monitoring.googleapis.com"
LOGGING_ROOT = "https://logging.googleapis.com/v2"
SEVERITY_RANK = {"DEFAULT": 0, "DEBUG": 1, "INFO": 2, "NOTICE": 3,
                 "WARNING": 4, "ERROR": 5, "CRITICAL": 6, "ALERT": 7, "EMERGENCY": 8}
ENTITY_KEYS = {
    "cluster": "cluster", "cluster_name": "cluster", "cluster_id": "cluster",
    "namespace": "namespace", "namespace_name": "namespace",
    "pod": "pod", "pod_name": "pod", "node": "node", "node_name": "node",
    "location": "location", "zone": "location", "container": "container",
    "container_name": "container",
}
POD_FAILURE_PATTERNS = [
    ("127", "Container command not found (exit 127)"),
    ("126", "Container command is not executable or permission denied (exit 126)"),
    ("137", "Container was killed, commonly due to OOM (exit 137 / SIGKILL)"),
    ("139", "Container process segmentation fault (exit 139 / SIGSEGV)"),
    ("143", "Container received SIGTERM (exit 143); check rollout or shutdown events"),
    ("oomkilled", "Container was OOMKilled; inspect memory limit and usage"),
    ("crashloopbackoff", "Container repeatedly exits and is in CrashLoopBackOff"),
    ("imagepullbackoff", "Kubernetes cannot pull the container image"),
    ("pending", "Pod is Pending; inspect scheduling, capacity, and resource requests"),
    ("evicted", "Pod was Evicted, usually due to node resource pressure"),
]
POD_FAILURE_METRIC_DIAGNOSES = {
    "pod_failure_exit_1": "application error (exit code 1)",
    "pod_failure_exit_126": "command is not executable or permission denied (exit 126)",
    "pod_failure_exit_127": "command not found (exit 127)",
    "pod_failure_exit_137": "OOMKilled or SIGKILL (exit 137)",
    "pod_failure_exit_139": "segmentation fault (exit 139)",
    "pod_failure_exit_143": "SIGTERM or graceful termination (exit 143)",
    "pod_failure_crashloopbackoff": "CrashLoopBackOff",
    "pod_failure_imagepullbackoff": "ImagePullBackOff",
    "pod_failure_pending": "Pending pod / scheduling or resource issue",
    "pod_failure_oomkilled": "OOMKilled / memory limit exceeded",
    "pod_failure_evicted": "Evicted pod / node resource pressure",
}


def classify_pod_failure(message: str) -> dict[str, str] | None:
    """Classify common Kubernetes reasons and process exit codes in event/log text."""
    text = message.lower()
    for marker, diagnosis in POD_FAILURE_PATTERNS:
        if marker == "127" or marker == "126":
            matched = any(token in text for token in (f"exit code {marker}", f"exitcode={marker}",
                                                       f"exit_code={marker}", f"exit status {marker}"))
        elif marker in {"137", "139", "143"}:
            signal = {"137": "sigkill", "139": "sigsegv", "143": "sigterm"}[marker]
            matched = any(token in text for token in (f"exit code {marker}", f"exitcode={marker}",
                                                       f"exit_code={marker}", f"exit status {marker}", signal))
        else:
            matched = marker in text
        if matched:
            return {"reason": marker, "diagnosis": diagnosis}
    # Generic application exit code 1 is common; require an explicit exit-code phrase.
    if any(token in text for token in ("exit code 1", "exitcode=1", "exit_code=1", "exit status 1")):
        return {"reason": "1", "diagnosis": "Application process exited with code 1; inspect its preceding logs"}
    return None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def load_input(path: Path, allow_placeholders: bool = False) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream) or {}
    for key in ("gcp", "metrics", "logs", "alerts", "notifications", "output"):
        cfg.setdefault(key, {})
    gcp = cfg["gcp"]
    project = str(gcp.get("project_id", "")).strip()
    if not project or (not allow_placeholders and "YOUR_" in project):
        raise ValueError("Set gcp.project_id in the input YAML before running")
    cluster = str(gcp.get("gke_cluster", "")).strip()
    if not cluster or (not allow_placeholders and "YOUR_" in cluster):
        raise ValueError("Set gcp.gke_cluster in the input YAML before running")
    gcp["project_id"], gcp["gke_cluster"] = project, cluster
    gcp.setdefault("location", "")
    gcp.setdefault("lookback_minutes", 60)
    gcp.setdefault("correlation_window_minutes", 10)
    gcp.setdefault("request_timeout_seconds", 30)
    gcp.setdefault("max_log_entries", 500)
    gcp.setdefault("max_alerts", 500)
    gcp.setdefault("max_points_per_query", 1500)
    return cfg


def validate_config(cfg: dict[str, Any]) -> None:
    """Catch input mistakes without contacting Google Cloud."""
    errors: list[str] = []
    gcp = cfg["gcp"]
    for key in ("lookback_minutes", "correlation_window_minutes", "request_timeout_seconds",
                "max_log_entries", "max_alerts", "max_points_per_query"):
        try:
            if float(gcp[key]) <= 0:
                errors.append(f"gcp.{key} must be greater than zero")
        except (KeyError, TypeError, ValueError):
            errors.append(f"gcp.{key} must be a positive number")
    try:
        if int(cfg["metrics"].get("step_seconds", 60)) <= 0:
            errors.append("metrics.step_seconds must be greater than zero")
    except (TypeError, ValueError):
        errors.append("metrics.step_seconds must be a positive integer")
    queries = cfg["metrics"].get("queries", {})
    if not isinstance(queries, dict):
        errors.append("metrics.queries must be a mapping of query names to settings")
    else:
        allowed = {">", ">=", "<", "<=", "=="}
        for name, query_cfg in queries.items():
            if isinstance(query_cfg, str):
                promql, threshold = query_cfg, {}
            elif isinstance(query_cfg, dict):
                if not query_cfg.get("enabled", True):
                    continue
                promql, threshold = query_cfg.get("promql", ""), query_cfg.get("threshold", {}) or {}
            else:
                errors.append(f"metrics.queries.{name} must be a string or mapping")
                continue
            if not isinstance(promql, str) or not promql.strip():
                errors.append(f"metrics.queries.{name}.promql is empty; disable the query or add PromQL")
            if threshold:
                if threshold.get("operator", ">=") not in allowed:
                    errors.append(f"metrics.queries.{name}.threshold.operator is unsupported")
                try:
                    float(threshold["value"])
                except (KeyError, TypeError, ValueError):
                    errors.append(f"metrics.queries.{name}.threshold.value must be numeric")
    notifications = cfg["notifications"]
    if notifications.get("enabled"):
        email_enabled = (notifications.get("email", {}) or {}).get("enabled", False)
        webhook_enabled = (notifications.get("webhook", {}) or {}).get("enabled", False)
        if not email_enabled and not webhook_enabled:
            errors.append("notifications.enabled is true, but no email or webhook channel is enabled")
    if errors:
        raise ValueError("Input validation failed:\n- " + "\n- ".join(errors))


def normalize_entities(*label_maps: Any, cluster_default: str = "") -> dict[str, str]:
    entities: dict[str, str] = {}
    for labels in label_maps:
        if not isinstance(labels, dict):
            continue
        for key, value in labels.items():
            normalized_key = ENTITY_KEYS.get(str(key).lower().replace("-", "_"))
            if normalized_key and value not in (None, ""):
                entities.setdefault(normalized_key, str(value))
    if cluster_default:
        entities.setdefault("cluster", cluster_default)
    return entities


def as_message(entry: dict[str, Any]) -> str:
    if entry.get("textPayload"):
        return str(entry["textPayload"])
    payload = entry.get("jsonPayload") or entry.get("protoPayload") or entry.get("payload")
    if isinstance(payload, str):
        return payload
    if payload is not None:
        return json.dumps(payload, ensure_ascii=False, default=str)
    return str(entry.get("message", ""))


class CloudDataSource:
    """Read-only client for Cloud Monitoring/Monarch and Cloud Logging APIs."""

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.gcp = cfg["gcp"]
        credentials, _ = google.auth.default(scopes=SCOPES)
        self.http = AuthorizedSession(credentials)
        self.timeout = int(self.gcp["request_timeout_seconds"])
        self.project = self.gcp["project_id"]
        self.cluster = self.gcp["gke_cluster"]

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.http.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        response = self.http.post(url, json=body, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def collect_metrics(self, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        section = self.cfg["metrics"]
        if not section.get("enabled", True):
            return [], []
        location = "global"
        base = (f"{MONITORING_ROOT}/v1/projects/{quote(self.project, safe='')}/"
                f"location/{location}/prometheus/api/v1/query_range")
        step = max(15, int(section.get("step_seconds", 60)))
        limit = max(1, int(self.gcp["max_points_per_query"]))
        signals: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for name, query_cfg in section.get("queries", {}).items():
            if isinstance(query_cfg, str):
                promql, threshold = query_cfg, {}
            else:
                if not query_cfg.get("enabled", True):
                    continue
                promql, threshold = query_cfg.get("promql", ""), query_cfg.get("threshold", {}) or {}
            if not promql.strip():
                errors.append({"source": f"metric:{name}", "error": "No PromQL expression configured"})
                continue
            promql = (promql.replace("{cluster}", self.cluster)
                      .replace("{location}", str(self.gcp.get("location", "")))
                      .replace("YOUR_GKE_CLUSTER_NAME", self.cluster)
                      .replace("YOUR_GKE_LOCATION", str(self.gcp.get("location", ""))))
            try:
                payload = self.get_json(base, {"query": promql, "start": start.timestamp(),
                                               "end": end.timestamp(), "step": f"{step}s"})
                if payload.get("status") != "success":
                    raise RuntimeError(str(payload.get("error", "Prometheus API returned non-success status")))
                series = payload.get("data", {}).get("result", [])
                count = 0
                for item in series:
                    labels = item.get("metric", {})
                    points = item.get("values") or ([item["value"]] if item.get("value") else [])
                    for point in points:
                        if count >= limit:
                            break
                        try:
                            event_time = parse_time(float(point[0]))
                            observed = float(point[1])
                        except (ValueError, TypeError, IndexError):
                            continue
                        if not event_time or event_time < start or event_time > end:
                            continue
                        entities = normalize_entities(labels)
                        metric_cluster = entities.get("cluster")
                        if metric_cluster and metric_cluster != self.cluster:
                            continue
                        entities.setdefault("cluster", self.cluster)
                        threshold_value = threshold.get("value")
                        operator = str(threshold.get("operator", ">="))
                        breached = threshold_value is not None and self._compare(
                            observed, operator, float(threshold_value))
                        signal = {
                            "kind": "metric", "source": name, "timestamp": iso(event_time),
                            "entities": entities,
                            "observed": observed, "unit": threshold.get("unit", "configured query units"),
                            "promql": promql,
                        }
                        if threshold_value is not None:
                            signal["threshold"] = {"operator": operator, "value": float(threshold_value),
                                                   "breached": breached,
                                                   "severity": str(threshold.get("severity", "WARNING")).upper()}
                        signals.append(signal)
                        count += 1
                    if count >= limit:
                        break
            except Exception as exc:
                errors.append({"source": f"metric:{name}", "error": str(exc)})
                LOG.warning("Metric query %s failed: %s", name, exc)
        return signals, errors

    @staticmethod
    def _compare(left: float, operator: str, right: float) -> bool:
        if operator == ">":
            return left > right
        if operator == ">=":
            return left >= right
        if operator == "<":
            return left < right
        if operator == "<=":
            return left <= right
        if operator == "==":
            return left == right
        raise ValueError(f"Unsupported metric threshold operator: {operator}")

    def collect_alerts(self, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        if not self.cfg["alerts"].get("enabled", True):
            return [], []
        url = f"{MONITORING_ROOT}/v3/projects/{quote(self.project, safe='')}/alerts"
        params: dict[str, Any] = {"pageSize": 1000}
        limit = max(1, int(self.gcp["max_alerts"]))
        signals: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        try:
            while len(signals) < limit:
                body = self.get_json(url, params)
                for alert in body.get("alerts", []):
                    opened = parse_time(alert.get("openTime"))
                    closed = parse_time(alert.get("closeTime"))
                    if (not opened and not closed) or (opened and opened > end) or (closed and closed < start):
                        continue
                    resource = alert.get("resource", {}) or {}
                    metric = alert.get("metric", {}) or {}
                    metadata = alert.get("metadata", {}) or {}
                    policy = alert.get("policySnapshot", {}) or {}
                    entities = normalize_entities(resource.get("labels"), metric.get("labels"),
                                                  metadata.get("systemLabels"))
                    alert_cluster = entities.get("cluster")
                    if alert_cluster and alert_cluster != self.cluster:
                        continue
                    if not alert_cluster:
                        if not self.cfg["alerts"].get("include_unscoped", False):
                            continue
                        entities["cluster"] = self.cluster
                    stamp = max(opened, start) if opened else closed
                    signals.append({
                        "kind": "alert", "source": policy.get("displayName", alert.get("name", "Cloud Monitoring alert")),
                        "timestamp": iso(stamp), "open_time": alert.get("openTime"),
                        "close_time": alert.get("closeTime"), "state": alert.get("state", "UNKNOWN"),
                        "severity": str(policy.get("severity", "WARNING")).upper(),
                        "entities": entities,
                        "alert_name": alert.get("name"), "metric_type": metric.get("type"),
                    })
                    if len(signals) >= limit:
                        break
                token = body.get("nextPageToken")
                if not token or len(signals) >= limit:
                    break
                params["pageToken"] = token
        except Exception as exc:
            errors.append({"source": "monitoring-alerts", "error": str(exc)})
            LOG.warning("Alert retrieval failed: %s", exc)
        return signals, errors

    def collect_logs(self, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        if not self.cfg["logs"].get("enabled", True):
            return [], []
        base_filter = str(self.cfg["logs"].get("filter", "")).strip()
        base_filter = base_filter.replace("{cluster}", self.cluster).replace("YOUR_GKE_CLUSTER_NAME", self.cluster)
        escaped_cluster = json.dumps(self.cluster)
        time_filter = f'timestamp >= "{iso(start)}" AND timestamp <= "{iso(end)}"'
        cluster_filter = f'resource.labels.cluster_name={escaped_cluster}'
        clauses = [x for x in (base_filter, cluster_filter, time_filter) if x]
        filter_text = " AND ".join(f"({part})" for part in clauses)
        url = f"{LOGGING_ROOT}/entries:list"
        limit = max(1, int(self.gcp["max_log_entries"]))
        body: dict[str, Any] = {"resourceNames": [f"projects/{self.project}"],
                                "filter": filter_text, "orderBy": "timestamp desc",
                                "pageSize": min(limit, 1000)}
        signals: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        try:
            while len(signals) < limit:
                payload = self.post_json(url, body)
                for entry in payload.get("entries", []):
                    stamp = parse_time(entry.get("timestamp"))
                    if not stamp or stamp < start or stamp > end:
                        continue
                    resource = entry.get("resource", {}) or {}
                    labels = entry.get("labels", {}) or {}
                    severity = str(entry.get("severity", "DEFAULT")).upper()
                    message = as_message(entry).strip()
                    signal = {
                        "kind": "log", "source": "Cloud Logging", "timestamp": iso(stamp),
                        "severity": severity, "message": message[:1200],
                        "entities": normalize_entities(resource.get("labels"), labels,
                                                        cluster_default=self.cluster),
                        "log_name": entry.get("logName"),
                    }
                    pod_failure = classify_pod_failure(message)
                    if pod_failure:
                        signal["pod_failure"] = pod_failure
                    signals.append(signal)
                    if len(signals) >= limit:
                        break
                token = payload.get("nextPageToken")
                if not token or len(signals) >= limit:
                    break
                body["pageToken"] = token
        except Exception as exc:
            errors.append({"source": "cloud-logging", "error": str(exc)})
            LOG.warning("Log retrieval failed: %s", exc)
        return signals, errors


def correlate(signals: list[dict[str, Any]], window_seconds: int) -> list[dict[str, Any]]:
    """Build fixed-anchor time windows, grouping signals by GKE identity."""
    ordered = sorted(signals, key=lambda item: item["timestamp"])
    groups: list[list[dict[str, Any]]] = []
    for signal in ordered:
        stamp = parse_time(signal["timestamp"])
        destination = None
        for group in reversed(groups):
            anchor = parse_time(group[0]["timestamp"])
            if (stamp - anchor).total_seconds() > window_seconds:  # type: ignore[operator]
                break
            current_cluster = signal.get("entities", {}).get("cluster")
            group_clusters = {item.get("entities", {}).get("cluster") for item in group}
            group_clusters.discard(None)
            if current_cluster and group_clusters and current_cluster not in group_clusters:
                continue
            # The configured cluster and bounded window define the broad join;
            # shared resource labels are retained to explain the relationship.
            destination = group
            break
        if destination is None:
            groups.append([signal])
        else:
            destination.append(signal)
    incidents = [build_incident(group, window_seconds) for group in groups]
    incidents.sort(key=lambda item: item["start_time"], reverse=True)
    for index, incident in enumerate(incidents, 1):
        incident["incident_id"] = f"INC-{index:04d}"
    return incidents


def build_incident(group: list[dict[str, Any]], window_seconds: int) -> dict[str, Any]:
    group.sort(key=lambda item: item["timestamp"])
    types = Counter(item["kind"] for item in group)
    entities: dict[str, set[str]] = {}
    for item in group:
        for key, value in item.get("entities", {}).items():
            entities.setdefault(key, set()).add(str(value))
    shared_entities = {key: sorted(values) for key, values in entities.items() if len(values) == 1}
    hypotheses: list[dict[str, Any]] = []
    def hypothesis(name: str, detail: str, score: int, kind: str) -> None:
        item = next((x for x in hypotheses if x["hypothesis"] == name), None)
        if item is None:
            item = {"hypothesis": name, "score": 0, "evidence_count": 0, "evidence": [], "category": kind}
            hypotheses.append(item)
        item["score"] += score
        item["evidence_count"] += 1
        item["evidence"].append(detail[:240])

    for signal in group:
        if signal["kind"] == "alert":
            name = signal.get("source", "Cloud Monitoring alert")
            hypothesis(name, f"{signal.get('state', 'UNKNOWN')} alert opened at {signal['timestamp']}", 3, "alert")
        elif signal["kind"] == "metric" and signal.get("threshold", {}).get("breached"):
            threshold = signal["threshold"]
            source = str(signal.get("source", ""))
            if source.startswith("pod_failure_"):
                diagnosis = POD_FAILURE_METRIC_DIAGNOSES.get(
                    source, source.removeprefix("pod_failure_").replace("_", " "))
                hypothesis(f"Kubernetes pod failure: {diagnosis}",
                           f"Metric {source} observed {signal['observed']:.4g}; "
                           f"threshold {signal['threshold']['operator']} {signal['threshold']['value']:.4g}",
                           5, "pod_failure")
            else:
                name = f"Elevated {source}"
                hypothesis(name, (f"Observed {signal['observed']:.4g} {signal.get('unit', '')}; "
                                  f"threshold {threshold['operator']} {threshold['value']:.4g}"), 4, "metric")
        elif signal["kind"] == "log":
            severity = str(signal.get("severity", "DEFAULT")).upper()
            message = str(signal.get("message", ""))
            lower = message.lower()
            pod_failure = signal.get("pod_failure")
            if pod_failure:
                hypothesis(f"Kubernetes pod failure: {pod_failure['diagnosis']}",
                           f"{signal.get('timestamp')}: {message.replace(chr(10), ' ')[:180]}", 5, "pod_failure")
            if SEVERITY_RANK.get(severity, 0) >= SEVERITY_RANK["ERROR"]:
                if any(word in lower for word in ("out of memory", "oomkilled", "oom kill")):
                    title = "Possible memory exhaustion (OOM)"
                elif any(word in lower for word in ("timeout", "timed out", "deadline exceeded")):
                    title = "Possible dependency or request timeout"
                elif any(word in lower for word in ("connection refused", "connection reset", "unreachable")):
                    title = "Possible network or dependency connectivity failure"
                elif any(word in lower for word in ("panic", "traceback", "uncaught exception", "fatal")):
                    title = "Application exception or process failure"
                else:
                    title = f"Error-level log activity ({severity})"
                excerpt = message.replace("\n", " ")[:180] or "No message payload"
                hypothesis(title, f"{signal.get('timestamp')}: {excerpt}", 3, "log")

    hypotheses.sort(key=lambda item: (-item["score"], item["hypothesis"]))
    for item in hypotheses:
        item["confidence"] = "high" if item["evidence_count"] >= 3 else (
            "medium" if item["evidence_count"] == 2 else "low")
    if not hypotheses:
        hypotheses.append({"hypothesis": "Insufficient evidence for a specific cause",
                           "score": 0, "evidence_count": 0, "evidence": [], "category": "unknown",
                           "confidence": "low"})
    severity = max((str(item.get("severity") or item.get("threshold", {}).get("severity", "INFO")).upper() for item in group),
                   key=lambda value: SEVERITY_RANK.get(value, 0))
    actionable = any(
        item["kind"] == "alert"
        or (item["kind"] == "metric" and item.get("threshold", {}).get("breached"))
        or (item["kind"] == "log" and bool(item.get("pod_failure")))
        or (item["kind"] == "log" and SEVERITY_RANK.get(str(item.get("severity", "DEFAULT")).upper(), 0)
            >= SEVERITY_RANK["ERROR"])
        for item in group
    )
    return {
        "start_time": group[0]["timestamp"], "end_time": group[-1]["timestamp"],
        "correlation_window_seconds": window_seconds, "severity": severity,
        "actionable": actionable, "signal_counts": dict(types), "shared_entities": shared_entities,
        "root_cause_hypotheses": hypotheses[:8], "signals": group,
    }


def report_markdown(report: dict[str, Any]) -> str:
    actionable_count = sum(1 for item in report["incidents"] if item.get("actionable"))
    lines = ["# GKE Reporting and Correlation", "",
             f"- Project: `{report['project_id']}`",
             f"- Cluster: `{report['cluster']}`",
             f"- Window: {report['window']['start']} to {report['window']['end']}",
             f"- Correlated signal groups: {len(report['incidents'])}",
             f"- Actionable groups: {actionable_count}",
             f"- Signals: {json.dumps(report['signal_counts'], sort_keys=True)}",
             f"- Metric samples by query: {json.dumps(report['metric_query_counts'], sort_keys=True)}", "",
             "> RCA items are ranked, rule-based hypotheses from time proximity and shared labels. They are not proof of causation.", ""]
    actionable = [item for item in report["incidents"] if item.get("actionable")]
    for incident in actionable:
        lines.extend([f"## {incident['incident_id']} · {incident['severity']} · {incident['start_time']}", "",
                      f"Signals: {json.dumps(incident['signal_counts'], sort_keys=True)}",
                      f"Shared entities: `{json.dumps(incident['shared_entities'], sort_keys=True)}`", "",
                      "### RCA hypotheses", ""])
        for item in incident["root_cause_hypotheses"]:
            lines.append(f"- **{item['hypothesis']}** ({item['confidence']} confidence; score {item['score']})")
            lines.extend(f"  - {evidence}" for evidence in item.get("evidence", []))
        lines.append("")
    if not actionable:
        lines.extend(["## Triage", "", "No alert, configured metric threshold breach, or error-level log was found in the selected window.", ""])
    if report.get("collection_errors"):
        lines.extend(["## Collection errors", ""])
        lines.extend(f"- `{item['source']}`: {item['error']}" for item in report["collection_errors"])
    return "\n".join(lines).rstrip() + "\n"


def notification_summary(report: dict[str, Any]) -> str:
    actionable = [item for item in report["incidents"] if item.get("actionable")]
    lines = [f"GKE correlation report: {report['cluster']} ({report['project_id']})",
             f"Window: {report['window']['start']} to {report['window']['end']}",
             f"Actionable groups: {len(actionable)}; signals: {json.dumps(report['signal_counts'], sort_keys=True)}"]
    for incident in actionable[:10]:
        cause = incident["root_cause_hypotheses"][0]["hypothesis"]
        lines.append(f"{incident['incident_id']} [{incident['severity']}] {cause} — {incident['start_time']}")
    if report.get("collection_errors"):
        lines.append(f"Collection errors: {len(report['collection_errors'])}")
    lines.append("RCA is heuristic and needs operator validation.")
    return "\n".join(lines)


def notify(cfg: dict[str, Any], report: dict[str, Any]) -> list[dict[str, str]]:
    settings = cfg["notifications"]
    if not settings.get("enabled", False):
        return [{"channel": "all", "status": "disabled"}]
    actionable = [item for item in report["incidents"] if item.get("actionable")]
    if not actionable and not settings.get("notify_on_empty", False):
        return [{"channel": "all", "status": "skipped", "detail": "No alerts, configured threshold breaches, or error-level logs"}]
    message = notification_summary(report)
    outcomes: list[dict[str, str]] = []
    email_cfg = settings.get("email", {}) or {}
    if email_cfg.get("enabled", False):
        try:
            recipients = email_cfg.get("to", [])
            host, sender = email_cfg.get("smtp_host"), email_cfg.get("from")
            if not recipients or not host or not sender:
                raise ValueError("Email requires smtp_host, from, and at least one to recipient")
            mail = EmailMessage()
            mail["Subject"] = (f"[{report['cluster']}] GKE alert correlation report: "
                               f"{len(actionable)} actionable group(s)")
            mail["From"], mail["To"] = sender, ", ".join(recipients)
            mail.set_content(message)
            with smtplib.SMTP(host, int(email_cfg.get("smtp_port", 587)),
                              timeout=int(email_cfg.get("timeout_seconds", 20))) as smtp:
                if email_cfg.get("starttls", True):
                    smtp.starttls(context=ssl.create_default_context())
                username = os.getenv(email_cfg.get("username_env", "SMTP_USERNAME"))
                password = os.getenv(email_cfg.get("password_env", "SMTP_PASSWORD"))
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(mail)
            outcomes.append({"channel": "email", "status": "sent", "detail": f"recipient_count={len(recipients)}"})
        except Exception as exc:
            outcomes.append({"channel": "email", "status": "failed", "detail": str(exc)})
    webhook_cfg = settings.get("webhook", {}) or {}
    if webhook_cfg.get("enabled", False):
        try:
            variable = webhook_cfg.get("url_env", "SRE_NOTIFICATION_WEBHOOK_URL")
            webhook_url = os.getenv(variable)
            if not webhook_url:
                raise ValueError(f"Set webhook URL in environment variable {variable}")
            response = requests.post(webhook_url, json={"text": message},
                                     timeout=int(webhook_cfg.get("timeout_seconds", 15)))
            response.raise_for_status()
            outcomes.append({"channel": "webhook", "status": "sent", "detail": f"HTTP {response.status_code}"})
        except Exception as exc:
            outcomes.append({"channel": "webhook", "status": "failed", "detail": str(exc)})
    if not outcomes:
        outcomes.append({"channel": "all", "status": "disabled", "detail": "No notification channel enabled"})
    return outcomes


def run(cfg: dict[str, Any]) -> tuple[dict[str, Any], Path, Path]:
    end = utc_now()
    start = end - timedelta(minutes=int(cfg["gcp"].get("lookback_minutes", 60)))
    source = CloudDataSource(cfg)
    metric_signals, metric_errors = source.collect_metrics(start, end)
    alert_signals, alert_errors = source.collect_alerts(start, end)
    log_signals, log_errors = source.collect_logs(start, end)
    signals = metric_signals + alert_signals + log_signals
    incidents = correlate(signals, int(cfg["gcp"].get("correlation_window_minutes", 10)) * 60)
    report = {
        "generated_at": iso(utc_now()), "project_id": source.project, "cluster": source.cluster,
        "window": {"start": iso(start), "end": iso(end)},
        "correlation_method": "fixed-anchor time windows for the configured cluster; resource labels are retained as evidence",
        "rca_method": "explainable rules for configured metric thresholds, alert metadata, and error-log patterns",
        "signal_counts": dict(Counter(item["kind"] for item in signals)),
        "metric_query_counts": {
            name: sum(1 for item in metric_signals if item["source"] == name)
            for name, query in cfg["metrics"].get("queries", {}).items()
            if cfg["metrics"].get("enabled", True)
            and (not isinstance(query, dict) or query.get("enabled", True))
        },
        "collection_errors": metric_errors + alert_errors + log_errors,
        "incidents": incidents,
        "notifications": [],
        "limitations": [
            "Root-cause items are ranked hypotheses, not causal proof.",
            "Metrics require Managed Service for Prometheus data in the configured project's metrics scope.",
            "Log correlation depends on cluster, namespace, pod, or node labels being present in source records.",
        ],
    }
    output_dir = Path(cfg["output"].get("directory", "./reports")).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = end.strftime("%Y%m%dT%H%M%SZ")
    stem = f"{source.cluster}-correlation-{stamp}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    report["notifications"] = notify(cfg, report)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(report_markdown(report), encoding="utf-8")
    return report, json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="reporting_input.yaml", help="YAML input variables and query settings")
    parser.add_argument("--validate-only", action="store_true",
                        help="Check YAML structure and metric/threshold settings without calling GCP")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        cfg = load_input(Path(args.input).expanduser(), allow_placeholders=args.validate_only)
        validate_config(cfg)
        if args.validate_only:
            LOG.info("Input configuration is structurally valid; GCP connectivity was not checked")
            return 0
        report, json_path, md_path = run(cfg)
        LOG.info("Collected %s signals and correlated %s incident groups",
                 sum(report["signal_counts"].values()), len(report["incidents"]))
        LOG.info("JSON report: %s", json_path.resolve())
        LOG.info("Markdown report: %s", md_path.resolve())
        for delivery in report["notifications"]:
            LOG.info("Notification %s: %s", delivery["channel"], delivery["status"])
        return 0
    except Exception as exc:
        LOG.error("Reporting run failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
