#!/usr/bin/env python3
"""Collect GKE/Kubernetes inventory, events, pod logs, health, metrics and traces.

Reads Kubernetes API and Metrics Server data, optional Prometheus time series,
and an optional backend-specific trace search endpoint. See README.md.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import ssl
import sys
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import requests
import yaml
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

LOG = logging.getLogger("k8s-observability")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream) or {}
    cfg.setdefault("cluster", {})
    cfg.setdefault("collection", {})
    cfg.setdefault("backends", {})
    return cfg


class Collector:
    def __init__(self, cfg: dict[str, Any], output: Path):
        self.cfg = cfg
        self.out = output
        self.cluster = cfg["cluster"]
        self.collection = cfg["collection"]
        self.backends = cfg["backends"]
        self.name = self.cluster.get("name", "kubernetes")
        self.errors: list[dict[str, Any]] = []
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "k8s-observability-collector/1.0"})

    def record_error(self, area: str, exc: Exception) -> None:
        entry = {"time": utc_now(), "area": area, "error": str(exc)}
        self.errors.append(entry)
        LOG.warning("%s collection failed: %s", area, exc)

    def collect_kubernetes(self) -> dict[str, Any]:
        """Collect API health, metadata, inventory, events, logs and metrics."""
        try:
            context = self.cluster.get("context") or None
            kubeconfig = self.cluster.get("kubeconfig") or None
            if kubeconfig:
                kubeconfig = str(Path(kubeconfig).expanduser())
            config.load_kube_config(config_file=kubeconfig, context=context)
        except Exception as exc:
            self.record_error("kubeconfig", exc)
            raise RuntimeError("Could not load kubeconfig; check cluster.kubeconfig/context") from exc

        core = client.CoreV1Api()
        apps = client.AppsV1Api()
        version_api = client.VersionApi()
        result: dict[str, Any] = {"cluster": self.name, "collected_at": utc_now()}
        try:
            result["version"] = version_api.get_code().to_dict()
            result["health"] = {"api_reachable": True, "checked_at": utc_now()}
        except Exception as exc:
            self.record_error("api_health", exc)
            result["health"] = {"api_reachable": False, "checked_at": utc_now()}

        def safe(area: str, fn):
            try:
                return fn()
            except Exception as exc:
                self.record_error(area, exc)
                return {"collection_error": str(exc)}

        result["nodes"] = safe("nodes", lambda: [x.to_dict() for x in core.list_node().items])
        result["namespaces"] = safe("namespaces", lambda: [x.to_dict() for x in core.list_namespace().items])
        result["pods"] = safe("pods", lambda: [x.to_dict() for x in core.list_pod_for_all_namespaces().items])
        result["deployments"] = safe("deployments", lambda: [x.to_dict() for x in apps.list_deployment_for_all_namespaces().items])
        result["services"] = safe("services", lambda: [x.to_dict() for x in core.list_service_for_all_namespaces().items])

        ns = self.collection.get("namespaces", [])
        events = safe("events", lambda: [x.to_dict() for x in core.list_event_for_all_namespaces(
            limit=int(self.collection.get("event_limit", 1000))).items])
        result["events"] = events
        logs_cfg = self.collection.get("pod_logs", {})
        if logs_cfg.get("enabled", True):
            result["pod_logs"] = self._collect_logs(core, ns, logs_cfg)
        if self.collection.get("metrics_api", True):
            result["resource_metrics"] = safe("metrics_api", self._metrics)
        result["summary"] = self._health_summary(result)
        return result

    def _metrics(self) -> dict[str, Any]:
        # CustomObjectsApi serves metrics.k8s.io; this requires metrics-server.
        api = client.CustomObjectsApi()
        node_metrics = api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
        nodes = client.CoreV1Api().list_node().items
        capacity = {n.metadata.name: n.status.allocatable.to_dict() for n in nodes}
        threshold = float(self.collection.get("alert_threshold_percent", 80))
        utilization = []
        for item in node_metrics.get("items", []):
            name = item.get("metadata", {}).get("name", "unknown")
            usage = item.get("usage", {})
            alloc = capacity.get(name, {})
            cpu_used = self._quantity(usage.get("cpu", "0"), "cpu")
            cpu_total = self._quantity(alloc.get("cpu", "0"), "cpu")
            mem_used = self._quantity(usage.get("memory", "0"), "memory")
            mem_total = self._quantity(alloc.get("memory", "0"), "memory")
            cpu_pct = 100 * cpu_used / cpu_total if cpu_total else None
            mem_pct = 100 * mem_used / mem_total if mem_total else None
            utilization.append({"node": name, "cpu_usage_cores": cpu_used,
                                "cpu_allocatable_cores": cpu_total, "cpu_percent": cpu_pct,
                                "memory_usage_bytes": int(mem_used), "memory_allocatable_bytes": int(mem_total),
                                "memory_percent": mem_pct,
                                "threshold_breached": bool((cpu_pct is not None and cpu_pct >= threshold) or
                                                            (mem_pct is not None and mem_pct >= threshold))})
        breaches = [n for n in utilization if n["threshold_breached"]]
        metrics = {"nodes": node_metrics,
                   "pods": api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "pods"),
                   "node_utilization": utilization, "threshold_percent": threshold,
                   "threshold_breaches": breaches,
                   "note": "Point-in-time usage divided by node allocatable capacity; Metrics Server is not historical."}
        if breaches:
            # Surface resource-threshold breaches in the top-level health summary.
            # collection_kubernetes() builds the summary after this method returns.
            try:
                self._send_threshold_email(breaches, threshold)
            except Exception as exc:
                self.record_error("email_alert", exc)
                metrics["alert_delivery_error"] = str(exc)
        return metrics

    @staticmethod
    def _quantity(value: str, kind: str) -> float:
        """Convert Kubernetes CPU or memory quantity to cores or bytes."""
        value = str(value).strip()
        suffixes = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3,
                    "Ti": 1024**4, "K": 1000, "M": 1000**2,
                    "G": 1000**3, "T": 1000**4}
        if kind == "cpu":
            if value.endswith("n"):
                return float(value[:-1]) / 1e9
            if value.endswith("u"):
                return float(value[:-1]) / 1e6
            if value.endswith("m"):
                return float(value[:-1]) / 1000
            return float(value)
        for suffix, multiplier in suffixes.items():
            if value.endswith(suffix):
                return float(value[:-len(suffix)]) * multiplier
        return float(value)

    def _send_threshold_email(self, breaches: list[dict[str, Any]], threshold: float) -> None:
        cfg = self.backends.get("email_alerts", {})
        if not cfg.get("enabled", False):
            LOG.warning("CPU/memory threshold >= %s%% reached; email alerts are disabled", threshold)
            return
        recipients = cfg.get("to", [])
        host, sender = cfg.get("smtp_host"), cfg.get("from")
        if not recipients or not host or not sender:
            raise ValueError("Enabled email_alerts requires smtp_host, from, and at least one to recipient")
        lines = [f"Cluster: {self.name}", f"Threshold: {threshold}% of node allocatable", ""]
        for node in breaches:
            cpu = f"{node['cpu_percent']:.1f}%" if node["cpu_percent"] is not None else "unknown"
            mem = f"{node['memory_percent']:.1f}%" if node["memory_percent"] is not None else "unknown"
            lines.append(f"{node['node']}: CPU {cpu}, memory {mem}")
        message = EmailMessage()
        message["Subject"] = f"[{self.name}] Kubernetes CPU/memory threshold reached"
        message["From"] = sender
        message["To"] = ", ".join(recipients)
        message.set_content("\n".join(lines))
        port = int(cfg.get("smtp_port", 587))
        username = os.getenv(cfg.get("username_env", "SMTP_USERNAME"))
        password = os.getenv(cfg.get("password_env", "SMTP_PASSWORD"))
        context = ssl.create_default_context()
        with smtplib.SMTP(host, port, timeout=int(cfg.get("timeout_seconds", 20))) as smtp:
            if cfg.get("starttls", True):
                smtp.starttls(context=context)
            if username and password:
                smtp.login(username, password)
            smtp.send_message(message)
        LOG.warning("Threshold alert emailed to configured team (%d recipient(s))", len(recipients))

    def _collect_logs(self, core: client.CoreV1Api, namespaces: list[str], options: dict[str, Any]) -> list[dict[str, Any]]:
        selector = options.get("label_selector", "")
        tail = int(options.get("tail_lines", 500))
        since = int(options.get("since_seconds", 3600))
        max_pods = int(options.get("max_pods", 200))
        pods = core.list_pod_for_all_namespaces(label_selector=selector).items
        if namespaces:
            pods = [p for p in pods if p.metadata.namespace in namespaces]
        captured = []
        for pod in pods[:max_pods]:
            for container in pod.spec.containers or []:
                item = {"namespace": pod.metadata.namespace, "pod": pod.metadata.name,
                        "container": container.name, "captured_at": utc_now()}
                try:
                    item["log"] = core.read_namespaced_pod_log(
                        pod.metadata.name, pod.metadata.namespace, container=container.name,
                        tail_lines=tail, since_seconds=since, timestamps=True,
                        _preload_content=True)
                except ApiException as exc:
                    item["error"] = f"Kubernetes API {exc.status}: {exc.reason}"
                captured.append(item)
        return captured

    @staticmethod
    def _health_summary(data: dict[str, Any]) -> dict[str, Any]:
        pods = data.get("pods", []) if isinstance(data.get("pods"), list) else []
        nodes = data.get("nodes", []) if isinstance(data.get("nodes"), list) else []
        pod_bad = [p.get("metadata", {}).get("name") for p in pods
                   if p.get("status", {}).get("phase") not in ("Running", "Succeeded")]
        node_bad = [n.get("metadata", {}).get("name") for n in nodes
                    if not any(c.get("type") == "Ready" and c.get("status") == "True"
                               for c in n.get("status", {}).get("conditions", []))]
        metrics = data.get("resource_metrics", {})
        breaches = metrics.get("threshold_breaches", []) if isinstance(metrics, dict) else []
        api_reachable = data.get("health", {}).get("api_reachable", False)
        incomplete = [key for key in ("nodes", "pods", "resource_metrics")
                      if isinstance(data.get(key), dict) and "collection_error" in data[key]]
        degraded = bool(node_bad or pod_bad or breaches or not api_reachable or incomplete)
        return {"api_reachable": api_reachable,
                "node_count": len(nodes), "not_ready_nodes": node_bad,
                "pod_count": len(pods), "non_running_pods": pod_bad,
                "incomplete_sections": incomplete,
                "threshold_percent": metrics.get("threshold_percent") if isinstance(metrics, dict) else None,
                "resource_threshold_breaches": breaches,
                "status": "degraded" if degraded else "healthy"}

    def collect_prometheus(self) -> dict[str, Any]:
        """Run configured PromQL queries against a Prometheus-compatible API."""
        backend = self.backends.get("prometheus", {})
        if not backend.get("enabled", False):
            return {"enabled": False}
        base = backend["url"].rstrip("/")
        headers = self._backend_headers(backend)
        output = {"enabled": True, "collected_at": utc_now(), "results": {}}
        end = int(time.time())
        start = end - int(backend.get("range_seconds", 3600))
        step = backend.get("step", "60s")
        for name, query in backend.get("queries", {}).items():
            try:
                response = self.session.get(base + "/api/v1/query_range", params={
                    "query": query, "start": start, "end": end, "step": step},
                    headers=headers, timeout=int(backend.get("timeout_seconds", 20)))
                response.raise_for_status()
                output["results"][name] = response.json()
            except Exception as exc:
                output["results"][name] = {"error": str(exc)}
                self.record_error("prometheus:" + name, exc)
        return output

    def collect_traces(self) -> dict[str, Any]:
        """Optional trace export/query bridge. Expects a configured backend URL."""
        backend = self.backends.get("traces", {})
        if not backend.get("enabled", False):
            return {"enabled": False, "note": "Configure a trace backend query API or OTLP receiver."}
        # OTLP is a write protocol; it cannot retrieve historical traces. The
        # backend-specific query URL must be supplied (e.g. Tempo search API).
        url = backend.get("query_url")
        if not url:
            return {"enabled": True, "retrieval_supported": False,
                    "note": "Set query_url for the trace backend search API. OTLP itself is ingest-only."}
        try:
            response = self.session.get(url, params=backend.get("params", {}),
                                        headers=self._backend_headers(backend),
                                        timeout=int(backend.get("timeout_seconds", 20)))
            response.raise_for_status()
            try:
                body = response.json()
            except ValueError:
                body = response.text
            return {"enabled": True, "retrieval_supported": True, "status_code": response.status_code,
                    "collected_at": utc_now(), "data": body}
        except Exception as exc:
            self.record_error("traces", exc)
            return {"enabled": True, "retrieval_supported": True, "error": str(exc)}

    @staticmethod
    def _backend_headers(backend: dict[str, Any]) -> dict[str, str]:
        headers = dict(backend.get("headers", {}))
        token_env = backend.get("token_env")
        if token_env and os.getenv(token_env):
            headers["Authorization"] = "Bearer " + os.environ[token_env]
        return headers

    def run(self) -> None:
        self.out.mkdir(parents=True, exist_ok=True)
        try:
            data = self.collect_kubernetes()
            data["prometheus"] = self.collect_prometheus()
            data["traces"] = self.collect_traces()
            data["errors"] = self.errors
            data["collector"] = {"version": "1.0.0", "cluster": self.name}
            write_json(self.out / f"{self.name}-snapshot-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json", data)
            LOG.info("Collection complete; output: %s", self.out.resolve())
        except Exception as exc:
            write_json(self.out / "collection-error.json", {"time": utc_now(), "cluster": self.name,
                                                               "fatal_error": str(exc), "errors": self.errors})
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="cluster_config.yaml", help="YAML cluster and backend configuration")
    parser.add_argument("--output", help="Override output directory")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg_path = Path(args.config).expanduser()
    try:
        cfg = load_config(cfg_path)
        output = Path(args.output or cfg["collection"].get("output_dir", "./output"))
        Collector(cfg, output).run()
        return 0
    except Exception as exc:
        LOG.error("Collector stopped: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
