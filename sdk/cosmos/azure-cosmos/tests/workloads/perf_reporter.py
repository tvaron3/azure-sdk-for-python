# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
"""Background reporter that drains Stats and upserts PerfResult documents to Cosmos DB."""

import logging
import os
import socket
import threading
import uuid
from datetime import datetime, timezone

from perf_stats import Stats

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


def _get_sdk_version() -> str:
    """Get the azure-cosmos SDK version string."""
    try:
        from azure.cosmos import __version__
        return __version__
    except Exception:
        return "unknown"


def _get_cpu_percent(process=None) -> float:
    """Get current process CPU percent."""
    if psutil and process:
        try:
            return process.cpu_percent(interval=None)
        except Exception:
            pass
    return 0.0


def _get_memory_bytes(process=None) -> int:
    """Get current process RSS in bytes."""
    if psutil and process:
        try:
            return process.memory_info().rss
        except Exception:
            pass
    # Fallback: parse /proc on Linux
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024  # kB to bytes
    except Exception:
        pass
    return 0


def _get_system_cpu_percent() -> float:
    """Get system-wide CPU percent."""
    if psutil:
        try:
            return psutil.cpu_percent(interval=None)
        except Exception:
            pass
    return 0.0


def _get_system_memory() -> tuple[int, int]:
    """Get system total and used memory in bytes."""
    if psutil:
        try:
            mem = psutil.virtual_memory()
            return mem.total, mem.used
        except Exception:
            pass
    # Fallback: parse /proc/meminfo
    try:
        info = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = int(parts[1]) * 1024
        total = info.get("MemTotal", 0)
        available = info.get("MemAvailable", 0)
        return total, total - available
    except Exception:
        pass
    return 0, 0


class PerfReporter:
    """Background reporter that upserts PerfResult documents to Cosmos DB.

    Uses a daemon thread with a sync CosmosClient. The reporter drains
    Stats at the configured interval and upserts one PerfResult document
    per operation. All errors are caught and logged — the workload is
    never affected.
    """

    def __init__(self, stats: Stats, config: dict):
        self._stats = stats
        self._config = config
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._client = None
        self._container = None
        self._hostname = socket.gethostname()
        self._sdk_version = _get_sdk_version()
        self._process = psutil.Process() if psutil else None

    def start(self):
        """Start the background reporting thread (daemon)."""
        self._thread = threading.Thread(target=self._run, daemon=True, name="perf-reporter")
        self._thread.start()
        logger.info("PerfReporter started (interval=%ds, workload_id=%s)",
                     self._config["report_interval"], self._config["workload_id"])

    def stop(self):
        """Stop the reporter and flush final results."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        # Final flush
        try:
            self._ensure_container()
            self._flush()
        except Exception as e:
            logger.warning("PerfReporter final flush failed: %s", e)
        # Close the CosmosClient to release connection pools
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
        logger.info("PerfReporter stopped")

    def _run(self):
        """Reporter loop: wait for interval, then flush."""
        try:
            self._ensure_container()
        except Exception as e:
            logger.warning("PerfReporter failed to initialize Cosmos client: %s", e)
            return

        # Prime psutil CPU counters (first call always returns 0)
        _get_cpu_percent(self._process)
        _get_system_cpu_percent()

        while not self._stop_event.wait(timeout=self._config["report_interval"]):
            try:
                self._flush()
            except Exception as e:
                logger.warning("PerfReporter flush failed: %s", e)

    def _ensure_container(self):
        """Lazily create the sync CosmosClient and get the container reference."""
        if self._container is not None:
            return

        from azure.cosmos import CosmosClient, PartitionKey
        from azure.identity import DefaultAzureCredential

        endpoint = self._config["results_endpoint"]
        if not endpoint:
            raise ValueError("RESULTS_COSMOS_URI is not set")

        credential = DefaultAzureCredential()
        self._client = CosmosClient(endpoint, credential)
        db = self._client.get_database_client(self._config["results_database"])
        self._container = db.get_container_client(self._config["results_container"])

    def _flush(self):
        """Drain stats and upsert PerfResult + ErrorResult documents."""
        if self._container is None:
            return

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        cpu = _get_cpu_percent(self._process)
        mem = _get_memory_bytes(self._process)
        sys_cpu = _get_system_cpu_percent()
        sys_total, sys_used = _get_system_memory()

        # Import workload configs for config snapshot
        concurrency = _safe_int_env("COSMOS_CONCURRENT_REQUESTS", 100)
        preferred = os.environ.get("COSMOS_PREFERRED_LOCATIONS", "")
        excluded = os.environ.get("COSMOS_CLIENT_EXCLUDED_LOCATIONS", "")
        ppcb = os.environ.get("AZURE_COSMOS_ENABLE_CIRCUIT_BREAKER", "false").lower() == "true"

        # Atomically drain both summaries and errors
        summaries, errors = self._stats.drain_all()
        for s in summaries:
            doc = {
                "id": str(uuid.uuid4()),
                "partition_key": str(uuid.uuid4()),
                "workload_id": self._config["workload_id"],
                "commit_sha": self._config["commit_sha"],
                "hostname": self._hostname,
                "TIMESTAMP": now,
                "operation": s["operation"],
                "count": s["count"],
                "errors": s["errors"],
                "min_ms": round(s["min_ms"], 3),
                "max_ms": round(s["max_ms"], 3),
                "mean_ms": round(s["mean_ms"], 3),
                "p50_ms": round(s["p50_ms"], 3),
                "p90_ms": round(s["p90_ms"], 3),
                "p99_ms": round(s["p99_ms"], 3),
                "cpu_percent": round(cpu, 1),
                "memory_bytes": mem,
                "system_cpu_percent": round(sys_cpu, 1),
                "system_total_memory_bytes": sys_total,
                "system_used_memory_bytes": sys_used,
                "sdk_language": "python",
                "sdk_version": self._sdk_version,
                "config_concurrency": concurrency,
                "config_application_region": preferred,
                "config_excluded_regions": excluded,
                "config_ppcb_enabled": ppcb,
            }
            try:
                self._container.upsert_item(doc)
            except Exception as e:
                logger.warning("PerfReporter upsert failed for %s: %s", s["operation"], e)

        # Upsert error documents
        for err in errors:
            doc = {
                "id": str(uuid.uuid4()),
                "partition_key": str(uuid.uuid4()),
                "workload_id": self._config["workload_id"],
                "commit_sha": self._config["commit_sha"],
                "hostname": self._hostname,
                "TIMESTAMP": now,
                "operation": err["operation"],
                "error_message": err["error_message"][:2000],
                "source_message": err["source_message"][:4000],
                "sdk_language": "python",
                "error_status_code": err.get("error_status_code"),
                "error_sub_status_code": err.get("error_sub_status_code"),
            }
            try:
                self._container.upsert_item(doc)
            except Exception as e:
                logger.warning("PerfReporter error upsert failed: %s", e)


def _safe_int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (ValueError, TypeError):
        return default
