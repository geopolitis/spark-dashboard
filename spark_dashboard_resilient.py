#!/usr/bin/env python3
import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


DEFAULTS = {
    "SPARK_DASHBOARD_PORT": "8090",
    "SPARK_DASHBOARD_INTERVAL": "15",
    "SPARK_DASHBOARD_HISTORY": str(24 * 60 * 60),
    "SPARK_DASHBOARD_HISTORY_FILE": "data/history.jsonl",
    "LOCAL_NODE_ID": "local",
    "LOCAL_NODE_LABEL": "Local node",
    "LOCAL_NODE_HOST": "127.0.0.1",
    "LOCAL_NODE_SSH": "",
    "LOCAL_VLLM_URL": "http://127.0.0.1:8080",
    "LOCAL_PROXY_URL": "http://127.0.0.1:8081",
    "REMOTE_NODE_ID": "remote",
    "REMOTE_NODE_LABEL": "Remote node",
    "REMOTE_NODE_HOST": "",
    "REMOTE_NODE_SSH": "",
    "REMOTE_VLLM_URL": "",
    "REMOTE_PROXY_URL": "",
}


def config(name):
    return os.environ.get(name, DEFAULTS[name])


def endpoint_url(explicit, host, port):
    if explicit:
        return explicit
    return f"http://{host}:{port}" if host else ""


PORT = int(config("SPARK_DASHBOARD_PORT"))
INTERVAL_SECONDS = int(config("SPARK_DASHBOARD_INTERVAL"))
HISTORY_SECONDS = int(config("SPARK_DASHBOARD_HISTORY"))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def config_path(name):
    path = config(name)
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)


HISTORY_FILE = config_path("SPARK_DASHBOARD_HISTORY_FILE")
HISTORY_COMPACT_EVERY = max(1, int(300 / max(INTERVAL_SECONDS, 1)))

def build_nodes():
    remote_host = config("REMOTE_NODE_HOST")
    remote_vllm_url = endpoint_url(config("REMOTE_VLLM_URL"), remote_host, 8080)
    remote_proxy_url = endpoint_url(config("REMOTE_PROXY_URL"), remote_host, 8081)
    nodes = [
        {
            "id": config("LOCAL_NODE_ID"),
            "label": config("LOCAL_NODE_LABEL"),
            "host": config("LOCAL_NODE_HOST"),
            "ssh": config("LOCAL_NODE_SSH") or None,
            "vllm": [config("LOCAL_VLLM_URL")],
            "proxy": [config("LOCAL_PROXY_URL")],
        },
    ]
    if remote_host or remote_vllm_url or remote_proxy_url:
        nodes.append(
            {
                "id": config("REMOTE_NODE_ID"),
                "label": config("REMOTE_NODE_LABEL"),
                "host": remote_host,
                "ssh": config("REMOTE_NODE_SSH") or remote_host or None,
                "vllm": [remote_vllm_url] if remote_vllm_url else [],
                "proxy": [remote_proxy_url] if remote_proxy_url else [],
            }
        )
    return nodes


NODES = build_nodes()

STATE = {
    "updated_at": None,
    "interval_seconds": INTERVAL_SECONDS,
    "history_seconds": HISTORY_SECONDS,
    "nodes": {},
    "history": [],
}
LOCK = threading.Lock()
SAMPLES_SINCE_COMPACT = 0


def now_ms():
    return int(time.time() * 1000)


def history_cutoff_ms():
    return now_ms() - HISTORY_SECONDS * 1000


def recent_history(items):
    cutoff = history_cutoff_ms()
    return [item for item in items if isinstance(item, dict) and item.get("t", 0) >= cutoff]


def load_history():
    items = []
    if not os.path.exists(HISTORY_FILE):
        return items
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        print(f"history load failed: {exc}", flush=True)
        return []
    return recent_history(items)


def append_history_sample(sample):
    try:
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        with open(HISTORY_FILE, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample, separators=(",", ":")) + "\n")
    except Exception as exc:
        print(f"history append failed: {exc}", flush=True)


def compact_history_file(history):
    try:
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        tmp_path = f"{HISTORY_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            for item in recent_history(history):
                handle.write(json.dumps(item, separators=(",", ":")) + "\n")
        os.replace(tmp_path, HISTORY_FILE)
    except Exception as exc:
        print(f"history compact failed: {exc}", flush=True)


def ok(value=None, **extra):
    data = {"ok": True, "error": None}
    if value is not None:
        data["value"] = value
    data.update(extra)
    return data


def fail(error, **extra):
    data = {"ok": False, "error": str(error)}
    data.update(extra)
    return data


def run_cmd(cmd, timeout=4):
    try:
        completed = subprocess.run(
            cmd,
            shell=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(message or f"exit {completed.returncode}")
        return completed.stdout.strip()
    except Exception as exc:
        raise RuntimeError(str(exc))


def node_cmd(node, command, timeout=4):
    if node.get("ssh"):
        safe = command.replace("'", "'\"'\"'")
        return run_cmd(
            f"ssh -o BatchMode=yes -o ConnectTimeout=2 -o ServerAliveInterval=2 "
            f"-o ServerAliveCountMax=1 {node['ssh']} '{safe}'",
            timeout=timeout,
        )
    return run_cmd(command, timeout=timeout)


def http_get(url, timeout=2):
    req = urllib.request.Request(url, headers={"User-Agent": "spark-dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read()
        return response.status, response.headers, body


def tcp_check(host, port, timeout=1.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return ok(latency_ms=None)
    except Exception as exc:
        return fail(exc)


def parse_float(text, pattern):
    match = re.search(pattern, text, re.MULTILINE)
    return float(match.group(1)) if match else None


def parse_sum(text, pattern):
    values = [float(match) for match in re.findall(pattern, text, re.MULTILINE)]
    return sum(values) if values else None


def parse_histogram(text, metric):
    buckets = {}
    for le, value in re.findall(rf'^{re.escape(metric)}_bucket\{{[^}}]*le="([^"]+)"[^}}]*\}}\s+([0-9.eE+-]+)', text, re.MULTILINE):
        boundary = float("inf") if le == "+Inf" else float(le)
        buckets[boundary] = buckets.get(boundary, 0.0) + float(value)
    count = parse_sum(text, rf'^{re.escape(metric)}_count\{{[^}}]*\}}\s+([0-9.eE+-]+)')
    total = parse_sum(text, rf'^{re.escape(metric)}_sum\{{[^}}]*\}}\s+([0-9.eE+-]+)')
    if not buckets and count is None and total is None:
        return None
    return {"buckets": buckets, "count": count or buckets.get(float("inf")), "sum": total}


def histogram_quantile(histogram, quantile):
    if not histogram or not histogram.get("buckets"):
        return None
    buckets = sorted(histogram["buckets"].items(), key=lambda item: item[0])
    total = histogram.get("count") or buckets[-1][1]
    if not total or total <= 0:
        return None
    target = total * quantile
    prev_le = 0.0
    prev_count = 0.0
    for le, count in buckets:
        if count >= target:
            if le == float("inf"):
                return prev_le if prev_le > 0 else None
            bucket_count = count - prev_count
            if bucket_count <= 0:
                return le
            fraction = (target - prev_count) / bucket_count
            return prev_le + (le - prev_le) * fraction
        prev_le = le
        prev_count = count
    return None


def parse_vllm_metrics(text):
    generation = parse_sum(text, r'^vllm:generation_tokens_total\{[^}]*\}\s+([0-9.eE+-]+)')
    prompt = parse_sum(text, r'^vllm:prompt_tokens_total\{[^}]*\}\s+([0-9.eE+-]+)')
    request_success = parse_sum(text, r'^vllm:request_success_total\{[^}]*\}\s+([0-9.eE+-]+)')
    prefix_hits = parse_sum(text, r'^vllm:prefix_cache_hits_total\{[^}]*\}\s+([0-9.eE+-]+)')
    prefix_queries = parse_sum(text, r'^vllm:prefix_cache_queries_total\{[^}]*\}\s+([0-9.eE+-]+)')
    prefill_compute = parse_sum(text, r'^vllm:prompt_tokens_by_source_total\{[^}]*source="local_compute"[^}]*\}\s+([0-9.eE+-]+)')
    prefill_cache_hit = parse_sum(text, r'^vllm:prompt_tokens_by_source_total\{[^}]*source="local_cache_hit"[^}]*\}\s+([0-9.eE+-]+)')
    prefill_external_kv = parse_sum(text, r'^vllm:prompt_tokens_by_source_total\{[^}]*source="external_kv_transfer"[^}]*\}\s+([0-9.eE+-]+)')
    preemptions = parse_sum(text, r'^vllm:num_preemptions_total\{[^}]*\}\s+([0-9.eE+-]+)')
    read_bytes = parse_sum(text, r'^vllm:estimated_read_bytes_per_gpu_total\{[^}]*\}\s+([0-9.eE+-]+)')
    write_bytes = parse_sum(text, r'^vllm:estimated_write_bytes_per_gpu_total\{[^}]*\}\s+([0-9.eE+-]+)')
    flops = parse_sum(text, r'^vllm:estimated_flops_per_gpu_total\{[^}]*\}\s+([0-9.eE+-]+)')
    kv = parse_sum(text, r'^vllm:kv_cache_usage_perc\{[^}]*\}\s+([0-9.eE+-]+)')
    running = parse_sum(text, r'^vllm:num_requests_running\{[^}]*\}\s+([0-9.eE+-]+)')
    waiting = parse_sum(text, r'^vllm:num_requests_waiting\{[^}]*\}\s+([0-9.eE+-]+)')
    draft = parse_sum(text, r'^vllm:spec_decode_num_draft_tokens_total\{[^}]*\}\s+([0-9.eE+-]+)')
    accepted = parse_sum(text, r'^vllm:spec_decode_num_accepted_tokens_total\{[^}]*\}\s+([0-9.eE+-]+)')
    errors = parse_sum(text, r'^vllm:request_success_total\{[^}]*finished_reason="error"[^}]*\}\s+([0-9.eE+-]+)') or 0.0
    request_stop = parse_sum(text, r'^vllm:request_success_total\{[^}]*finished_reason="stop"[^}]*\}\s+([0-9.eE+-]+)') or 0.0
    request_length = parse_sum(text, r'^vllm:request_success_total\{[^}]*finished_reason="length"[^}]*\}\s+([0-9.eE+-]+)') or 0.0
    request_abort = parse_sum(text, r'^vllm:request_success_total\{[^}]*finished_reason="abort"[^}]*\}\s+([0-9.eE+-]+)') or 0.0
    request_repetition = parse_sum(text, r'^vllm:request_success_total\{[^}]*finished_reason="repetition"[^}]*\}\s+([0-9.eE+-]+)') or 0.0
    ttft = parse_histogram(text, "vllm:time_to_first_token_seconds")
    request_prompt_tokens = parse_histogram(text, "vllm:request_prompt_tokens")
    acceptance = None
    if draft and draft > 0 and accepted is not None:
        acceptance = accepted / draft
    prefix_hit_rate = None
    if prefix_queries and prefix_queries > 0 and prefix_hits is not None:
        prefix_hit_rate = prefix_hits / prefix_queries
    return {
        "generation_tokens": generation,
        "prompt_tokens": prompt,
        "total_tokens": (generation or 0.0) + (prompt or 0.0),
        "request_success": request_success,
        "prefix_cache_hits": prefix_hits,
        "prefix_cache_queries": prefix_queries,
        "prefix_hit_rate": prefix_hit_rate,
        "prefill_compute_tokens": prefill_compute,
        "prefill_cache_hit_tokens": prefill_cache_hit,
        "prefill_external_kv_tokens": prefill_external_kv,
        "preemptions": preemptions,
        "estimated_read_bytes": read_bytes,
        "estimated_write_bytes": write_bytes,
        "estimated_flops": flops,
        "kv_cache_usage": kv,
        "requests_running": running,
        "requests_waiting": waiting,
        "draft_tokens": draft,
        "accepted_tokens": accepted,
        "acceptance_rate": acceptance,
        "request_errors": errors,
        "request_stop": request_stop,
        "request_length": request_length,
        "request_abort": request_abort,
        "request_repetition": request_repetition,
        "ttft_count": ttft.get("count") if ttft else None,
        "ttft_avg_s": (ttft.get("sum") / ttft.get("count")) if ttft and ttft.get("sum") is not None and ttft.get("count") else None,
        "ttft_p50_s": histogram_quantile(ttft, 0.50),
        "ttft_p95_s": histogram_quantile(ttft, 0.95),
        "request_prompt_count": request_prompt_tokens.get("count") if request_prompt_tokens else None,
        "request_prompt_avg_tokens": (request_prompt_tokens.get("sum") / request_prompt_tokens.get("count")) if request_prompt_tokens and request_prompt_tokens.get("sum") is not None and request_prompt_tokens.get("count") else None,
        "request_prompt_p50_tokens": histogram_quantile(request_prompt_tokens, 0.50),
        "request_prompt_p95_tokens": histogram_quantile(request_prompt_tokens, 0.95),
    }


def collect_vllm(node):
    results = []
    for base in node.get("vllm", []):
        item = {"base_url": base, "version": None, "models": None, "metrics": None, "ok": False, "error": None}
        try:
            _, _, body = http_get(f"{base}/version", timeout=2)
            version = json.loads(body.decode("utf-8"))
            item["version"] = version.get("version")
            item["ok"] = True
        except Exception as exc:
            item["error"] = f"version: {exc}"
        try:
            _, _, body = http_get(f"{base}/v1/models", timeout=2)
            models = json.loads(body.decode("utf-8"))
            item["models"] = models.get("data", [])
            item["ok"] = True
        except Exception as exc:
            if item["error"]:
                item["error"] += f"; models: {exc}"
            else:
                item["error"] = f"models: {exc}"
        try:
            _, _, body = http_get(f"{base}/metrics", timeout=2)
            item["metrics"] = parse_vllm_metrics(body.decode("utf-8", "replace"))
            item["ok"] = True
        except Exception as exc:
            if item["error"]:
                item["error"] += f"; metrics: {exc}"
            else:
                item["error"] = f"metrics: {exc}"
        results.append(item)
    return results


def parse_vllm_command(command):
    if not command:
        return None
    try:
        parts = shlex.split(command)
    except Exception:
        parts = command.split()
    if "serve" not in parts:
        return {"command": command}
    serve_index = parts.index("serve")
    config = {"command": command, "model_path": None}
    if serve_index + 1 < len(parts) and not parts[serve_index + 1].startswith("-"):
        config["model_path"] = parts[serve_index + 1]
    value_flags = {
        "--served-model-name": "served_model_name",
        "--max-model-len": "max_model_len",
        "--max-num-seqs": "max_num_seqs",
        "--max-num-batched-tokens": "max_num_batched_tokens",
        "--gpu-memory-utilization": "gpu_memory_utilization",
        "--kv-cache-dtype": "kv_cache_dtype",
        "--load-format": "load_format",
        "--attention-backend": "attention_backend",
        "--moe-backend": "moe_backend",
        "--tool-call-parser": "tool_call_parser",
    }
    bool_flags = {
        "--enable-prefix-caching": "prefix_caching",
        "--enable-chunked-prefill": "chunked_prefill",
        "--async-scheduling": "async_scheduling",
        "--trust-remote-code": "trust_remote_code",
    }
    for index, part in enumerate(parts):
        if part in value_flags and index + 1 < len(parts):
            config[value_flags[part]] = parts[index + 1]
        elif any(part.startswith(flag + "=") for flag in value_flags):
            flag, value = part.split("=", 1)
            config[value_flags[flag]] = value
        elif part in bool_flags:
            config[bool_flags[part]] = True
    return config


def collect_vllm_runtime(node):
    try:
        command = node_cmd(
            node,
            "ps -eo args= | awk '/vllm serve/ && !/awk/ {print; exit}'",
            timeout=3,
        )
        return ok(parse_vllm_command(command))
    except Exception as exc:
        return fail(exc)


def collect_proxy(node):
    items = []
    for base in node.get("proxy", []):
        item = {"base_url": base, "ok": False, "error": None, "metrics": None}
        try:
            _, _, body = http_get(f"{base}/stats", timeout=2)
            stats = json.loads(body.decode("utf-8"))
            counters = stats.get("counters") or {}
            item["metrics"] = {
                "requests": counters.get("http_requests"),
                "errors_5xx": counters.get("http_5xx"),
                "chat_requests": counters.get("chat_requests"),
                "backend_errors": counters.get("backend_errors"),
                "web_search_calls": counters.get("web_search_calls"),
                "web_search_errors": counters.get("web_search_errors"),
                "source_port": ((stats.get("last_request") or {}).get("client_port")),
            }
            item["ok"] = True
        except Exception as exc:
            try:
                _, _, body = http_get(f"{base}/metrics", timeout=2)
                text = body.decode("utf-8", "replace")
                item["metrics"] = {
                    "requests": parse_float(text, r'proxy_requests_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)'),
                    "errors_5xx": parse_float(text, r'proxy_5xx_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)'),
                }
                item["ok"] = True
            except Exception as metrics_exc:
                item["error"] = f"stats: {exc}; metrics: {metrics_exc}"
        items.append(item)
    return items


def collect_hardware(node):
    try:
        load = node_cmd(node, "cat /proc/loadavg", timeout=3)
        cpu = node_cmd(node, "awk '/^cpu / {print}' /proc/stat", timeout=3)
        gpu = node_cmd(
            node,
            "nvidia-smi --query-gpu=name,driver_version,temperature.gpu,utilization.gpu,power.draw --format=csv,noheader,nounits 2>/dev/null || true",
            timeout=4,
        )
        mem = node_cmd(
            node,
            "awk '/MemTotal|MemAvailable|SwapTotal|SwapFree/ {print $1,$2}' /proc/meminfo",
            timeout=3,
        )
        disk = node_cmd(node, "df -PB1 / | tail -1", timeout=3)
        disk_io = node_cmd(
            node,
            "awk '$3 !~ /^(loop|ram|zram)/ {read+=$6; write+=$10} END {print read*512, write*512}' /proc/diskstats",
            timeout=3,
        )
        net = node_cmd(
            node,
            "awk 'NR>2 {gsub(\":\",\"\",$1); print $1,$2,$10}' /proc/net/dev",
            timeout=3,
        )
        ip_addr = node_cmd(node, "ip -j addr show", timeout=3)
        mem_values = {}
        for line in mem.splitlines():
            parts = line.split()
            if len(parts) == 2:
                mem_values[parts[0].rstrip(":")] = int(parts[1]) * 1024
        disk_parts = disk.split()
        disk_info = None
        if len(disk_parts) >= 6:
            disk_info = {
                "total": int(disk_parts[1]),
                "used": int(disk_parts[2]),
                "available": int(disk_parts[3]),
                "mount": disk_parts[5],
            }
        net_items = []
        for line in net.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                net_items.append({"iface": parts[0], "rx": int(parts[1]), "tx": int(parts[2])})
        try:
            addr_items = json.loads(ip_addr)
            addr_by_iface = {}
            for item in addr_items:
                iface = item.get("ifname")
                addresses = []
                for addr in item.get("addr_info", []):
                    local = addr.get("local")
                    family = addr.get("family")
                    prefix = addr.get("prefixlen")
                    if local and family in ("inet", "inet6"):
                        addresses.append(f"{local}/{prefix}")
                addr_by_iface[iface] = {
                    "operstate": item.get("operstate"),
                    "mtu": item.get("mtu"),
                    "addresses": addresses,
                }
            for item in net_items:
                item.update(addr_by_iface.get(item["iface"], {}))
        except Exception:
            pass
        cpu_parts = cpu.split()
        cpu_info = None
        if len(cpu_parts) >= 8:
            values = [int(value) for value in cpu_parts[1:]]
            idle = values[3] + (values[4] if len(values) > 4 else 0)
            iowait = values[4] if len(values) > 4 else 0
            total = sum(values)
            cpu_info = {"total": total, "idle": idle, "iowait": iowait}
        disk_io_parts = disk_io.split()
        disk_io_info = None
        if len(disk_io_parts) >= 2:
            disk_io_info = {"read_bytes": int(disk_io_parts[0]), "write_bytes": int(disk_io_parts[1])}
        gpu_info = None
        if gpu.strip():
            parts = [part.strip() for part in gpu.splitlines()[0].split(",")]
            if len(parts) >= 5:
                def maybe_float(value):
                    try:
                        return float(value)
                    except Exception:
                        return None
                gpu_info = {
                    "name": parts[0],
                    "driver_version": parts[1],
                    "temperature_c": maybe_float(parts[2]),
                    "utilization_pct": maybe_float(parts[3]),
                    "power_w": maybe_float(parts[4]),
                }
        return ok(
            {
                "loadavg": load,
                "cpu": cpu_info,
                "gpu": gpu_info,
                "memory": mem_values,
                "disk": disk_info,
                "disk_io": disk_io_info,
                "network": net_items,
            }
        )
    except Exception as exc:
        return fail(exc)


def collect_node(node):
    host = node["host"]
    status = {
        "id": node["id"],
        "label": node["label"],
        "host": host,
        "updated_at": now_ms(),
        "tcp": {
            "ssh": tcp_check(host, 22),
            "dashboard": tcp_check(host, 8090),
        },
        "hardware": collect_hardware(node),
        "vllm": collect_vllm(node),
        "vllm_runtime": collect_vllm_runtime(node),
        "proxy": collect_proxy(node),
    }
    status["ok"] = bool(
        status["hardware"].get("ok")
        or any(item.get("ok") for item in status["vllm"])
        or any(item.get("ok") for item in status["proxy"])
    )
    return status


def derive_sample(nodes, previous):
    sample = {"t": now_ms(), "nodes": {}}
    for node_id, node in nodes.items():
        hardware = (node.get("hardware") or {}).get("value") or {}
        memory = hardware.get("memory") or {}
        mem_total = memory.get("MemTotal") or 0
        mem_available = memory.get("MemAvailable") or 0
        memory_pressure = (1.0 - (mem_available / mem_total)) if mem_total else None
        net_rx = sum(item.get("rx", 0) for item in hardware.get("network") or [])
        net_tx = sum(item.get("tx", 0) for item in hardware.get("network") or [])
        disk_io = hardware.get("disk_io") or {}
        disk_read = disk_io.get("read_bytes") or 0
        disk_write = disk_io.get("write_bytes") or 0
        cpu = hardware.get("cpu") or {}
        gpu = hardware.get("gpu") or {}
        generation = 0.0
        prompt = 0.0
        request_success = 0.0
        prefix_hits = 0.0
        prefix_queries = 0.0
        prefill_compute = 0.0
        prefill_cache_hit = 0.0
        prefill_external_kv = 0.0
        preemptions = 0.0
        estimated_read_bytes = 0.0
        estimated_write_bytes = 0.0
        estimated_flops = 0.0
        running = 0.0
        waiting = 0.0
        kv = None
        accepted = None
        draft = None
        errors = 0.0
        request_stop = 0.0
        request_length = 0.0
        request_abort = 0.0
        request_repetition = 0.0
        proxy_requests = 0.0
        proxy_chat_requests = 0.0
        proxy_web_search_calls = 0.0
        proxy_errors_5xx = 0.0
        proxy_backend_errors = 0.0
        proxy_web_search_errors = 0.0
        ttft_count = 0.0
        ttft_sum = 0.0
        ttft_p50 = None
        ttft_p95 = None
        prompt_count = 0.0
        prompt_sum = 0.0
        prompt_p50 = None
        prompt_p95 = None
        max_model_len = None
        for item in node.get("vllm", []):
            metrics = item.get("metrics") or {}
            for model in item.get("models") or []:
                if model.get("max_model_len"):
                    max_model_len = max(max_model_len or 0.0, float(model["max_model_len"]))
            generation += metrics.get("generation_tokens") or 0.0
            prompt += metrics.get("prompt_tokens") or 0.0
            request_success += metrics.get("request_success") or 0.0
            prefix_hits += metrics.get("prefix_cache_hits") or 0.0
            prefix_queries += metrics.get("prefix_cache_queries") or 0.0
            prefill_compute += metrics.get("prefill_compute_tokens") or 0.0
            prefill_cache_hit += metrics.get("prefill_cache_hit_tokens") or 0.0
            prefill_external_kv += metrics.get("prefill_external_kv_tokens") or 0.0
            preemptions += metrics.get("preemptions") or 0.0
            estimated_read_bytes += metrics.get("estimated_read_bytes") or 0.0
            estimated_write_bytes += metrics.get("estimated_write_bytes") or 0.0
            estimated_flops += metrics.get("estimated_flops") or 0.0
            running += metrics.get("requests_running") or 0.0
            waiting += metrics.get("requests_waiting") or 0.0
            errors += metrics.get("request_errors") or 0.0
            request_stop += metrics.get("request_stop") or 0.0
            request_length += metrics.get("request_length") or 0.0
            request_abort += metrics.get("request_abort") or 0.0
            request_repetition += metrics.get("request_repetition") or 0.0
            if metrics.get("kv_cache_usage") is not None:
                kv = max(kv or 0.0, metrics["kv_cache_usage"])
            if metrics.get("accepted_tokens") is not None:
                accepted = (accepted or 0.0) + metrics["accepted_tokens"]
            if metrics.get("draft_tokens") is not None:
                draft = (draft or 0.0) + metrics["draft_tokens"]
            if metrics.get("ttft_count") and metrics.get("ttft_avg_s") is not None:
                ttft_count += metrics["ttft_count"]
                ttft_sum += metrics["ttft_count"] * metrics["ttft_avg_s"]
            if metrics.get("ttft_p50_s") is not None:
                ttft_p50 = max(ttft_p50 or 0.0, metrics["ttft_p50_s"])
            if metrics.get("ttft_p95_s") is not None:
                ttft_p95 = max(ttft_p95 or 0.0, metrics["ttft_p95_s"])
            if metrics.get("request_prompt_count") and metrics.get("request_prompt_avg_tokens") is not None:
                prompt_count += metrics["request_prompt_count"]
                prompt_sum += metrics["request_prompt_count"] * metrics["request_prompt_avg_tokens"]
            if metrics.get("request_prompt_p50_tokens") is not None:
                prompt_p50 = max(prompt_p50 or 0.0, metrics["request_prompt_p50_tokens"])
            if metrics.get("request_prompt_p95_tokens") is not None:
                prompt_p95 = max(prompt_p95 or 0.0, metrics["request_prompt_p95_tokens"])
        for item in node.get("proxy", []):
            metrics = item.get("metrics") or {}
            proxy_requests += metrics.get("requests") or 0.0
            proxy_chat_requests += metrics.get("chat_requests") or 0.0
            proxy_web_search_calls += metrics.get("web_search_calls") or 0.0
            proxy_errors_5xx += metrics.get("errors_5xx") or 0.0
            proxy_backend_errors += metrics.get("backend_errors") or 0.0
            proxy_web_search_errors += metrics.get("web_search_errors") or 0.0
        prev = previous.get("nodes", {}).get(node_id, {}) if previous else {}
        elapsed = max((sample["t"] - previous.get("t", sample["t"])) / 1000.0, 1.0) if previous else None
        gen_tps = None
        prompt_tps = None
        total_tps = None
        request_rate = None
        err_rate = None
        cpu_busy = None
        cpu_iowait = None
        disk_read_bps = None
        disk_write_bps = None
        net_rx_bps = None
        net_tx_bps = None
        vllm_read_bps = None
        vllm_write_bps = None
        prefill_compute_tps = None
        prefill_cache_hit_tps = None
        prefill_external_kv_tps = None
        request_stop_rate = None
        request_length_rate = None
        request_abort_rate = None
        request_repetition_rate = None
        proxy_request_rate = None
        proxy_chat_rate = None
        proxy_web_search_rate = None
        proxy_5xx_rate = None
        proxy_backend_error_rate = None
        proxy_web_search_error_rate = None
        flops_per_sec = None
        network_interfaces = {}
        if elapsed and prev:
            gen_tps = max((generation - prev.get("generation_tokens", generation)) / elapsed, 0.0)
            prompt_tps = max((prompt - prev.get("prompt_tokens", prompt)) / elapsed, 0.0)
            total_tps = gen_tps + prompt_tps
            request_rate = max((request_success - prev.get("request_success", request_success)) / elapsed, 0.0)
            err_rate = max((errors - prev.get("request_errors", errors)) / elapsed, 0.0)
            total_delta = (cpu.get("total", 0) - prev.get("cpu_total", cpu.get("total", 0)))
            idle_delta = (cpu.get("idle", 0) - prev.get("cpu_idle", cpu.get("idle", 0)))
            if total_delta > 0:
                cpu_busy = max(min(1.0 - idle_delta / total_delta, 1.0), 0.0)
                cpu_iowait = max(min((cpu.get("iowait", 0) - prev.get("cpu_iowait_ticks", cpu.get("iowait", 0))) / total_delta, 1.0), 0.0)
            disk_read_bps = max((disk_read - prev.get("disk_read_bytes", disk_read)) / elapsed, 0.0)
            disk_write_bps = max((disk_write - prev.get("disk_write_bytes", disk_write)) / elapsed, 0.0)
            net_rx_bps = max((net_rx - prev.get("net_rx_bytes", net_rx)) / elapsed, 0.0)
            net_tx_bps = max((net_tx - prev.get("net_tx_bytes", net_tx)) / elapsed, 0.0)
            vllm_read_bps = max((estimated_read_bytes - prev.get("estimated_read_bytes", estimated_read_bytes)) / elapsed, 0.0)
            vllm_write_bps = max((estimated_write_bytes - prev.get("estimated_write_bytes", estimated_write_bytes)) / elapsed, 0.0)
            prefill_compute_tps = max((prefill_compute - prev.get("prefill_compute_tokens", prefill_compute)) / elapsed, 0.0)
            prefill_cache_hit_tps = max((prefill_cache_hit - prev.get("prefill_cache_hit_tokens", prefill_cache_hit)) / elapsed, 0.0)
            prefill_external_kv_tps = max((prefill_external_kv - prev.get("prefill_external_kv_tokens", prefill_external_kv)) / elapsed, 0.0)
            request_stop_rate = max((request_stop - prev.get("request_stop", request_stop)) / elapsed, 0.0)
            request_length_rate = max((request_length - prev.get("request_length", request_length)) / elapsed, 0.0)
            request_abort_rate = max((request_abort - prev.get("request_abort", request_abort)) / elapsed, 0.0)
            request_repetition_rate = max((request_repetition - prev.get("request_repetition", request_repetition)) / elapsed, 0.0)
            proxy_request_rate = max((proxy_requests - prev.get("proxy_requests", proxy_requests)) / elapsed, 0.0)
            proxy_chat_rate = max((proxy_chat_requests - prev.get("proxy_chat_requests", proxy_chat_requests)) / elapsed, 0.0)
            proxy_web_search_rate = max((proxy_web_search_calls - prev.get("proxy_web_search_calls", proxy_web_search_calls)) / elapsed, 0.0)
            proxy_5xx_rate = max((proxy_errors_5xx - prev.get("proxy_errors_5xx", proxy_errors_5xx)) / elapsed, 0.0)
            proxy_backend_error_rate = max((proxy_backend_errors - prev.get("proxy_backend_errors", proxy_backend_errors)) / elapsed, 0.0)
            proxy_web_search_error_rate = max((proxy_web_search_errors - prev.get("proxy_web_search_errors", proxy_web_search_errors)) / elapsed, 0.0)
            flops_per_sec = max((estimated_flops - prev.get("estimated_flops", estimated_flops)) / elapsed, 0.0)
            prev_ifaces = prev.get("network_interfaces") or {}
            for iface in hardware.get("network") or []:
                name = iface.get("iface")
                if not name or name == "lo":
                    continue
                old = prev_ifaces.get(name) or {}
                rx = iface.get("rx", 0)
                tx = iface.get("tx", 0)
                network_interfaces[name] = {
                    "rx": rx,
                    "tx": tx,
                    "rx_bps": max((rx - old.get("rx", rx)) / elapsed, 0.0),
                    "tx_bps": max((tx - old.get("tx", tx)) / elapsed, 0.0),
                }
        else:
            for iface in hardware.get("network") or []:
                name = iface.get("iface")
                if name and name != "lo":
                    network_interfaces[name] = {"rx": iface.get("rx", 0), "tx": iface.get("tx", 0), "rx_bps": None, "tx_bps": None}
        sample["nodes"][node_id] = {
            "ok": node.get("ok", False),
            "cpu_total": cpu.get("total"),
            "cpu_idle": cpu.get("idle"),
            "cpu_iowait_ticks": cpu.get("iowait"),
            "cpu_busy": cpu_busy,
            "cpu_iowait": cpu_iowait,
            "gpu_util_pct": gpu.get("utilization_pct"),
            "gpu_temp_c": gpu.get("temperature_c"),
            "gpu_power_w": gpu.get("power_w"),
            "memory_pressure": memory_pressure,
            "disk_read_bytes": disk_read,
            "disk_write_bytes": disk_write,
            "disk_read_bps": disk_read_bps,
            "disk_write_bps": disk_write_bps,
            "net_rx_bytes": net_rx,
            "net_tx_bytes": net_tx,
            "net_rx_bps": net_rx_bps,
            "net_tx_bps": net_tx_bps,
            "generation_tokens": generation,
            "prompt_tokens": prompt,
            "total_tokens": generation + prompt,
            "request_success": request_success,
            "request_rate": request_rate,
            "prefix_cache_hits": prefix_hits,
            "prefix_cache_queries": prefix_queries,
            "prefix_hit_rate": prefix_hits / prefix_queries if prefix_queries else None,
            "prefill_compute_tokens": prefill_compute,
            "prefill_cache_hit_tokens": prefill_cache_hit,
            "prefill_external_kv_tokens": prefill_external_kv,
            "prefill_compute_tps": prefill_compute_tps,
            "prefill_cache_hit_tps": prefill_cache_hit_tps,
            "prefill_external_kv_tps": prefill_external_kv_tps,
            "preemptions": preemptions,
            "estimated_read_bytes": estimated_read_bytes,
            "estimated_write_bytes": estimated_write_bytes,
            "estimated_flops": estimated_flops,
            "vllm_read_bps": vllm_read_bps,
            "vllm_write_bps": vllm_write_bps,
            "flops_per_sec": flops_per_sec,
            "requests_running": running,
            "requests_waiting": waiting,
            "generation_tps": gen_tps,
            "prompt_tps": prompt_tps,
            "total_tps": total_tps,
            "kv_cache_usage": kv,
            "accepted_tokens": accepted,
            "draft_tokens": draft,
            "acceptance_rate": accepted / draft if draft else None,
            "request_errors": errors,
            "error_rate": err_rate,
            "request_stop": request_stop,
            "request_length": request_length,
            "request_abort": request_abort,
            "request_repetition": request_repetition,
            "request_stop_rate": request_stop_rate,
            "request_length_rate": request_length_rate,
            "request_abort_rate": request_abort_rate,
            "request_repetition_rate": request_repetition_rate,
            "proxy_requests": proxy_requests,
            "proxy_chat_requests": proxy_chat_requests,
            "proxy_web_search_calls": proxy_web_search_calls,
            "proxy_errors_5xx": proxy_errors_5xx,
            "proxy_backend_errors": proxy_backend_errors,
            "proxy_web_search_errors": proxy_web_search_errors,
            "proxy_request_rate": proxy_request_rate,
            "proxy_chat_rate": proxy_chat_rate,
            "proxy_web_search_rate": proxy_web_search_rate,
            "proxy_5xx_rate": proxy_5xx_rate,
            "proxy_backend_error_rate": proxy_backend_error_rate,
            "proxy_web_search_error_rate": proxy_web_search_error_rate,
            "network_interfaces": network_interfaces,
            "ttft_avg_s": ttft_sum / ttft_count if ttft_count else None,
            "ttft_p50_s": ttft_p50,
            "ttft_p95_s": ttft_p95,
            "context_max_tokens": max_model_len,
            "context_prompt_avg_tokens": prompt_sum / prompt_count if prompt_count else None,
            "context_prompt_p50_tokens": prompt_p50,
            "context_prompt_p95_tokens": prompt_p95,
            "context_usage_p95": (prompt_p95 / max_model_len) if prompt_p95 is not None and max_model_len else None,
        }
    return sample


def collector():
    global SAMPLES_SINCE_COMPACT
    while True:
        nodes = {}
        for node in NODES:
            try:
                nodes[node["id"]] = collect_node(node)
            except Exception as exc:
                nodes[node["id"]] = {
                    "id": node["id"],
                    "label": node["label"],
                    "host": node["host"],
                    "updated_at": now_ms(),
                    "ok": False,
                    "error": str(exc),
                }
        with LOCK:
            previous = STATE["history"][-1] if STATE["history"] else None
            sample = derive_sample(nodes, previous)
            STATE["updated_at"] = now_ms()
            STATE["nodes"] = nodes
            STATE["history"].append(sample)
            STATE["history"] = recent_history(STATE["history"])
            history_snapshot = list(STATE["history"])
        append_history_sample(sample)
        SAMPLES_SINCE_COMPACT += 1
        if SAMPLES_SINCE_COMPACT >= HISTORY_COMPACT_EVERY:
            compact_history_file(history_snapshot)
            SAMPLES_SINCE_COMPACT = 0
        time.sleep(INTERVAL_SECONDS)


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Spark Cluster Dashboard</title>
  <style>
    :root { color-scheme: dark; --bg:#080b10; --panel:#111821; --line:#2a394b; --text:#eef3f8; --muted:#91a1b4; --ok:#36d399; --bad:#fb7185; --warn:#fbbf24; --blue:#60a5fa; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--text); }
    header { padding: 20px 24px 12px; border-bottom: 1px solid var(--line); display:flex; justify-content:space-between; gap:16px; align-items:flex-end; }
    h1 { margin:0; font-size: 22px; }
    .muted { color: var(--muted); font-size: 13px; }
    main { padding: 18px 24px 32px; display: grid; gap: 18px; }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .cards { display:grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 12px; align-items: stretch; }
    .panel, .card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; min-width: 0; }
    .card strong { display:block; font-size: 24px; margin-top: 8px; }
    .card.cluster { grid-column: 1 / -1; display:grid; grid-template-columns: minmax(180px, 260px) repeat(4, minmax(0, 1fr)); gap: 14px; align-items:center; }
    .card.node-summary { display:grid; gap: 12px; }
    .summary-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; }
    .summary-head strong { margin-top: 4px; }
    .tile-grid { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .tile { border: 1px solid rgba(255,255,255,.08); border-radius: 7px; padding: 10px; background: rgba(255,255,255,.025); min-width:0; }
    .tile[data-tip], .metric-tip[data-tip] { cursor: help; position: relative; }
    .tile[data-tip]::after, .metric-tip[data-tip]::after {
      content: attr(data-tip);
      position: absolute;
      left: 10px;
      top: calc(100% + 8px);
      width: max-content;
      max-width: min(360px, 80vw);
      padding: 8px 10px;
      border-radius: 6px;
      border: 1px solid var(--line);
      background: #05080d;
      color: var(--text);
      box-shadow: 0 10px 30px rgba(0,0,0,.35);
      font-size: 12px;
      line-height: 1.35;
      font-weight: 500;
      white-space: normal;
      opacity: 0;
      transform: translateY(4px);
      pointer-events: none;
      transition: opacity .12s ease, transform .12s ease;
      z-index: 50;
    }
    .tile[data-tip]::before, .metric-tip[data-tip]::before {
      content: "";
      position: absolute;
      left: 18px;
      top: calc(100% + 3px);
      border: 5px solid transparent;
      border-bottom-color: #05080d;
      opacity: 0;
      pointer-events: none;
      transition: opacity .12s ease;
      z-index: 51;
    }
    .tile[data-tip]:hover::after, .tile[data-tip]:focus-visible::after,
    .metric-tip[data-tip]:hover::after, .metric-tip[data-tip]:focus-visible::after,
    .tile[data-tip]:hover::before, .tile[data-tip]:focus-visible::before,
    .metric-tip[data-tip]:hover::before, .metric-tip[data-tip]:focus-visible::before {
      opacity: 1;
      transform: translateY(0);
    }
    .tile span { display:block; color: var(--muted); font-size: 12px; }
    .tile b { display:block; font-size: 18px; margin-top: 4px; font-weight: 650; overflow-wrap:anywhere; }
    .tile small { display:block; color: var(--muted); margin-top: 4px; line-height: 1.35; }
    .span-2 { grid-column: span 2; }
    .gpu-diagram { display:grid; gap: 6px; margin-top: 9px; }
    .gauge { display:grid; grid-template-columns: 42px minmax(0,1fr) 44px; gap: 6px; align-items:center; font-size: 11px; color: var(--muted); }
    .gauge-track { height: 7px; border-radius: 999px; background: rgba(255,255,255,.08); overflow:hidden; border: 1px solid rgba(255,255,255,.08); }
    .gauge-fill { display:block; height: 100%; width: var(--w); border-radius: inherit; background: var(--c); }
    .node-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom: 12px; }
    .status { padding: 4px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--line); }
    .ok { color: var(--ok); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .row { display:grid; grid-template-columns: 160px minmax(0, 1fr); gap:10px; padding: 6px 0; border-top: 1px solid rgba(255,255,255,.06); }
    .row:first-child { border-top: 0; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; overflow-wrap:anywhere; }
    .chart { width: 100%; height: 180px; border: 1px solid var(--line); border-radius: 8px; background: #0b1118; }
    .chart.large { height: 260px; }
    .chart text { font-size: 10px; }
    .chart text:not([fill]) { fill: var(--muted); }
    .chart path, .chart line { vector-effect: non-scaling-stroke; }
    .chart-category { display: grid; gap: 12px; }
    .category-head { display:flex; justify-content:space-between; gap:16px; align-items:flex-end; padding: 2px 2px 0; }
    .category-head h2 { margin: 0; font-size: 18px; }
    .category-head p { margin: 3px 0 0; color: var(--muted); font-size: 12px; line-height: 1.35; }
    .chart-head { margin-bottom: 10px; }
    .chart-head h2 { margin: 0 0 4px; font-size: 16px; }
    .chart-head p { margin: 0; color: var(--muted); font-size: 12px; line-height: 1.35; }
    .chart-head code { color: var(--text); font-size: 11px; }
    .error { margin-top: 8px; padding: 8px; background: rgba(251,113,133,.08); border: 1px solid rgba(251,113,133,.35); border-radius: 6px; color: #fecdd3; }
    .small { font-size: 12px; }
    @media (max-width: 1200px) { .cards, .grid { grid-template-columns: 1fr; } .card.cluster { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 700px) { .card.cluster, .tile-grid { grid-template-columns: 1fr; } .span-2 { grid-column: auto; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Spark Cluster Dashboard</h1>
      <div class="muted">Failure-tolerant view of vLLM, node hardware, proxy, and backend health.</div>
    </div>
    <div class="muted" id="updated">Loading...</div>
  </header>
  <main>
    <section class="cards" id="cards"></section>
    <section class="grid" id="nodes"></section>
    <section class="chart-category">
      <div class="category-head">
        <div>
          <h2>vLLM Serving</h2>
          <p>Token throughput, context pressure, cache behavior, latency, queueing, and request outcomes.</p>
        </div>
      </div>
      <div class="grid">
        <div class="panel"><div class="chart-head"><h2>Generation TPS - 24h</h2><p>rising output TPS means decode is active; persistently low values during requests point to model/kernel bottlenecks or oversized context. Metrics: <code>generation_tps</code>.</p></div><svg class="chart" id="throughput"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Prompt TPS - 24h</h2><p>spikes mean prefill/context ingestion. If prompt TPS dominates generation TPS, reduce prompt size, improve prefix cache reuse, or lower concurrency. Metrics: <code>prompt_tps</code>.</p></div><svg class="chart" id="prompt-throughput"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Total Token TPS - 24h</h2><p>total throughput should rise under load. If requests are active and this stays flat, check queue, KV cache, and GPU utilization. Metrics: <code>total_tps</code>.</p></div><svg class="chart" id="total-throughput"></svg></div>
        <div class="panel"><div class="chart-head"><h2>KV Cache - 24h</h2><p>high KV cache means context/concurrency pressure. If it approaches 1.0, reduce max context/concurrency or raise available GPU memory. Metrics: <code>kv_cache_usage</code>.</p></div><svg class="chart" id="kv"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Context Window Usage - 24h</h2><p>p95 near the max context means agents are sending huge prompts; expect higher TTFT and cache pressure. Consider compaction or lower max output. Metrics: <code>context_usage_p95</code>, <code>context_prompt_p95_tokens</code>.</p></div><svg class="chart large" id="context-usage"></svg></div>
        <div class="panel"><div class="chart-head"><h2>vLLM Prefill Compute / Cache Tokens/s - 24h</h2><p>cache-hit tokens are cheap; compute tokens are expensive. If compute dominates for repeated agent work, improve prompt reuse or prefix caching. Metrics: <code>prefill_compute_tps</code>, <code>prefill_cache_hit_tps</code>.</p></div><svg class="chart large" id="vllm-prefill-source"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Prefix Cache Efficiency - 24h</h2><p>higher hit rate is good. Low hit rate with repeated workflows means prompts are changing too much or cache is being evicted. Metrics: <code>prefix_hit_rate</code>, <code>prefill_cache_hit_tps</code>.</p></div><svg class="chart large" id="prefix-efficiency"></svg></div>
        <div class="panel"><div class="chart-head"><h2>TTFT p95 / p50 - 24h</h2><p>p95 should stay close to p50. If p95 climbs, large prompts, queueing, or KV pressure are causing slow first tokens. Metrics: <code>ttft_p95_s</code>, <code>ttft_p50_s</code>.</p></div><svg class="chart large" id="ttft-latency"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Queue Pressure - 24h</h2><p>waiting should usually be zero. If waiting grows, lower concurrency, reduce max tokens/context, or increase serving capacity. Metrics: <code>requests_running</code>, <code>requests_waiting</code>.</p></div><svg class="chart large" id="queue-pressure"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Prompt vs Generation TPS - 24h</h2><p>prompt-heavy workloads are context-bound; generation-heavy workloads are decode-bound. Use this to decide whether to optimize cache/context or decode throughput. Metrics: <code>prompt_tps</code>, <code>generation_tps</code>.</p></div><svg class="chart large" id="prompt-generation"></svg></div>
        <div class="panel"><div class="chart-head"><h2>DFlash Acceptance - 24h</h2><p>higher acceptance means speculative tokens are useful; low acceptance means draft work is wasted and may hurt throughput. Metrics: <code>acceptance_rate</code>.</p></div><svg class="chart" id="acceptance"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Error Rate - 24h</h2><p>should stay at zero. Any rise means failed vLLM requests; check context overflow, backend errors, and client timeouts. Metrics: <code>error_rate</code>.</p></div><svg class="chart" id="errors"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Request Outcomes - 24h</h2><p>stop is healthy. Length means max tokens truncation; error/abort/repetition need investigation. Metrics: <code>request_stop_rate</code>, <code>request_length_rate</code>, <code>error_rate</code>, <code>request_abort_rate</code>.</p></div><svg class="chart large" id="request-outcomes"></svg></div>
      </div>
    </section>

    <section class="chart-category">
      <div class="category-head">
        <div>
          <h2>OS / Hardware</h2>
          <p>Host CPU, memory, GPU thermals, GPU power, and storage pressure.</p>
        </div>
      </div>
      <div class="grid">
        <div class="panel"><div class="chart-head"><h2>GPU Temperature - 24h</h2><p>rising temperature under load is normal; sustained high temperature can throttle clocks. Improve cooling if temperature climbs while TPS drops. Metrics: <code>gpu_temp_c</code>.</p></div><svg class="chart" id="gpu-temp"></svg></div>
        <div class="panel"><div class="chart-head"><h2>GPU Power - 24h</h2><p>power rising with TPS means GPU work is happening; low power during requests suggests CPU, I/O, queue, or scheduler bottleneck. Metrics: <code>gpu_power_w</code>.</p></div><svg class="chart" id="gpu-power"></svg></div>
        <div class="panel"><div class="chart-head"><h2>GPU Temp / Power / Util - 24h</h2><p>healthy load shows utilization and power moving together. High temperature with low utilization suggests cooling or background load; low utilization with queues suggests serving bottlenecks. Metrics: <code>gpu_temp_c</code>, <code>gpu_power_w</code>, <code>gpu_util_pct</code>.</p></div><svg class="chart large" id="gpu-combined"></svg></div>
        <div class="panel"><div class="chart-head"><h2>CPU Busy - 24h</h2><p>CPU spikes are normal for orchestration; sustained high CPU with low GPU utilization means host-side overhead may be limiting vLLM. Metrics: <code>cpu_busy</code>.</p></div><svg class="chart" id="cpu"></svg></div>
        <div class="panel"><div class="chart-head"><h2>IOwait - 24h</h2><p>low IOwait is good. If IOwait rises during inference, disk, swap, or model loading is stalling the host. Metrics: <code>cpu_iowait</code>.</p></div><svg class="chart" id="iowait"></svg></div>
        <div class="panel"><div class="chart-head"><h2>Memory Pressure / KV Cache - 24h</h2><p>high host memory plus high KV cache means you are close to pressure. Reduce context/concurrency or increase memory headroom. Metrics: <code>memory_pressure</code>, <code>kv_cache_usage</code>.</p></div><svg class="chart large" id="memory-kv"></svg></div>
        <div class="panel"><div class="chart-head"><h2>IOwait / Disk I/O - 24h</h2><p>disk traffic is fine during startup, but high IOwait during requests suggests swap or storage bottlenecks. Metrics: <code>cpu_iowait</code>, <code>disk_read_bps</code>.</p></div><svg class="chart large" id="io-disk"></svg></div>
      </div>
    </section>

    <section class="chart-category">
      <div class="category-head">
        <div>
          <h2>Network</h2>
          <p>Interface traffic for API, dashboard, Wi-Fi, and node-to-node paths.</p>
        </div>
      </div>
      <div class="grid">
        <div class="panel"><div class="chart-head"><h2>Network Interfaces - 24h</h2><p>IB/bond traffic should carry node-to-node work; Wi-Fi traffic should mostly be API/proxy/dashboard. Unexpected Wi-Fi spikes may mean traffic is not using IB. Metrics: per-interface RX+TX bytes/s.</p></div><svg class="chart large" id="network-ifaces"></svg></div>
      </div>
    </section>

    <section class="chart-category">
      <div class="category-head">
        <div>
          <h2>Proxy / Internet Access</h2>
          <p>Proxy request mix, web-search activity, and upstream/backend failure rate.</p>
        </div>
      </div>
      <div class="grid">
        <div class="panel"><div class="chart-head"><h2>Proxy Activity / Errors - 24h</h2><p>request/search rates show tool and internet use. 5xx/backend errors should stay zero; if they rise, inspect proxy backend and internet path. Metrics: <code>proxy_request_rate</code>, <code>proxy_chat_rate</code>, <code>proxy_web_search_rate</code>, <code>proxy_5xx_rate</code>.</p></div><svg class="chart large" id="proxy-activity"></svg></div>
      </div>
    </section>
  </main>
  <script>
    const fmt = (v, d=1) => {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "n/a";
      return new Intl.NumberFormat(undefined, {minimumFractionDigits: d, maximumFractionDigits: d}).format(Number(v));
    };
    const ms = v => v === null || v === undefined || Number.isNaN(Number(v)) ? "n/a" : `${fmt(Number(v) * 1000, 0)} ms`;
    const pct = v => v === null || v === undefined || Number.isNaN(Number(v)) ? "n/a" : `${fmt(Number(v) * 100, 1)}%`;
    const bytes = v => {
      if (v === null || v === undefined || Number.isNaN(v)) return "n/a";
      const units = ["B","KB","MB","GB","TB"];
      let n = Number(v), i = 0;
      while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
      return `${fmt(n, i ? 1 : 0)} ${units[i]}`;
    };
    function esc(s) { return String(s ?? "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
    function tip(s) { return ` data-tip="${esc(s)}" tabindex="0"`; }
    function firstModel(node) {
      for (const v of node.vllm || []) {
        const model = (v.models || [])[0];
        if (model) return model;
      }
      return {};
    }
    function shortModelName(name) {
      name = String(name || "n/a");
      if (name.length <= 34) return name;
      const parts = name.split("/");
      return parts.length > 1 ? parts.slice(-2).join("/") : `${name.slice(0, 31)}...`;
    }
    function clamp(v, min=0, max=100) {
      v = Number(v);
      if (Number.isNaN(v)) return 0;
      return Math.max(min, Math.min(max, v));
    }
    function gpuDiagram(h) {
      const temp = clamp(h.gpu_temp_c, 0, 100);
      const power = clamp(h.gpu_power_w, 0, 120);
      const util = clamp(h.gpu_util_pct, 0, 100);
      return `<div class="gpu-diagram" aria-label="GPU temperature, power, and utilization diagram">
        <div class="gauge"><span>Util</span><div class="gauge-track"><i class="gauge-fill" style="--w:${util}%;--c:#50d5ff"></i></div><span>${fmt(h.gpu_util_pct,0)}%</span></div>
        <div class="gauge"><span>Temp</span><div class="gauge-track"><i class="gauge-fill" style="--w:${temp}%;--c:#ff6b6b"></i></div><span>${fmt(h.gpu_temp_c,0)}C</span></div>
        <div class="gauge"><span>Power</span><div class="gauge-track"><i class="gauge-fill" style="--w:${power / 120 * 100}%;--c:#e8c547"></i></div><span>${fmt(h.gpu_power_w,1)}W</span></div>
      </div>`;
    }
    function nodeMetric(node, key) {
      for (const v of node.vllm || []) if (v.metrics && v.metrics[key] !== null && v.metrics[key] !== undefined) return v.metrics[key];
      return null;
    }
    function renderChart(id, history, metric, maxHint, nodeDefs) {
      const svg = document.getElementById(id);
      history = Array.isArray(history) ? history : [];
      const W = 640, H = 180, L = 44, R = 12, T = 12, B = 28;
      svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
      const palette = ["#36d399", "#60a5fa", "#fbbf24", "#fb7185"];
      const series = (nodeDefs || []).map((node, idx) => ({
        node: node.id,
        label: node.label || node.id,
        color: palette[idx % palette.length],
        points: history.map(h => ({ t:h.t, v:h.nodes[node.id] ? h.nodes[node.id][metric] : null })).filter(p => p.v !== null && p.v !== undefined)
      }));
      const all = series.flatMap(s => s.points);
      const minT = history[0]?.t || Date.now() - 1, maxT = history[history.length - 1]?.t || Date.now();
      let maxV = Math.max(maxHint || 0, ...all.map(p => p.v), 1);
      const x = t => L + ((t - minT) / Math.max(maxT - minT, 1)) * (W - L - R);
      const y = v => H - B - (v / maxV) * (H - T - B);
      let html = `<line x1="${L}" y1="${T}" x2="${L}" y2="${H-B}" stroke="#2a394b"/><line x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}" stroke="#2a394b"/>`;
      for (let i=0;i<=4;i++) {
        const val = maxV * i / 4, yy = y(val);
        html += `<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#172232"/><text x="4" y="${yy+3}">${fmt(val, metric.includes("rate") || metric.includes("usage") || metric.includes("acceptance") ? 2 : 1)}</text>`;
      }
      html += `<text x="${L}" y="${H-8}">${new Date(minT).toLocaleTimeString()}</text><text x="${W-92}" y="${H-8}">${new Date(maxT).toLocaleTimeString()}</text>`;
      for (const s of series) {
        if (!s.points.length) continue;
        const d = s.points.map((p,i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(" ");
        html += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2"/><text x="${W-110}" y="${18 + series.indexOf(s) * 16}" fill="${s.color}">${esc(s.label)}</text>`;
      }
      svg.innerHTML = html;
    }
    function renderGpuCombinedChart(id, history, nodeDefs) {
      const svg = document.getElementById(id);
      if (!svg) return;
      history = Array.isArray(history) ? history : [];
      const W = 760, H = 260, L = 48, R = 58, T = 20, B = 42;
      svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
      const minT = history[0]?.t || Date.now() - 1, maxT = history[history.length - 1]?.t || Date.now();
      const nodes = (nodeDefs || []).map((node, idx) => ({
        id: node.id,
        label: node.label || node.id,
        dash: idx === 0 ? "" : "5 4",
        alpha: idx === 0 ? 1 : .95,
      }));
      const colors = {temp:"#ff6b6b", power:"#e8c547", util:"#50d5ff"};
      const x = t => L + ((t - minT) / Math.max(maxT - minT, 1)) * (W - L - R);
      const yTemp = v => H - B - (clamp(v, 0, 100) / 100) * (H - T - B);
      const yPower = v => H - B - (clamp(v, 0, 120) / 120) * (H - T - B);
      const yUtil = v => H - B - (clamp(v, 0, 100) / 100) * (H - T - B);
      const pathFor = (node, metric, yFn) => {
        const pts = history.map(h => ({t:h.t, v:h.nodes?.[node]?.[metric]})).filter(p => p.v !== null && p.v !== undefined);
        return pts.map((p,i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${yFn(p.v).toFixed(1)}`).join(" ");
      };
      let html = `
        <line x1="${L}" y1="${T}" x2="${L}" y2="${H-B}" stroke="#31445b"/>
        <line x1="${W-R}" y1="${T}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>
        <line x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>
      `;
      for (let i=0; i<=4; i++) {
        const yy = T + (i / 4) * (H - T - B);
        html += `<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#172232"/>`;
      }
      html += `
        <text x="4" y="${T+4}" fill="${colors.temp}">100C</text>
        <text x="10" y="${H-B+4}" fill="${colors.temp}">0C</text>
        <text x="${W-R+6}" y="${T+4}" fill="${colors.power}">120W</text>
        <text x="${W-R+6}" y="${H-B+4}" fill="${colors.power}">0W</text>
        <text x="${L}" y="${H-12}" fill="${colors.util}">0%</text>
        <text x="${W-R-22}" y="${H-12}" fill="${colors.util}">100%</text>
        <text x="${L}" y="${H-2}">${new Date(minT).toLocaleTimeString()}</text>
        <text x="${W-R-72}" y="${H-2}">${new Date(maxT).toLocaleTimeString()}</text>
      `;
      for (const node of nodes) {
        const tempPath = pathFor(node.id, "gpu_temp_c", yTemp);
        const powerPath = pathFor(node.id, "gpu_power_w", yPower);
        const utilPath = pathFor(node.id, "gpu_util_pct", yUtil);
        if (tempPath) html += `<path d="${tempPath}" fill="none" stroke="${colors.temp}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
        if (powerPath) html += `<path d="${powerPath}" fill="none" stroke="${colors.power}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
        if (utilPath) html += `<path d="${utilPath}" fill="none" stroke="${colors.util}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
      }
      html += `
        <text x="${L+6}" y="${T+14}" fill="${colors.temp}">temp C</text>
        <text x="${L+70}" y="${T+14}" fill="${colors.power}">power W</text>
        <text x="${L+142}" y="${T+14}" fill="${colors.util}">util %</text>
        <text x="${W-R-138}" y="${T+14}">solid ${esc(nodes[0]?.label || "node 1")}</text>
        <text x="${W-R-138}" y="${T+30}">dashed ${esc(nodes[1]?.label || "node 2")}</text>
      `;
      svg.innerHTML = html;
    }
    function renderVllmPrefillSourceChart(id, history, nodeDefs) {
      const svg = document.getElementById(id);
      if (!svg) return;
      history = Array.isArray(history) ? history : [];
      const W = 760, H = 260, L = 66, R = 72, T = 20, B = 42;
      svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
      const minT = history[0]?.t || Date.now() - 1, maxT = history[history.length - 1]?.t || Date.now();
      const nodes = (nodeDefs || []).map((node, idx) => ({
        id: node.id,
        label: node.label || node.id,
        dash: idx === 0 ? "" : "5 4",
        alpha: idx === 0 ? 1 : .95,
      }));
      const computeColor = "#60a5fa";
      const cacheColor = "#fbbf24";
      const computeValues = [];
      const cacheValues = [];
      for (const h of history) {
        for (const node of nodes) {
          const sample = h.nodes?.[node.id] || {};
          if (sample.prefill_compute_tps !== null && sample.prefill_compute_tps !== undefined) computeValues.push(sample.prefill_compute_tps);
          if (sample.prefill_cache_hit_tps !== null && sample.prefill_cache_hit_tps !== undefined) cacheValues.push(sample.prefill_cache_hit_tps);
        }
      }
      const maxCompute = Math.max(...computeValues, 1);
      const maxCache = Math.max(...cacheValues, 1);
      const x = t => L + ((t - minT) / Math.max(maxT - minT, 1)) * (W - L - R);
      const yCompute = v => H - B - (Number(v || 0) / maxCompute) * (H - T - B);
      const yCache = v => H - B - (Number(v || 0) / maxCache) * (H - T - B);
      const pathFor = (node, metric, yFn) => {
        const pts = history.map(h => ({t:h.t, v:h.nodes?.[node]?.[metric]})).filter(p => p.v !== null && p.v !== undefined);
        return pts.map((p,i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${yFn(p.v).toFixed(1)}`).join(" ");
      };
      let html = `
        <line x1="${L}" y1="${T}" x2="${L}" y2="${H-B}" stroke="#31445b"/>
        <line x1="${W-R}" y1="${T}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>
        <line x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>
      `;
      for (let i=0; i<=4; i++) {
        const yy = T + (i / 4) * (H - T - B);
        const computeVal = maxCompute * (4 - i) / 4;
        const cacheVal = maxCache * (4 - i) / 4;
        html += `<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#172232"/>`;
        html += `<text x="4" y="${yy+3}" fill="${computeColor}">${fmt(computeVal, 0)}/s</text>`;
        html += `<text x="${W-R+6}" y="${yy+3}" fill="${cacheColor}">${fmt(cacheVal, 0)}/s</text>`;
      }
      html += `
        <text x="${L}" y="${H-12}">${new Date(minT).toLocaleTimeString()}</text>
        <text x="${W-R-72}" y="${H-12}">${new Date(maxT).toLocaleTimeString()}</text>
      `;
      for (const node of nodes) {
        const computePath = pathFor(node.id, "prefill_compute_tps", yCompute);
        const cachePath = pathFor(node.id, "prefill_cache_hit_tps", yCache);
        if (computePath) html += `<path d="${computePath}" fill="none" stroke="${computeColor}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
        if (cachePath) html += `<path d="${cachePath}" fill="none" stroke="${cacheColor}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
      }
      html += `
        <text x="${L+6}" y="${T+14}" fill="${computeColor}">compute tok/s</text>
        <text x="${L+106}" y="${T+14}" fill="${cacheColor}">cache-hit tok/s</text>
        <text x="${W-R-138}" y="${T+14}">solid ${esc(nodes[0]?.label || "node 1")}</text>
        <text x="${W-R-138}" y="${T+30}">dashed ${esc(nodes[1]?.label || "node 2")}</text>
      `;
      svg.innerHTML = html;
    }
    function renderDualAxisMetricChart(id, history, nodeDefs, leftMetric, rightMetric, labels) {
      const svg = document.getElementById(id);
      if (!svg) return;
      history = Array.isArray(history) ? history : [];
      const W = 760, H = 260, L = 58, R = 62, T = 20, B = 42;
      svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
      const minT = history[0]?.t || Date.now() - 1, maxT = history[history.length - 1]?.t || Date.now();
      const nodes = (nodeDefs || []).map((node, idx) => ({id: node.id, label: node.label || node.id, dash: idx === 0 ? "" : "5 4", alpha: idx === 0 ? 1 : .95}));
      const leftColor = labels.leftColor || "#60a5fa";
      const rightColor = labels.rightColor || "#fbbf24";
      const leftFmt = labels.leftFmt || fmt;
      const rightFmt = labels.rightFmt || fmt;
      const leftVals = [], rightVals = [];
      for (const h of history) for (const node of nodes) {
        const sample = h.nodes?.[node.id] || {};
        if (sample[leftMetric] !== null && sample[leftMetric] !== undefined) leftVals.push(sample[leftMetric]);
        if (sample[rightMetric] !== null && sample[rightMetric] !== undefined) rightVals.push(sample[rightMetric]);
      }
      const maxLeft = Math.max(labels.leftMax || 0, ...leftVals, 1);
      const maxRight = Math.max(labels.rightMax || 0, ...rightVals, 1);
      const x = t => L + ((t - minT) / Math.max(maxT - minT, 1)) * (W - L - R);
      const yLeft = v => H - B - (Number(v || 0) / maxLeft) * (H - T - B);
      const yRight = v => H - B - (Number(v || 0) / maxRight) * (H - T - B);
      const pathFor = (node, metric, yFn) => {
        const pts = history.map(h => ({t:h.t, v:h.nodes?.[node]?.[metric]})).filter(p => p.v !== null && p.v !== undefined);
        return pts.map((p,i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${yFn(p.v).toFixed(1)}`).join(" ");
      };
      let html = `<line x1="${L}" y1="${T}" x2="${L}" y2="${H-B}" stroke="#31445b"/><line x1="${W-R}" y1="${T}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/><line x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>`;
      for (let i=0; i<=4; i++) {
        const yy = T + (i / 4) * (H - T - B);
        html += `<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#172232"/>`;
        html += `<text x="4" y="${yy+3}" fill="${leftColor}">${leftFmt(maxLeft * (4 - i) / 4)}</text>`;
        html += `<text x="${W-R+6}" y="${yy+3}" fill="${rightColor}">${rightFmt(maxRight * (4 - i) / 4)}</text>`;
      }
      html += `<text x="${L}" y="${H-12}">${new Date(minT).toLocaleTimeString()}</text><text x="${W-R-72}" y="${H-12}">${new Date(maxT).toLocaleTimeString()}</text>`;
      for (const node of nodes) {
        const leftPath = pathFor(node.id, leftMetric, yLeft);
        const rightPath = pathFor(node.id, rightMetric, yRight);
        if (leftPath) html += `<path d="${leftPath}" fill="none" stroke="${leftColor}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
        if (rightPath) html += `<path d="${rightPath}" fill="none" stroke="${rightColor}" stroke-width="2" stroke-dasharray="${node.dash}" opacity="${node.alpha}"/>`;
      }
      html += `<text x="${L+6}" y="${T+14}" fill="${leftColor}">${esc(labels.left)}</text><text x="${L+126}" y="${T+14}" fill="${rightColor}">${esc(labels.right)}</text><text x="${W-R-138}" y="${T+14}">solid ${esc(nodes[0]?.label || "node 1")}</text><text x="${W-R-138}" y="${T+30}">dashed ${esc(nodes[1]?.label || "node 2")}</text>`;
      svg.innerHTML = html;
    }
    function renderMultiMetricChart(id, history, nodeDefs, metrics, formatValue) {
      const svg = document.getElementById(id);
      if (!svg) return;
      history = Array.isArray(history) ? history : [];
      const W = 760, H = 260, L = 58, R = 16, T = 20, B = 42;
      svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
      const minT = history[0]?.t || Date.now() - 1, maxT = history[history.length - 1]?.t || Date.now();
      const series = [];
      for (const node of nodeDefs || []) for (const metric of metrics) {
        const pts = history.map(h => ({t:h.t, v:h.nodes?.[node.id]?.[metric.key]})).filter(p => p.v !== null && p.v !== undefined);
        series.push({label:`${node.label || node.id} ${metric.label}`, color:metric.color, dash:metric.dash || (nodeDefs.indexOf(node) ? "5 4" : ""), points:pts});
      }
      const all = series.flatMap(s => s.points);
      const maxV = Math.max(...all.map(p => p.v), 1);
      const x = t => L + ((t - minT) / Math.max(maxT - minT, 1)) * (W - L - R);
      const y = v => H - B - (Number(v || 0) / maxV) * (H - T - B);
      let html = `<line x1="${L}" y1="${T}" x2="${L}" y2="${H-B}" stroke="#31445b"/><line x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}" stroke="#31445b"/>`;
      for (let i=0; i<=4; i++) {
        const val = maxV * i / 4, yy = y(val);
        html += `<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#172232"/><text x="4" y="${yy+3}">${formatValue ? formatValue(val) : fmt(val, 2)}</text>`;
      }
      html += `<text x="${L}" y="${H-12}">${new Date(minT).toLocaleTimeString()}</text><text x="${W-R-72}" y="${H-12}">${new Date(maxT).toLocaleTimeString()}</text>`;
      series.forEach((s, idx) => {
        if (!s.points.length) return;
        const d = s.points.map((p,i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(" ");
        html += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-dasharray="${s.dash}" opacity=".95"/>`;
        if (idx < 6) html += `<text x="${W-176}" y="${18 + idx * 14}" fill="${s.color}">${esc(s.label)}</text>`;
      });
      svg.innerHTML = html;
    }
    function renderNetworkInterfaceChart(id, history, nodeDefs) {
      const metrics = [];
      const colors = ["#36d399", "#60a5fa", "#fbbf24", "#fb7185"];
      const ifaces = ["bond0", "wlP9s9", "enp1s0f0np0", "enP2p1s0f0np0"];
      for (const iface of ifaces) {
        metrics.push({key:`network_interface_${iface}_bps`, label: iface, color: colors[metrics.length % colors.length]});
      }
      const patched = history.map(h => {
        const item = JSON.parse(JSON.stringify(h));
        for (const node of nodeDefs || []) {
          const sample = item.nodes?.[node.id] || {};
          for (const iface of ifaces) {
            const row = sample.network_interfaces?.[iface];
            sample[`network_interface_${iface}_bps`] = row ? (row.rx_bps || 0) + (row.tx_bps || 0) : null;
          }
        }
        return item;
      });
      renderMultiMetricChart(id, patched, nodeDefs, metrics, v => `${bytes(v)}/s`);
    }
    function render(data) {
      data = data || {};
      data.nodes = data.nodes || {};
      data.history = Array.isArray(data.history) ? data.history : [];
      document.getElementById("updated").textContent = data.updated_at ? `Updated ${new Date(data.updated_at).toLocaleTimeString()} every ${data.interval_seconds || "?"}s` : "No data yet";
      const nodes = Object.values(data.nodes || {});
      const latest = data.history[data.history.length - 1] || {nodes:{}};
      const cardRows = [];
      const expectedBackends = nodes.flatMap(n => [...(n.vllm || []), ...(n.proxy || [])]);
      const unhealthyBackends = expectedBackends.filter(item => !item.ok).length;
      const allNodesOk = nodes.length > 0 && nodes.every(n => n.ok) && unhealthyBackends === 0;
      const totals = nodes.reduce((acc, n) => {
        const h = latest.nodes[n.id] || {};
        acc.generation += h.generation_tps || 0;
        acc.prompt += h.prompt_tps || 0;
        acc.total += h.total_tps || 0;
        acc.power += h.gpu_power_w || 0;
        acc.issues += [...(n.vllm || []), ...(n.proxy || [])].filter(item => !item.ok).length;
        return acc;
      }, {generation:0, prompt:0, total:0, power:0, issues:0});
      cardRows.push(`<article class="card cluster">
        <div>
          <span class="muted metric-tip"${tip("Overall status derived from node health plus vLLM and proxy backend health.")}>Cluster weather</span>
          <strong class="${allNodesOk ? "ok" : "warn"}">${allNodesOk ? "Clear" : "Watch"}</strong>
          <div class="muted">${nodes.length} nodes | ${unhealthyBackends} backend issues</div>
        </div>
        <div class="tile"${tip("Total token throughput across nodes. Generation is output tokens/sec; prompt is prefill/input tokens/sec.")}><span>Total TPS</span><b>${fmt(totals.total)}</b><small>gen ${fmt(totals.generation)} | prompt ${fmt(totals.prompt)}</small></div>
        <div class="tile"${tip("Current combined GPU power draw reported by nvidia-smi across monitored nodes.")}><span>GPU Power</span><b>${fmt(totals.power,1)}W</b><small>combined current draw</small></div>
        <div class="tile"${tip("Number of vLLM and proxy endpoints currently reachable and responding.")}><span>Backends</span><b>${expectedBackends.length - unhealthyBackends}/${expectedBackends.length}</b><small>vLLM + proxy online</small></div>
        <div class="tile"${tip("Number of retained samples. Retention is controlled by SPARK_DASHBOARD_HISTORY.")}><span>History</span><b>${fmt((data.history || []).length,0)}</b><small>${fmt((data.history || []).length * (data.interval_seconds || 0) / 60,0)} min sampled</small></div>
      </article>`);
      for (const n of nodes) {
        const h = latest.nodes[n.id] || {};
        const model = firstModel(n);
        const primaryVllm = (n.vllm || [])[0] || {};
        const runtime = (n.vllm_runtime || {}).value || {};
        const contextLen = model.max_model_len || runtime.max_model_len;
        const nodeBackends = [...(n.vllm || []), ...(n.proxy || [])];
        const backendOk = nodeBackends.filter(item => item.ok).length;
        const ifaceWithIp = ((n.hardware || {}).value?.network || []).filter(i => (i.addresses || []).length && i.iface !== "lo");
        const primaryIps = ifaceWithIp.slice(0, 2).map(i => `${i.iface} ${(i.addresses || []).filter(a => !a.startsWith("fe80")).join(", ") || i.addresses[0]}`).join(" | ");
        cardRows.push(`<article class="card node-summary">
          <div class="summary-head">
            <div>
              <span class="muted">${esc(n.label)}</span>
              <strong class="${n.ok && backendOk === nodeBackends.length ? "ok" : "warn"}">${n.ok && backendOk === nodeBackends.length ? "Online" : "Degraded"}</strong>
            </div>
            <div class="status ${backendOk === nodeBackends.length ? "ok" : "warn"}">${backendOk}/${nodeBackends.length} backends</div>
          </div>
          <div class="tile-grid">
            <div class="tile"${tip("Currently served model, vLLM version, and key launch parameters parsed from /version, /v1/models, and the vLLM serve process.")}><span>Model</span><b>${esc(shortModelName(model.id || runtime.served_model_name || runtime.model_path))}</b><small>vLLM ${esc(primaryVllm.version || "n/a")} | ctx ${fmt(contextLen,0)} | seqs ${esc(runtime.max_num_seqs || "n/a")} | batch ${esc(runtime.max_num_batched_tokens || "n/a")}</small></div>
            <div class="tile"${tip("Output-token generation speed, with prompt/input token prefill speed and combined token throughput.")}><span>Tokens</span><b>${fmt(h.generation_tps)} gen/s</b><small>prompt ${fmt(h.prompt_tps)} | total ${fmt(h.total_tps)}</small></div>
            <div class="tile"${tip("TTFT is time to first token from vLLM's Prometheus histogram. Main value is p95 latency; smaller is better.")}><span>TTFT p95</span><b>${ms(h.ttft_p95_s)}</b><small>p50 ${ms(h.ttft_p50_s)} | avg ${ms(h.ttft_avg_s)}</small></div>
            <div class="tile"${tip("GPU utilization percentage, with current GPU temperature in Celsius and power draw in watts.")}><span>GPU Util</span><b>${fmt(h.gpu_util_pct,0)}%</b><small>${fmt(h.gpu_temp_c,0)}C / ${fmt(h.gpu_power_w,1)}W</small>${gpuDiagram(h)}</div>
            <div class="tile"${tip("CPU busy percentage from /proc/stat. IOwait is time waiting on disk or device I/O; memory is used pressure from MemAvailable.")}><span>CPU</span><b>${pct(h.cpu_busy)}</b><small>iowait ${pct(h.cpu_iowait)} | mem ${pct(h.memory_pressure)}</small></div>
            <div class="tile"${tip("vLLM runtime pressure and cache configuration. KV is cache occupancy; prefix is prefix-cache hit rate.")}><span>vLLM Cache</span><b>KV ${pct(h.kv_cache_usage)}</b><small>${esc(runtime.kv_cache_dtype || "kv n/a")} | prefix ${pct(h.prefix_hit_rate)} | ${runtime.prefix_caching ? "prefix on" : "prefix n/a"}</small></div>
            <div class="tile"${tip("Host disk read plus write throughput, derived from /proc/diskstats deltas.")}><span>Host I/O</span><b>${bytes((h.disk_read_bps || 0) + (h.disk_write_bps || 0))}/s</b><small>R ${bytes(h.disk_read_bps)}/s | W ${bytes(h.disk_write_bps)}/s</small></div>
            <div class="tile"${tip("Aggregate network receive plus transmit throughput across interfaces, derived from /proc/net/dev deltas.")}><span>Network</span><b>${bytes((h.net_rx_bps || 0) + (h.net_tx_bps || 0))}/s</b><small>RX ${bytes(h.net_rx_bps)}/s | TX ${bytes(h.net_tx_bps)}/s</small></div>
            <div class="tile span-2"${tip("Detected non-loopback interface addresses, useful for checking Wi-Fi, IB/bond, and service routing.")}><span>Addresses</span><b>${esc(primaryIps || "no-ip")}</b><small>${ifaceWithIp.length} interfaces with addresses</small></div>
            <div class="tile"${tip("Lifetime token counters exported by vLLM since the server started.")}><span>Total tokens</span><b>${fmt(h.total_tokens,0)}</b><small>prompt ${fmt(h.prompt_tokens,0)} | gen ${fmt(h.generation_tokens,0)}</small></div>
          </div>
        </article>`);
      }
      document.getElementById("cards").innerHTML = cardRows.join("") || `<article class="card"><span class="bad">No node data collected yet</span></article>`;
      document.getElementById("nodes").innerHTML = nodes.map(n => {
        const hw = n.hardware || {};
        const mem = hw.value?.memory || {};
        const memTotal = mem.MemTotal || 0, memAvail = mem.MemAvailable || 0;
        const memUsed = memTotal ? 1 - memAvail / memTotal : null;
        const disk = hw.value?.disk || {};
        const network = (hw.value?.network || [])
          .filter(i => i.iface !== "lo")
          .map(i => `<div class="mono small">${esc(i.iface)} ${esc(i.operstate || "")} ${esc((i.addresses || []).join(", ") || "no-ip")} RX ${bytes(i.rx)} TX ${bytes(i.tx)}</div>`)
          .join("");
        const vllm = (n.vllm || []).map(v => {
          const m = v.metrics || {};
          const model = (v.models || [])[0] || {};
          const runtime = (n.vllm_runtime || {}).value || {};
          return `<div class="${v.ok ? "ok" : "bad"} mono">${esc(v.base_url)} ${v.ok ? "ok" : esc(v.error)}
            <div class="small">model ${esc(model.id || "n/a")} ctx ${fmt(model.max_model_len,0)} vLLM ${esc(v.version || "n/a")}</div>
            <div class="small">root ${esc(model.root || runtime.model_path || "n/a")}</div>
            <div class="small">params gpu_mem ${esc(runtime.gpu_memory_utilization || "n/a")} seqs ${esc(runtime.max_num_seqs || "n/a")} batch ${esc(runtime.max_num_batched_tokens || "n/a")} kv ${esc(runtime.kv_cache_dtype || "n/a")}</div>
            <div class="small">cache prefix ${runtime.prefix_caching ? "on" : "n/a"} chunked_prefill ${runtime.chunked_prefill ? "on" : "n/a"} attention ${esc(runtime.attention_backend || "n/a")} moe ${esc(runtime.moe_backend || "n/a")}</div>
            <div class="small">gen ${fmt(m.generation_tokens,0)} prompt ${fmt(m.prompt_tokens,0)} total ${fmt(m.total_tokens,0)}</div>
            <div class="small">running ${fmt(m.requests_running,0)} waiting ${fmt(m.requests_waiting,0)} KV ${pct(m.kv_cache_usage)} prefix ${pct(m.prefix_hit_rate)}</div>
            <div class="small">TTFT p95 ${ms(m.ttft_p95_s)} p50 ${ms(m.ttft_p50_s)} avg ${ms(m.ttft_avg_s)}</div>
            <div class="small">success ${fmt(m.request_success,0)} errors ${fmt(m.request_errors,0)} preemptions ${fmt(m.preemptions,0)}</div>
          </div>`;
        }).join("");
        const proxy = (n.proxy || []).map(p => `<div class="${p.ok ? "ok" : "warn"} mono">${esc(p.base_url)} ${p.ok ? `ok requests ${fmt(p.metrics?.requests,0)} chat ${fmt(p.metrics?.chat_requests,0)} search ${fmt(p.metrics?.web_search_calls,0)} 5xx ${fmt(p.metrics?.errors_5xx,0)} backend_errors ${fmt(p.metrics?.backend_errors,0)} source_port ${fmt(p.metrics?.source_port,0)}` : esc(p.error)}</div>`).join("");
        const errors = [];
        if (!hw.ok) errors.push(`hardware: ${hw.error}`);
        for (const v of n.vllm || []) if (!v.ok) errors.push(`vLLM ${v.base_url}: ${v.error}`);
        for (const p of n.proxy || []) if (!p.ok) errors.push(`proxy ${p.base_url}: ${p.error}`);
        return `<article class="panel">
          <div class="node-head"><h2>${esc(n.label)}</h2><span class="status ${n.ok ? "ok" : "bad"}">${n.ok ? "healthy/degraded-safe" : "degraded"}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Node hostname or address used by the dashboard for checks and collection.")}>Host</span><span class="mono">${esc(n.host)}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Approximate used memory pressure calculated as 1 - MemAvailable / MemTotal.")}>Memory pressure</span><span>${pct(memUsed)}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("CPU busy and IOwait percentages calculated between dashboard samples from /proc/stat.")}>CPU / IOwait</span><span>busy ${pct((latest.nodes[n.id] || {}).cpu_busy)} | iowait ${pct((latest.nodes[n.id] || {}).cpu_iowait)}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("GPU name, utilization, temperature and power draw from nvidia-smi.")}>GPU</span><span>${esc(hw.value?.gpu?.name || "n/a")} | ${fmt(hw.value?.gpu?.utilization_pct,0)}% | ${fmt(hw.value?.gpu?.temperature_c,0)}C | ${fmt(hw.value?.gpu?.power_w,1)}W</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Linux 1, 5 and 15 minute load averages from /proc/loadavg.")}>Load</span><span class="mono">${esc(hw.value?.loadavg || "n/a")}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Root filesystem capacity and used bytes from df.")}>Root disk</span><span>${bytes(disk.used)} / ${bytes(disk.total)} used</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Per-interface state, IP addresses and cumulative RX/TX counters.")}>Network interfaces</span><span>${network || "n/a"}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("vLLM endpoint health, model metadata, token counters, queues, KV cache, TTFT and errors.")}>vLLM</span><span>${vllm || "n/a"}</span></div>
          <div class="row"><span class="muted metric-tip"${tip("Proxy endpoint health, request counters, web-search counters, 5xx/backend errors and latest source port.")}>Proxy</span><span>${proxy || "n/a"}</span></div>
          ${errors.length ? `<div class="error">${errors.map(esc).join("<br>")}</div>` : ""}
        </article>`;
      }).join("");
      const nodeDefs = nodes.map(n => ({id: n.id, label: n.label}));
      renderChart("throughput", data.history, "generation_tps", 1, nodeDefs);
      renderChart("prompt-throughput", data.history, "prompt_tps", 1, nodeDefs);
      renderChart("total-throughput", data.history, "total_tps", 1, nodeDefs);
      renderChart("kv", data.history, "kv_cache_usage", 1, nodeDefs);
      renderDualAxisMetricChart("context-usage", data.history, nodeDefs, "context_usage_p95", "context_prompt_p95_tokens", {left:"p95 context", right:"p95 tokens", leftMax:1, leftFmt:pct, rightFmt:v=>fmt(v,0)});
      renderChart("gpu-temp", data.history, "gpu_temp_c", 1, nodeDefs);
      renderChart("gpu-power", data.history, "gpu_power_w", 1, nodeDefs);
      renderGpuCombinedChart("gpu-combined", data.history, nodeDefs);
      renderChart("cpu", data.history, "cpu_busy", 1, nodeDefs);
      renderChart("iowait", data.history, "cpu_iowait", 1, nodeDefs);
      renderChart("acceptance", data.history, "acceptance_rate", 1, nodeDefs);
      renderChart("errors", data.history, "error_rate", 1, nodeDefs);
      renderVllmPrefillSourceChart("vllm-prefill-source", data.history, nodeDefs);
      renderDualAxisMetricChart("prefix-efficiency", data.history, nodeDefs, "prefix_hit_rate", "prefill_cache_hit_tps", {left:"hit rate", right:"cache tok/s", leftMax:1, leftFmt:pct, rightFmt:v=>`${fmt(v,0)}/s`});
      renderDualAxisMetricChart("ttft-latency", data.history, nodeDefs, "ttft_p95_s", "ttft_p50_s", {left:"p95", right:"p50", leftFmt:ms, rightFmt:ms});
      renderDualAxisMetricChart("queue-pressure", data.history, nodeDefs, "requests_running", "requests_waiting", {left:"running", right:"waiting", leftFmt:v=>fmt(v,0), rightFmt:v=>fmt(v,0)});
      renderDualAxisMetricChart("prompt-generation", data.history, nodeDefs, "prompt_tps", "generation_tps", {left:"prompt tok/s", right:"gen tok/s", leftFmt:v=>fmt(v,1), rightFmt:v=>fmt(v,1)});
      renderDualAxisMetricChart("memory-kv", data.history, nodeDefs, "memory_pressure", "kv_cache_usage", {left:"memory", right:"KV", leftMax:1, rightMax:1, leftFmt:pct, rightFmt:pct});
      renderDualAxisMetricChart("io-disk", data.history, nodeDefs, "cpu_iowait", "disk_read_bps", {left:"iowait", right:"disk read/s", leftMax:1, leftFmt:pct, rightFmt:v=>`${bytes(v)}/s`});
      renderNetworkInterfaceChart("network-ifaces", data.history, nodeDefs);
      renderMultiMetricChart("proxy-activity", data.history, nodeDefs, [
        {key:"proxy_request_rate", label:"proxy", color:"#60a5fa"},
        {key:"proxy_chat_rate", label:"chat", color:"#36d399"},
        {key:"proxy_web_search_rate", label:"search", color:"#fbbf24"},
        {key:"proxy_5xx_rate", label:"5xx", color:"#fb7185"}
      ], v=>fmt(v,2));
      renderMultiMetricChart("request-outcomes", data.history, nodeDefs, [
        {key:"request_stop_rate", label:"stop", color:"#36d399"},
        {key:"request_length_rate", label:"length", color:"#fbbf24"},
        {key:"error_rate", label:"error", color:"#fb7185"},
        {key:"request_abort_rate", label:"abort", color:"#a78bfa"},
        {key:"request_repetition_rate", label:"repeat", color:"#60a5fa"}
      ], v=>fmt(v,2));
    }
    async function refresh() {
      try {
        const res = await fetch("/api/state", {cache:"no-store"});
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        render(await res.json());
      } catch (err) {
        document.getElementById("updated").textContent = `Dashboard API failed: ${err}`;
        document.getElementById("cards").innerHTML = `<article class="card"><span class="bad">Dashboard API failed</span><strong>Degraded</strong><div class="muted">${esc(err)}</div></article>`;
      }
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, body, status=200, content_type="text/plain; charset=utf-8"):
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def prometheus_metrics(self):
        with LOCK:
            payload = json.loads(json.dumps(STATE))
        latest = payload.get("history", [])[-1] if payload.get("history") else {"nodes": {}}
        lines = [
            "# HELP spark_dashboard_node_up Node has at least one successful collector.",
            "# TYPE spark_dashboard_node_up gauge",
            "# HELP spark_dashboard_generation_tps vLLM generation tokens per second.",
            "# TYPE spark_dashboard_generation_tps gauge",
            "# HELP spark_dashboard_memory_pressure Node memory pressure, 0 to 1.",
            "# TYPE spark_dashboard_memory_pressure gauge",
            "# HELP spark_dashboard_cpu_busy Node CPU busy ratio, 0 to 1.",
            "# TYPE spark_dashboard_cpu_busy gauge",
            "# HELP spark_dashboard_disk_io_bps Disk read plus write bytes per second.",
            "# TYPE spark_dashboard_disk_io_bps gauge",
            "# HELP spark_dashboard_network_bps Network rx plus tx bytes per second.",
            "# TYPE spark_dashboard_network_bps gauge",
            "# HELP spark_dashboard_backend_error_rate Backend request error rate per second.",
            "# TYPE spark_dashboard_backend_error_rate gauge",
            "# HELP spark_dashboard_prompt_tps vLLM prompt tokens per second.",
            "# TYPE spark_dashboard_prompt_tps gauge",
            "# HELP spark_dashboard_total_tps vLLM total tokens per second.",
            "# TYPE spark_dashboard_total_tps gauge",
            "# HELP spark_dashboard_prompt_tokens_total Cumulative vLLM prompt tokens.",
            "# TYPE spark_dashboard_prompt_tokens_total counter",
            "# HELP spark_dashboard_generation_tokens_total Cumulative vLLM generation tokens.",
            "# TYPE spark_dashboard_generation_tokens_total counter",
            "# HELP spark_dashboard_cpu_iowait Node CPU iowait ratio, 0 to 1.",
            "# TYPE spark_dashboard_cpu_iowait gauge",
            "# HELP spark_dashboard_gpu_temperature_c GPU temperature in Celsius.",
            "# TYPE spark_dashboard_gpu_temperature_c gauge",
            "# HELP spark_dashboard_gpu_power_w GPU power draw in watts.",
            "# TYPE spark_dashboard_gpu_power_w gauge",
            "# HELP spark_dashboard_ttft_p95_seconds vLLM p95 time to first token in seconds.",
            "# TYPE spark_dashboard_ttft_p95_seconds gauge",
            "# HELP spark_dashboard_ttft_p50_seconds vLLM p50 time to first token in seconds.",
            "# TYPE spark_dashboard_ttft_p50_seconds gauge",
            "# HELP spark_dashboard_ttft_avg_seconds vLLM average time to first token in seconds.",
            "# TYPE spark_dashboard_ttft_avg_seconds gauge",
        ]
        for node_id, node in (payload.get("nodes") or {}).items():
            sample = (latest.get("nodes") or {}).get(node_id, {})
            labels = f'node="{node_id}"'
            lines.append(f"spark_dashboard_node_up{{{labels}}} {1 if node.get('ok') else 0}")
            lines.append(f"spark_dashboard_generation_tps{{{labels}}} {sample.get('generation_tps') or 0}")
            lines.append(f"spark_dashboard_memory_pressure{{{labels}}} {sample.get('memory_pressure') or 0}")
            lines.append(f"spark_dashboard_cpu_busy{{{labels}}} {sample.get('cpu_busy') or 0}")
            lines.append(f"spark_dashboard_disk_io_bps{{{labels}}} {(sample.get('disk_read_bps') or 0) + (sample.get('disk_write_bps') or 0)}")
            lines.append(f"spark_dashboard_network_bps{{{labels}}} {(sample.get('net_rx_bps') or 0) + (sample.get('net_tx_bps') or 0)}")
            lines.append(f"spark_dashboard_backend_error_rate{{{labels}}} {sample.get('error_rate') or 0}")
            lines.append(f"spark_dashboard_prompt_tps{{{labels}}} {sample.get('prompt_tps') or 0}")
            lines.append(f"spark_dashboard_total_tps{{{labels}}} {sample.get('total_tps') or 0}")
            lines.append(f"spark_dashboard_prompt_tokens_total{{{labels}}} {sample.get('prompt_tokens') or 0}")
            lines.append(f"spark_dashboard_generation_tokens_total{{{labels}}} {sample.get('generation_tokens') or 0}")
            lines.append(f"spark_dashboard_cpu_iowait{{{labels}}} {sample.get('cpu_iowait') or 0}")
            lines.append(f"spark_dashboard_gpu_temperature_c{{{labels}}} {sample.get('gpu_temp_c') or 0}")
            lines.append(f"spark_dashboard_gpu_power_w{{{labels}}} {sample.get('gpu_power_w') or 0}")
            lines.append(f"spark_dashboard_ttft_p95_seconds{{{labels}}} {sample.get('ttft_p95_s') or 0}")
            lines.append(f"spark_dashboard_ttft_p50_seconds{{{labels}}} {sample.get('ttft_p50_s') or 0}")
            lines.append(f"spark_dashboard_ttft_avg_seconds{{{labels}}} {sample.get('ttft_avg_s') or 0}")
        return "\n".join(lines) + "\n"

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index.html"):
            body = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if (
            self.path.startswith("/api/state")
            or self.path.startswith("/api/snapshot")
            or self.path.startswith("/api/metrics")
        ):
            with LOCK:
                payload = json.loads(json.dumps(STATE))
            self.send_json(payload)
            return
        if self.path.startswith("/metrics"):
            self.send_text(self.prometheus_metrics())
            return
        if self.path.startswith("/health"):
            self.send_json({"ok": True, "updated_at": STATE.get("updated_at")})
            return
        self.send_json({"ok": False, "error": "not found"}, status=404)


def main():
    restored = load_history()
    if restored:
        with LOCK:
            STATE["history"] = restored
            STATE["updated_at"] = restored[-1].get("t")
        compact_history_file(restored)
        print(f"Loaded {len(restored)} history samples from {HISTORY_FILE}", flush=True)
    threading.Thread(target=collector, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Spark dashboard listening on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
