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


def now_ms():
    return int(time.time() * 1000)


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
    ttft = parse_histogram(text, "vllm:time_to_first_token_seconds")
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
        "ttft_count": ttft.get("count") if ttft else None,
        "ttft_avg_s": (ttft.get("sum") / ttft.get("count")) if ttft and ttft.get("sum") is not None and ttft.get("count") else None,
        "ttft_p50_s": histogram_quantile(ttft, 0.50),
        "ttft_p95_s": histogram_quantile(ttft, 0.95),
    }


def collect_vllm(node):
    results = []
    for base in node.get("vllm", []):
        item = {"base_url": base, "models": None, "metrics": None, "ok": False, "error": None}
        try:
            _, _, body = http_get(f"{base}/v1/models", timeout=2)
            models = json.loads(body.decode("utf-8"))
            item["models"] = models.get("data", [])
            item["ok"] = True
        except Exception as exc:
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
        ttft_count = 0.0
        ttft_sum = 0.0
        ttft_p50 = None
        ttft_p95 = None
        for item in node.get("vllm", []):
            metrics = item.get("metrics") or {}
            generation += metrics.get("generation_tokens") or 0.0
            prompt += metrics.get("prompt_tokens") or 0.0
            request_success += metrics.get("request_success") or 0.0
            prefix_hits += metrics.get("prefix_cache_hits") or 0.0
            prefix_queries += metrics.get("prefix_cache_queries") or 0.0
            preemptions += metrics.get("preemptions") or 0.0
            estimated_read_bytes += metrics.get("estimated_read_bytes") or 0.0
            estimated_write_bytes += metrics.get("estimated_write_bytes") or 0.0
            estimated_flops += metrics.get("estimated_flops") or 0.0
            running += metrics.get("requests_running") or 0.0
            waiting += metrics.get("requests_waiting") or 0.0
            errors += metrics.get("request_errors") or 0.0
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
        flops_per_sec = None
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
            flops_per_sec = max((estimated_flops - prev.get("estimated_flops", estimated_flops)) / elapsed, 0.0)
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
            "ttft_avg_s": ttft_sum / ttft_count if ttft_count else None,
            "ttft_p50_s": ttft_p50,
            "ttft_p95_s": ttft_p95,
        }
    return sample


def collector():
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
            cutoff = now_ms() - HISTORY_SECONDS * 1000
            STATE["history"] = [item for item in STATE["history"] if item["t"] >= cutoff]
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
    .tile[title], .metric-tip[title] { cursor: help; }
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
    .chart text { fill: var(--muted); font-size: 10px; }
    .chart path, .chart line { vector-effect: non-scaling-stroke; }
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
    <section class="grid">
      <div class="panel"><h2 class="metric-tip" title="Output-token generation rate over the retained history window.">Generation TPS - 24h</h2><svg class="chart" id="throughput"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Prompt/input token processing rate over the retained history window.">Prompt TPS - 24h</h2><svg class="chart" id="prompt-throughput"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Combined prompt plus generation token throughput.">Total Token TPS - 24h</h2><svg class="chart" id="total-throughput"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="vLLM KV cache occupancy percentage. High values can increase queueing or preemptions.">KV Cache - 24h</h2><svg class="chart" id="kv"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="GPU core temperature in Celsius from nvidia-smi.">GPU Temperature - 24h</h2><svg class="chart" id="gpu-temp"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="GPU power draw in watts from nvidia-smi.">GPU Power - 24h</h2><svg class="chart" id="gpu-power"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Combined chart with temperature, power and GPU utilization; dashed line is the peer node.">GPU Temp / Power / Util - 24h</h2><svg class="chart large" id="gpu-combined"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="CPU non-idle time calculated from /proc/stat deltas.">CPU Busy - 24h</h2><svg class="chart" id="cpu"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="CPU time waiting on disk or device I/O from /proc/stat deltas.">IOwait - 24h</h2><svg class="chart" id="iowait"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Speculative decoding acceptance ratio from vLLM draft and accepted token counters, when exported.">DFlash Acceptance - 24h</h2><svg class="chart" id="acceptance"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="vLLM request error rate calculated from request_success_total with finished_reason=error.">Error Rate - 24h</h2><svg class="chart" id="errors"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Estimated vLLM read bandwidth per second derived from vLLM counters.">vLLM Read Bytes/s - 24h</h2><svg class="chart" id="vllm-read"></svg></div>
      <div class="panel"><h2 class="metric-tip" title="Estimated vLLM write bandwidth per second derived from vLLM counters.">vLLM Write Bytes/s - 24h</h2><svg class="chart" id="vllm-write"></svg></div>
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
    function tip(s) { return ` title="${esc(s)}"`; }
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
            <div class="tile"${tip("Currently served vLLM model and key launch parameters parsed from /v1/models and the vLLM serve process.")}><span>Model</span><b>${esc(shortModelName(model.id || runtime.served_model_name || runtime.model_path))}</b><small>ctx ${fmt(contextLen,0)} | seqs ${esc(runtime.max_num_seqs || "n/a")} | batch ${esc(runtime.max_num_batched_tokens || "n/a")}</small></div>
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
            <div class="small">model ${esc(model.id || "n/a")} ctx ${fmt(model.max_model_len,0)}</div>
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
      renderChart("gpu-temp", data.history, "gpu_temp_c", 1, nodeDefs);
      renderChart("gpu-power", data.history, "gpu_power_w", 1, nodeDefs);
      renderGpuCombinedChart("gpu-combined", data.history, nodeDefs);
      renderChart("cpu", data.history, "cpu_busy", 1, nodeDefs);
      renderChart("iowait", data.history, "cpu_iowait", 1, nodeDefs);
      renderChart("acceptance", data.history, "acceptance_rate", 1, nodeDefs);
      renderChart("errors", data.history, "error_rate", 1, nodeDefs);
      renderChart("vllm-read", data.history, "vllm_read_bps", 1, nodeDefs);
      renderChart("vllm-write", data.history, "vllm_write_bps", 1, nodeDefs);
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
    threading.Thread(target=collector, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Spark dashboard listening on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
