# Spark Dashboard

Failure-tolerant dashboard for the Toula/Koula NVIDIA Spark nodes.

## Files

- `spark_dashboard_resilient.py` - single-file dashboard server.
- `start_spark_dashboard_resilient.sh` - restart helper used on Toula.
- `deploy_to_toula.sh` - copies these files to `/home/geo/Gemini` on Toula and restarts the dashboard.

## Current Ports

- Dashboard: `http://toula:8090/`
- Toula vLLM: `http://127.0.0.1:8080`
- Toula proxy: `http://127.0.0.1:8081`
- Koula vLLM: `http://10.10.10.2:8080`
- Koula proxy: `http://10.10.10.2:8081`

## Run Locally On Toula

```bash
cd /home/geo/Gemini
./start_spark_dashboard_resilient.sh
```

## Deploy From This Folder

```bash
./deploy_to_toula.sh
```

## Health Checks

```bash
curl http://toula:8090/health
curl http://toula:8090/api/state
curl http://toula:8090/metrics
```

## Metrics Covered

- Node status and backend health.
- CPU busy and IOwait.
- Memory pressure.
- Disk I/O and network throughput.
- Per-interface IP addresses.
- GPU temperature, power, and utilization.
- vLLM prompt/generation/total token counters and rates.
- vLLM queue, KV cache, prefix cache, request errors, and preemptions.
- Proxy request, chat, search, 5xx, backend error, and source-port stats.
