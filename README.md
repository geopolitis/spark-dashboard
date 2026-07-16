# Spark Dashboard

Failure-tolerant dashboard for a two-node NVIDIA Spark/vLLM deployment.

The dashboard monitors local and remote services without assuming specific hostnames. The Python file contains safe defaults; `.env` should only contain values you want to override.

## Files

- `spark_dashboard_resilient.py` - single-file dashboard server.
- `start_spark_dashboard_resilient.sh` - restart helper for the dashboard host.
- `deploy.sh` - copies the dashboard to a configured host and restarts it.
- `.env` - active local configuration.
- `.env.example` - generic configuration template.
- `requirements.txt` - Python dependency file. The dashboard currently uses only the standard library.

## Configuration

Copy the template and edit only the values that differ from the defaults:

```bash
cp .env.example .env
```

Common overrides:

```bash
REMOTE_NODE_HOST=remote-host
REMOTE_NODE_SSH=remote-host
REMOTE_NODE_LABEL="Remote node"
```

Useful optional overrides:

```bash
SPARK_DASHBOARD_PORT=8090
SPARK_DASHBOARD_INTERVAL=15
SPARK_DASHBOARD_HISTORY=86400

LOCAL_NODE_LABEL="Local node"
LOCAL_VLLM_URL=http://127.0.0.1:8080
LOCAL_PROXY_URL=http://127.0.0.1:8081

REMOTE_VLLM_URL=http://remote-host:8080
REMOTE_PROXY_URL=http://remote-host:8081

DASHBOARD_DEPLOY_HOST=dashboard-host
DASHBOARD_DEPLOY_DIR=/home/user/dashboard
```

When `REMOTE_VLLM_URL` or `REMOTE_PROXY_URL` are omitted, they are derived from `REMOTE_NODE_HOST` using ports `8080` and `8081`.

## Run

On the dashboard host:

```bash
cd /path/to/dashboard
DASHBOARD_HOME="$PWD" ./start_spark_dashboard_resilient.sh
```

The start script loads `$DASHBOARD_HOME/.env` when present.

## Deploy

From this repository:

```bash
DASHBOARD_DEPLOY_HOST=dashboard-host \
DASHBOARD_DEPLOY_DIR=/home/user/dashboard \
./deploy.sh
```

## Health Checks

```bash
curl http://dashboard-host:8090/health
curl http://dashboard-host:8090/api/state
curl http://dashboard-host:8090/metrics
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
