# Spark Dashboard

Failure-tolerant dashboard for a two-node NVIDIA Spark/vLLM deployment.

The dashboard monitors local and remote services without assuming specific hostnames. Configure node labels, SSH targets, vLLM endpoints, proxy endpoints, and sampling windows in `.env`.

## Files

- `spark_dashboard_resilient.py` - single-file dashboard server.
- `start_spark_dashboard_resilient.sh` - restart helper for the dashboard host.
- `deploy.sh` - copies the dashboard to a configured host and restarts it.
- `.env` - active local configuration.
- `.env.example` - generic configuration template.
- `requirements.txt` - Python dependency file. The dashboard currently uses only the standard library.

## Configuration

Copy the template and edit it for your environment:

```bash
cp .env.example .env
```

Key variables:

```bash
SPARK_DASHBOARD_PORT=8090
SPARK_DASHBOARD_INTERVAL=15
SPARK_DASHBOARD_HISTORY=21600

LOCAL_NODE_ID=local
LOCAL_NODE_LABEL=Local node
LOCAL_NODE_HOST=127.0.0.1
LOCAL_NODE_SSH=
LOCAL_VLLM_URL=http://127.0.0.1:8080
LOCAL_PROXY_URL=http://127.0.0.1:8081

REMOTE_NODE_ID=remote
REMOTE_NODE_LABEL=Remote node
REMOTE_NODE_HOST=remote-host
REMOTE_NODE_SSH=remote-host
REMOTE_VLLM_URL=http://remote-host:8080
REMOTE_PROXY_URL=http://remote-host:8081
```

Deploy helper variables:

```bash
DASHBOARD_DEPLOY_HOST=dashboard-host
DASHBOARD_DEPLOY_DIR=/home/user/dashboard
```

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
