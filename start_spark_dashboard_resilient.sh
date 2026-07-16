#!/usr/bin/env bash
set -euo pipefail

DASHBOARD_HOME="${DASHBOARD_HOME:-/home/geo/Gemini}"
cd "$DASHBOARD_HOME"

if [[ -f "$DASHBOARD_HOME/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$DASHBOARD_HOME/.env"
  set +a
fi

ps -eo pid=,args= |
  awk -v script="$DASHBOARD_HOME/spark_dashboard_resilient.py" '$2 == "python3" && $3 == script {print $1}' |
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done

ps -eo pid=,args= |
  awk -v forward="$DASHBOARD_HOME/tcp_forward.py" '$2 == "python3" && $3 == forward && $0 ~ /--listen-port 8090/ {print $1}' |
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done

nohup python3 "$DASHBOARD_HOME/spark_dashboard_resilient.py" > "$DASHBOARD_HOME/spark_dashboard_resilient.out" 2>&1 &
echo "dashboard pid=$!"
