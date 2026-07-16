#!/usr/bin/env bash
set -euo pipefail

cd /home/geo/Gemini

if [[ -f /home/geo/Gemini/.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /home/geo/Gemini/.env
  set +a
fi

ps -eo pid=,args= |
  awk '$2 == "python3" && $3 == "/home/geo/Gemini/spark_dashboard_resilient.py" {print $1}' |
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done

ps -eo pid=,args= |
  awk '$2 == "python3" && $3 == "/home/geo/Gemini/tcp_forward.py" && $0 ~ /--listen-port 8090/ {print $1}' |
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done

nohup python3 /home/geo/Gemini/spark_dashboard_resilient.py > /home/geo/Gemini/spark_dashboard_resilient.out 2>&1 &
echo "dashboard pid=$!"
