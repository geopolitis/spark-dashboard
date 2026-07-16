#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_HOST="${DASHBOARD_DEPLOY_HOST:-dashboard-host}"
TARGET_DIR="${DASHBOARD_DEPLOY_DIR:-/home/geo/Gemini}"

scp "$DIR/spark_dashboard_resilient.py" "$TARGET_HOST:$TARGET_DIR/spark_dashboard_resilient.py"
scp "$DIR/start_spark_dashboard_resilient.sh" "$TARGET_HOST:$TARGET_DIR/start_spark_dashboard_resilient.sh"
scp "$DIR/.env" "$TARGET_HOST:$TARGET_DIR/.env"

ssh -A "$TARGET_HOST" "chmod +x '$TARGET_DIR/spark_dashboard_resilient.py' '$TARGET_DIR/start_spark_dashboard_resilient.sh' && DASHBOARD_HOME='$TARGET_DIR' '$TARGET_DIR/start_spark_dashboard_resilient.sh'"
