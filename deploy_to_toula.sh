#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

scp "$DIR/spark_dashboard_resilient.py" toula:/home/geo/Gemini/spark_dashboard_resilient.py
scp "$DIR/start_spark_dashboard_resilient.sh" toula:/home/geo/Gemini/start_spark_dashboard_resilient.sh

ssh -A toula 'chmod +x /home/geo/Gemini/spark_dashboard_resilient.py /home/geo/Gemini/start_spark_dashboard_resilient.sh && /home/geo/Gemini/start_spark_dashboard_resilient.sh'
