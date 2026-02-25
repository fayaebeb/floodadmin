#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-${WEBSITES_PORT:-8000}}"
exec python -m hypercorn app:app --bind "0.0.0.0:${PORT}" --access-logfile -
