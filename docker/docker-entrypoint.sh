#!/usr/bin/env sh
set -e

# Simple entrypoint that can optionally run the pipeline before starting API
if [ "${RUN_PIPELINE:-0}" = "1" ]; then
  python -m market-data.scripts.run_mvp || true
fi

exec "$@"



