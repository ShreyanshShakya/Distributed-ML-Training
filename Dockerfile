# --------------------------------------------------------------
#  DMLF – one image, three possible entry‑points
# --------------------------------------------------------------
FROM python:3.11-slim AS base

# ---- system deps (only what is needed for building wheels) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
        && rm -rf /var/lib/apt/lists/*

# ---- Python dependencies ---------------------------------------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Application code -----------------------------------------
COPY . .

# --------------------------------------------------------------
#  Helper script – chooses the role at container start‑up
# --------------------------------------------------------------
COPY <<'EOF' /app/entrypoint.sh
#!/usr/bin/env bash
set -euo pipefail

# The role can be supplied via the ENV VAR `DMLF_ROLE`
#   manager   – starts the gRPC + HTTP health server
#   agent     – registers with the manager and waits for jobs
#   cli       – runs a single `dmlf.cli submit …` command and exits
# If the variable is not set we fall back to the first argument
ROLE="${DMLF_ROLE:-${1:-}}"

case "$ROLE" in
  manager)
    echo "=== Starting DMLF Cluster Manager ==="
    exec python -m dmlf.manager.cluster_manager --config /app/config.yaml
    ;;
  agent)
    echo "=== Starting DMLF Node Agent ==="
    exec python -m dmlf.agent.agent --config /app/config.yaml
    ;;
  cli)
    # The remaining arguments are passed straight to the CLI.
    # Example: docker run … cli submit /app/dmlf/configs/resnet.yaml
    shift   # drop the "cli" token
    echo "=== Running DMLF CLI: $* ==="
    exec python -m dmlf.cli --config /app/config.yaml "$@"
    ;;
  *)
    echo "ERROR: Unknown role '$ROLE'. Use one of: manager | agent | cli"
    exit 1
    ;;
esac
EOF

RUN chmod +x /app/entrypoint.sh

# --------------------------------------------------------------
#  Health‑check – only meaningful for the manager role
# --------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# --------------------------------------------------------------
#  Default – show usage if the container is started without args
# --------------------------------------------------------------
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--help"]