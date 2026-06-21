#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.integration.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" build

docker compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from integration-tests
