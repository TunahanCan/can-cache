#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.integration.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" build

docker compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from integration-tests
