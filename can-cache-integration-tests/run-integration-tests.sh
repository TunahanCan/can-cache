#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_COUNT="${APP_COUNT:-2}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
COMPOSE_FILE="$(mktemp "${TMPDIR:-/tmp}/can-cache-integration-${APP_COUNT}.XXXXXX.yml")"

case "${APP_COUNT}" in
  2|4|8|16) ;;
  *)
    echo "APP_COUNT must be one of: 2, 4, 8, 16 (got ${APP_COUNT})" >&2
    exit 1
    ;;
esac

generate_compose_file() {
  local file="$1"
  local count="$2"

  cat > "${file}" <<YAML
name: can-cache-integration-tests

services:
  can-cache-agent:
    build:
      context: ${REPO_ROOT}
      dockerfile: Dockerfile.agent
    image: can-cache-agent:integration
    environment:
      AGENT_LISTEN_HOST: 0.0.0.0
      AGENT_LISTEN_PORT: "11211"
      AGENT_REGISTRATION_ENABLED: "true"
      AGENT_REGISTRATION_HOST: 0.0.0.0
      AGENT_REGISTRATION_PORT: "11311"
      AGENT_DISCOVERY_ENABLED: "false"
      AGENT_DASHBOARD_MODE: "off"
      QUARKUS_LOG_LEVEL: INFO

YAML

  for index in $(seq 1 "${count}"); do
    cat >> "${file}" <<YAML
  can-cache-app-${index}:
    build:
      context: ${REPO_ROOT}
      dockerfile: Dockerfile
    image: can-cache-app:integration
    environment:
      QUARKUS_HTTP_PORT: "$((9000 + index))"
      APP_NETWORK_HOST: 0.0.0.0
      APP_NETWORK_PORT: "11212"
      APP_AGENT_ENABLED: "true"
      APP_AGENT_HOST: can-cache-agent
      APP_AGENT_PORT: "11211"
      APP_AGENT_REGISTRATION_PORT: "11311"
      APP_AGENT_ADVERTISED_HOST: can-cache-app-${index}
      APP_CLUSTER_DISCOVERY_NODE_ID: can-cache-app-${index}
      APP_CLUSTER_DISCOVERY_HEARTBEAT_INTERVAL_MILLIS: "1000"
      APP_CLUSTER_DISCOVERY_FAILURE_TIMEOUT_MILLIS: "10000"
      APP_CLUSTER_REPLICATION_ADVERTISE_HOST: can-cache-app-${index}
      APP_CLUSTER_REPLICATION_PORT: "18080"
      APP_CLUSTER_COORDINATION_ANTI_ENTROPY_INTERVAL_MILLIS: "1000"
    depends_on:
      - can-cache-agent

YAML
  done

  cat >> "${file}" <<YAML
  integration-tests:
    build:
      context: ${SCRIPT_DIR}
    depends_on:
      - can-cache-agent
YAML

  for index in $(seq 1 "${count}"); do
    echo "      - can-cache-app-${index}" >> "${file}"
  done

  cat >> "${file}" <<YAML
    environment:
      CAN_CACHE_HOST: can-cache-agent
      CAN_CACHE_PORT: "11211"
      CAN_CACHE_AGENT_HOST: can-cache-agent
      CAN_CACHE_AGENT_HTTP_PORT: "8080"
      CAN_CACHE_AGENT_STATUS_PATH: "/agent/instances"
      CAN_CACHE_METRICS_HOST: can-cache-app-1
      CAN_CACHE_METRICS_PORT: "9001"
      CAN_CACHE_METRICS_PATH: "/metrics"
      CAN_CACHE_METRICS_CACHE_HOST: can-cache-app-1
      CAN_CACHE_METRICS_CACHE_PORT: "11212"
      CAN_CACHE_APP_COUNT: "${count}"
      CAN_CACHE_WAIT_TIMEOUT_SECONDS: "${WAIT_TIMEOUT_SECONDS}"
YAML

  for index in $(seq 1 "${count}"); do
    cat >> "${file}" <<YAML
      CAN_CACHE_APP_${index}_HOST: can-cache-app-${index}
      CAN_CACHE_APP_${index}_PORT: "11212"
      CAN_CACHE_APP_${index}_REPLICATION_PORT: "18080"
YAML
  done

  cat >> "${file}" <<YAML
      CAN_CACHE_APP1_HOST: can-cache-app-1
      CAN_CACHE_APP1_PORT: "11212"
      CAN_CACHE_APP1_REPLICATION_PORT: "18080"
      CAN_CACHE_APP2_HOST: can-cache-app-2
      CAN_CACHE_APP2_PORT: "11212"
      CAN_CACHE_APP2_REPLICATION_PORT: "18080"
    command: ["mvn", "-B", "test"]
YAML
}

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down --remove-orphans -v >/dev/null 2>&1 || true
  rm -f "${COMPOSE_FILE}"
}

trap cleanup EXIT

generate_compose_file "${COMPOSE_FILE}" "${APP_COUNT}"

docker compose -f "${COMPOSE_FILE}" build

docker compose -f "${COMPOSE_FILE}" up --abort-on-container-exit --exit-code-from integration-tests
