#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
GRADLEW="${REPO_ROOT}/gradlew"
RUNTIME_DIR="${REPO_ROOT}/.local-runtime"
LOG_DIR="${RUNTIME_DIR}/logs"
PID_DIR="${RUNTIME_DIR}/pids"

AGENT_LOG="${LOG_DIR}/agent.log"
APP1_LOG="${LOG_DIR}/app-1.log"
APP2_LOG="${LOG_DIR}/app-2.log"

AGENT_PID_FILE="${PID_DIR}/agent.pid"
APP1_PID_FILE="${PID_DIR}/app-1.pid"
APP2_PID_FILE="${PID_DIR}/app-2.pid"

AGENT_HOST="127.0.0.1"
AGENT_PROXY_PORT="11211"
AGENT_HTTP_PORT="8080"
AGENT_REGISTRATION_PORT="11311"
APP1_PORT="11212"
APP2_PORT="11213"
APP1_HTTP_PORT="8081"
APP2_HTTP_PORT="8082"
APP1_REPLICATION_PORT="18081"
APP2_REPLICATION_PORT="18082"

usage() {
  cat <<'USAGE'
Usage: ./local-setup.sh <start|stop|restart|status|test|logs>

Commands:
  start    Build and start 1 can-cache-agent + 2 can-cache-application instances.
  stop     Stop all started local processes.
  restart  Stop then start.
  status   Show whether processes are running.
  test     Verify the running stack through the agent proxy.
  logs     Tail all logs.
USAGE
}

is_running() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1

  local pid
  pid="$(cat "${pid_file}")"
  [[ -n "${pid}" ]] || return 1

  kill -0 "${pid}" 2>/dev/null
}

start_process() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  shift 3

  if is_running "${pid_file}"; then
    echo "[${name}] already running (pid=$(cat "${pid_file}"))"
    return 0
  fi

  echo "[${name}] starting..."
  nohup "$@" >"${log_file}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pid_file}"
  sleep 1

  if is_running "${pid_file}"; then
    echo "[${name}] started (pid=${pid})"
  else
    echo "[${name}] failed to start, check ${log_file}" >&2
    return 1
  fi
}

stop_process() {
  local name="$1"
  local pid_file="$2"

  if ! is_running "${pid_file}"; then
    rm -f "${pid_file}"
    echo "[${name}] not running"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}")"
  echo "[${name}] stopping pid=${pid}"
  kill "${pid}" 2>/dev/null || true

  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 0.5
  done

  if kill -0 "${pid}" 2>/dev/null; then
    echo "[${name}] force killing pid=${pid}"
    kill -9 "${pid}" 2>/dev/null || true
  fi

  rm -f "${pid_file}"
  echo "[${name}] stopped"
}

build_apps() {
  echo "[build] packaging can-cache-agent"
  "${GRADLEW}" -q :can-cache-agent:clean :can-cache-agent:build -x test

  echo "[build] packaging can-cache-application"
  "${GRADLEW}" -q :can-cache-application:clean :can-cache-application:build -x test
}

wait_for_port() {
  local name="$1"
  local host="$2"
  local port="$3"
  local attempts="${4:-60}"

  echo "[wait] ${name} ${host}:${port}"
  for _ in $(seq 1 "${attempts}"); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      echo "[wait] ${name} is reachable"
      return 0
    fi
    sleep 0.5
  done

  echo "[wait] ${name} did not become reachable at ${host}:${port}" >&2
  return 1
}

agent_status_json() {
  curl -fsS "http://${AGENT_HOST}:${AGENT_HTTP_PORT}/agent/instances"
}

wait_for_agent_health() {
  echo "[wait] agent sees two healthy cache nodes"
  for _ in $(seq 1 80); do
    local body
    body="$(agent_status_json 2>/dev/null || true)"
    if [[ "${body}" == *'"totalInstances":2'* && "${body}" == *'"healthyInstances":2'* ]]; then
      echo "[wait] agent health is ready"
      return 0
    fi
    sleep 0.5
  done

  echo "[wait] agent did not report two healthy nodes" >&2
  agent_status_json >&2 || true
  return 1
}

agent_request() {
  local payload="$1"
  PAYLOAD="${payload}" python3 - "${AGENT_HOST}" "${AGENT_PROXY_PORT}" <<'PY'
import os
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
payload = os.environ["PAYLOAD"].encode()

with socket.create_connection((host, port), timeout=3) as sock:
    sock.settimeout(3)
    sock.sendall(payload)
    chunks = []
    while True:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            break
        chunks.append(chunk)

sys.stdout.buffer.write(b"".join(chunks))
PY
}

test_stack() {
  wait_for_port "agent proxy" "${AGENT_HOST}" "${AGENT_PROXY_PORT}" 20
  wait_for_agent_health

  local response
  response="$(agent_request $'flush_all\r\nset agent:smoke 0 60 2\r\nok\r\nget agent:smoke\r\nquit\r\n')"
  if [[ "${response}" != *"STORED"* || "${response}" != *"VALUE agent:smoke 0 2"* || "${response}" != *"ok"* ]]; then
    echo "[test] smoke test failed" >&2
    printf "%s\n" "${response}" >&2
    return 1
  fi

  echo "[test] agent proxy smoke test passed"
}

start_all() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  build_apps

  start_process "agent" "${AGENT_PID_FILE}" "${AGENT_LOG}" \
    java \
    -Dagent.listen.port="${AGENT_PROXY_PORT}" \
    -Dagent.registration.port="${AGENT_REGISTRATION_PORT}" \
    -Dagent.discovery.enabled=false \
    -Dagent.dashboard.mode=off \
    -jar "${REPO_ROOT}/can-cache-agent/build/quarkus-app/quarkus-run.jar"

  wait_for_port "agent registration" "${AGENT_HOST}" "${AGENT_REGISTRATION_PORT}" 60

  start_process "app-1" "${APP1_PID_FILE}" "${APP1_LOG}" \
    java \
    -Dquarkus.http.port="${APP1_HTTP_PORT}" \
    -Dapp.network.port="${APP1_PORT}" \
    -Dapp.cluster.replication.port="${APP1_REPLICATION_PORT}" \
    -Dapp.cluster.discovery.node-id=node-1 \
    -Dapp.agent.enabled=true \
    -Dapp.agent.host="${AGENT_HOST}" \
    -Dapp.agent.port="${AGENT_PROXY_PORT}" \
    -Dapp.agent.registration-port="${AGENT_REGISTRATION_PORT}" \
    -Dapp.agent.advertised-host="${AGENT_HOST}" \
    -jar "${REPO_ROOT}/can-cache-application/build/quarkus-app/quarkus-run.jar"

  start_process "app-2" "${APP2_PID_FILE}" "${APP2_LOG}" \
    java \
    -Dquarkus.http.port="${APP2_HTTP_PORT}" \
    -Dapp.network.port="${APP2_PORT}" \
    -Dapp.cluster.replication.port="${APP2_REPLICATION_PORT}" \
    -Dapp.cluster.discovery.node-id=node-2 \
    -Dapp.agent.enabled=true \
    -Dapp.agent.host="${AGENT_HOST}" \
    -Dapp.agent.port="${AGENT_PROXY_PORT}" \
    -Dapp.agent.registration-port="${AGENT_REGISTRATION_PORT}" \
    -Dapp.agent.advertised-host="${AGENT_HOST}" \
    -jar "${REPO_ROOT}/can-cache-application/build/quarkus-app/quarkus-run.jar"

  wait_for_port "app-1 cache" "${AGENT_HOST}" "${APP1_PORT}" 60
  wait_for_port "app-2 cache" "${AGENT_HOST}" "${APP2_PORT}" 60
  test_stack

  echo
  echo "Local stack ready:"
  echo "  Agent endpoint: ${AGENT_HOST}:${AGENT_PROXY_PORT}"
  echo "  Cache nodes:    ${AGENT_HOST}:${APP1_PORT}, ${AGENT_HOST}:${APP2_PORT}"
  echo "  Logs:           ${LOG_DIR}"
}

status_all() {
  for entry in \
    "agent:${AGENT_PID_FILE}" \
    "app-1:${APP1_PID_FILE}" \
    "app-2:${APP2_PID_FILE}"; do
    local name="${entry%%:*}"
    local pid_file="${entry##*:}"
    if is_running "${pid_file}"; then
      echo "[${name}] running (pid=$(cat "${pid_file}"))"
    else
      echo "[${name}] stopped"
    fi
  done
}

stop_all() {
  stop_process "app-2" "${APP2_PID_FILE}"
  stop_process "app-1" "${APP1_PID_FILE}"
  stop_process "agent" "${AGENT_PID_FILE}"
}

logs_all() {
  mkdir -p "${LOG_DIR}"
  echo "Tailing logs from ${LOG_DIR}"
  tail -n 100 -F "${AGENT_LOG}" "${APP1_LOG}" "${APP2_LOG}"
}

main() {
  local command="${1:-}"

  case "${command}" in
    start)
      start_all
      ;;
    stop)
      stop_all
      ;;
    restart)
      stop_all
      start_all
      ;;
    status)
      status_all
      ;;
    test)
      test_stack
      ;;
    logs)
      logs_all
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
