#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
MVNW="${REPO_ROOT}/mvnw"
RUNTIME_DIR="${REPO_ROOT}/.local-runtime"
LOG_DIR="${RUNTIME_DIR}/logs"
PID_DIR="${RUNTIME_DIR}/pids"

AGENT_LOG="${LOG_DIR}/agent.log"
APP1_LOG="${LOG_DIR}/app-1.log"
APP2_LOG="${LOG_DIR}/app-2.log"

AGENT_PID_FILE="${PID_DIR}/agent.pid"
APP1_PID_FILE="${PID_DIR}/app-1.pid"
APP2_PID_FILE="${PID_DIR}/app-2.pid"

usage() {
  cat <<'USAGE'
Usage: ./local-setup.sh <start|stop|restart|status|logs>

Commands:
  start    Build and start 1 can-cache-agent + 2 can-cache-application instances.
  stop     Stop all started local processes.
  restart  Stop then start.
  status   Show whether processes are running.
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
  "${MVNW}" -q -f "${REPO_ROOT}/can-cache-agent/pom.xml" package -DskipTests

  echo "[build] packaging can-cache-application"
  "${MVNW}" -q -f "${REPO_ROOT}/can-cache-application/pom.xml" package -DskipTests
}

start_all() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  build_apps

  start_process "agent" "${AGENT_PID_FILE}" "${AGENT_LOG}" \
    java \
    -Dagent.listen.port=11211 \
    -Dagent.registration.port=11311 \
    -Dagent.dashboard.mode=off \
    -jar "${REPO_ROOT}/can-cache-agent/target/quarkus-app/quarkus-run.jar"

  start_process "app-1" "${APP1_PID_FILE}" "${APP1_LOG}" \
    java \
    -Dquarkus.http.port=8081 \
    -Dapp.network.port=11212 \
    -Dapp.cluster.replication.port=18081 \
    -Dapp.cluster.discovery.node-id=node-1 \
    -Dapp.rdb.path=data-node-1.rdb \
    -Dapp.agent.enabled=true \
    -Dapp.agent.host=127.0.0.1 \
    -Dapp.agent.port=11211 \
    -Dapp.agent.registration-port=11311 \
    -Dapp.agent.advertised-host=127.0.0.1 \
    -jar "${REPO_ROOT}/can-cache-application/target/quarkus-app/quarkus-run.jar"

  start_process "app-2" "${APP2_PID_FILE}" "${APP2_LOG}" \
    java \
    -Dquarkus.http.port=8082 \
    -Dapp.network.port=11213 \
    -Dapp.cluster.replication.port=18082 \
    -Dapp.cluster.discovery.node-id=node-2 \
    -Dapp.rdb.path=data-node-2.rdb \
    -Dapp.agent.enabled=true \
    -Dapp.agent.host=127.0.0.1 \
    -Dapp.agent.port=11211 \
    -Dapp.agent.registration-port=11311 \
    -Dapp.agent.advertised-host=127.0.0.1 \
    -jar "${REPO_ROOT}/can-cache-application/target/quarkus-app/quarkus-run.jar"

  echo
  echo "Local stack ready:"
  echo "  Agent endpoint: 127.0.0.1:11211"
  echo "  Cache nodes:    127.0.0.1:11212, 127.0.0.1:11213"
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
