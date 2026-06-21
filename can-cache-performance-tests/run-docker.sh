#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-docker.sh [PROFILE] [-- JMETER_ARGS...]

Builds the custom JMeter Java sampler, starts one can-cache-agent plus the
requested number of can-cache-application containers, waits until every app is
registered and healthy, validates cross-connection data transfer, then runs the
selected JMeter plan in Docker.

Profiles:
  small   Lightweight smoke workload (default)
  medium  Steady mid-tier workload
  large   High concurrency workload
  xl      Saturation-level workload

Environment overrides:
  TARGET_HOST            Target inside Docker network (default: can-cache-agent)
  TARGET_PORT            Target TCP port (default: 11211)
  APP_COUNT              Number of cache apps behind one agent: 2, 4, or 8 (default: 2)
  CONNECTION_MODE        single or separate sampler connections (default: separate)
  REPLICATION_FACTOR     Cluster replication factor for apps (default: 3)
  ANTI_ENTROPY_INTERVAL_MILLIS App anti-entropy interval (default: 30000)
  TTL_SECONDS            TTL in seconds for generated SET commands
  CONNECT_TIMEOUT_MILLIS Socket connect timeout (ms)
  READ_TIMEOUT_MILLIS    Socket read timeout (ms)
  KEY_PREFIX             Prefix for generated cache keys
  PAYLOAD_SIZE           Payload size in bytes
  DURATION_SECONDS       Thread group duration override in seconds
  RESULT_FILE            Result path under the repository
  JMETER_IMAGE           JMeter image (default: anasoid/jmeter:5.6.3-plugins-21-jre)
  JMETER_HEAP            JMeter JVM heap (default: -Xms64m -Xmx256m -XX:MaxMetaspaceSize=128m)
  MVN_IMAGE              Maven/JDK image for sampler build
  WAIT_TIMEOUT_SECONDS   Stack/data-transfer wait timeout (default: 120)
  ALLOW_JMETER_ERRORS    Set to 1 to keep exit code 0 when samples fail
  KEEP_STACK             Set to 1 to leave containers running after the test

Arguments after `--` are passed directly to JMeter.
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

profile="${1:-small}"
if [[ $# -gt 0 && ${1} != "--" ]]; then
  shift
fi
if [[ ${profile} == "--" ]]; then
  profile="small"
fi
if [[ ${1:-} == "--" ]]; then
  shift
fi

case "${profile}" in
  small|medium|large|xl) ;;
  *)
    echo "Unknown profile: ${profile}" >&2
    usage >&2
    exit 1
    ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available on PATH." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required." >&2
  exit 1
fi

results_dir="${script_dir}/results"
mkdir -p "${results_dir}"

app_count="${APP_COUNT:-2}"
case "${app_count}" in
  2|4|8) ;;
  *)
    echo "APP_COUNT must be one of: 2, 4, 8 (got ${app_count})" >&2
    exit 1
    ;;
esac

plan="can-cache-performance-tests/jmeter/can-cache-${profile}.jmx"
timestamp="$(date -u +%Y%m%d-%H%M%S)"
result_file="${RESULT_FILE:-can-cache-performance-tests/results/can-cache-${profile}-${timestamp}.jtl}"
sampler_jar="can-cache-performance-tests/target/can-cache-performance-test-0.0.1-SNAPSHOT.jar"
mvn_image="${MVN_IMAGE:-maven:3.9.11-eclipse-temurin-21}"
jmeter_log="${result_file%.jtl}.log"
connection_mode="${CONNECTION_MODE:-separate}"
target_host="${TARGET_HOST:-can-cache-agent}"
target_port="${TARGET_PORT:-11211}"
wait_timeout_seconds="${WAIT_TIMEOUT_SECONDS:-120}"
compose_file="$(mktemp "${TMPDIR:-/tmp}/can-cache-performance-${app_count}.XXXXXX.yml")"

generate_compose_file() {
  local file="$1"
  local count="$2"
  local jmeter_image="${JMETER_IMAGE:-anasoid/jmeter:5.6.3-plugins-21-jre}"
  local jmeter_heap="${JMETER_HEAP:--Xms64m -Xmx256m -XX:MaxMetaspaceSize=128m}"
  local replication_factor="${REPLICATION_FACTOR:-3}"
  local anti_entropy_interval="${ANTI_ENTROPY_INTERVAL_MILLIS:-30000}"

  cat > "${file}" <<YAML
name: can-cache-performance

services:
  can-cache-agent:
    build:
      context: ${repo_root}
      dockerfile: Dockerfile.agent
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
      context: ${repo_root}
      dockerfile: Dockerfile
    environment:
      QUARKUS_HTTP_PORT: "$((8080 + index))"
      APP_NETWORK_HOST: 0.0.0.0
      APP_NETWORK_PORT: "11212"
      APP_AGENT_ENABLED: "true"
      APP_AGENT_HOST: can-cache-agent
      APP_AGENT_PORT: "11211"
      APP_AGENT_REGISTRATION_PORT: "11311"
      APP_AGENT_ADVERTISED_HOST: can-cache-app-${index}
      APP_CLUSTER_DISCOVERY_NODE_ID: can-cache-app-${index}
      APP_CLUSTER_REPLICATION_FACTOR: "${replication_factor}"
      APP_CLUSTER_REPLICATION_ADVERTISE_HOST: can-cache-app-${index}
      APP_CLUSTER_REPLICATION_PORT: "18080"
      APP_CLUSTER_COORDINATION_ANTI_ENTROPY_INTERVAL_MILLIS: "${anti_entropy_interval}"
    depends_on:
      - can-cache-agent

YAML
  done

  cat >> "${file}" <<YAML
  wait-for-agent:
    image: python:3.13-alpine
    environment:
      WAIT_HEALTH_URL: http://can-cache-agent:8080/agent/instances
      WAIT_HEALTHY_INSTANCES: "${count}"
      WAIT_TIMEOUT_SECONDS: "${wait_timeout_seconds}"
    command:
      - python
      - -c
      - |
        import json
        import os
        import time
        from urllib.request import urlopen

        url = os.environ["WAIT_HEALTH_URL"]
        expected = int(os.environ["WAIT_HEALTHY_INSTANCES"])
        timeout = float(os.environ["WAIT_TIMEOUT_SECONDS"])
        deadline = time.monotonic() + timeout
        last = "not checked"
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=2) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                healthy = int(payload.get("healthyInstances", 0))
                total = int(payload.get("totalInstances", 0))
                if healthy >= expected:
                    print(f"agent ready: healthy={healthy} total={total}", flush=True)
                    raise SystemExit(0)
                last = f"healthy={healthy} total={total}"
            except Exception as error:
                last = str(error)
            time.sleep(1)
        raise SystemExit(f"timed out waiting for {url}: {last}")
    depends_on:
      - can-cache-agent
YAML

  for index in $(seq 1 "${count}"); do
    echo "      - can-cache-app-${index}" >> "${file}"
  done

  cat >> "${file}" <<YAML

  jmeter:
    image: ${jmeter_image}
    entrypoint: ["jmeter"]
    working_dir: /workspace
    environment:
      HEAP: "${jmeter_heap}"
    volumes:
      - ${repo_root}:/workspace
    depends_on:
      - can-cache-agent
YAML

  for index in $(seq 1 "${count}"); do
    echo "      - can-cache-app-${index}" >> "${file}"
  done
}

validate_jmeter_results() {
  if [[ ${ALLOW_JMETER_ERRORS:-0} == "1" ]]; then
    echo "[jmeter] skipping result validation because ALLOW_JMETER_ERRORS=1" >&2
    return 0
  fi

  python3 - "${repo_root}/${result_file}" <<'PY'
import csv
import math
import sys

path = sys.argv[1]
total = 0
failed = 0
elapsed = []
timestamps = []

with open(path, newline="") as handle:
    for row in csv.DictReader(handle):
        total += 1
        if row.get("success") != "true":
            failed += 1
        try:
            elapsed.append(int(row.get("elapsed", "0")))
            timestamps.append(int(row.get("timeStamp", "0")))
        except ValueError:
            pass

if total == 0:
    raise SystemExit("JMeter produced no samples")
if failed:
    raise SystemExit(f"JMeter reported {failed} failed samples")

elapsed.sort()
avg = round(sum(elapsed) / len(elapsed), 2) if elapsed else 0
p95 = elapsed[max(0, math.ceil(len(elapsed) * 0.95) - 1)] if elapsed else 0
max_elapsed = elapsed[-1] if elapsed else 0
duration_ms = max(timestamps) - min(timestamps) if len(timestamps) > 1 else 0
throughput = round(total / (duration_ms / 1000), 2) if duration_ms > 0 else total
print(
    f"[jmeter] samples={total} failed={failed} avg_ms={avg} p95_ms={p95} max_ms={max_elapsed} throughput_per_sec={throughput}",
    file=sys.stderr,
)
PY
}

wait_for_data_transfer() {
  docker compose -f "${compose_file}" run --rm -T \
    -e PERF_TARGET_HOST="${target_host}" \
    -e PERF_TARGET_PORT="${target_port}" \
    -e PERF_APP_COUNT="${app_count}" \
    wait-for-agent python - <<'PY'
import os
import socket
import time

host = os.environ["PERF_TARGET_HOST"]
port = int(os.environ["PERF_TARGET_PORT"])
expected = int(os.environ["PERF_APP_COUNT"])
timeout = float(os.environ["WAIT_TIMEOUT_SECONDS"])
deadline = time.monotonic() + timeout
key = f"perf-transfer-warmup-{time.time_ns()}"
value = (f"value-{time.time_ns()}").encode("utf-8")
required_reads = max(expected, 3)
last = "not checked"

def request(payload):
    with socket.create_connection((host, port), timeout=2) as sock:
        sock.settimeout(3)
        sock.sendall(payload)
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            joined = b"".join(chunks)
            if joined.endswith(b"\r\n") and (b"\r\nEND\r\n" in joined or joined in (b"STORED\r\n", b"DELETED\r\n", b"NOT_FOUND\r\n")):
                return joined
        return b"".join(chunks)

def set_key():
    response = request(b"set " + key.encode() + b" 0 60 " + str(len(value)).encode() + b"\r\n" + value + b"\r\n")
    return response.strip() == b"STORED", response

def get_key():
    response = request(b"get " + key.encode() + b"\r\n")
    return value in response and response.endswith(b"END\r\n"), response

while time.monotonic() < deadline:
    try:
        ok, response = set_key()
        if not ok:
            last = f"set response={response!r}"
            time.sleep(0.5)
            continue
        successful_reads = 0
        for _ in range(required_reads):
            ok, response = get_key()
            if ok:
                successful_reads += 1
            else:
                last = f"get response={response!r}"
                break
        if successful_reads >= required_reads:
            print(f"data transfer ready: {successful_reads}/{required_reads} cross-connection reads succeeded")
            raise SystemExit(0)
    except Exception as error:
        last = repr(error)
    time.sleep(0.5)

raise SystemExit(f"timed out waiting for stable data transfer through {host}:{port}: {last}")
PY
}

validate_agent_distribution() {
  docker compose -f "${compose_file}" run --rm -T wait-for-agent python - <<'PY'
import json
import os
from urllib.request import urlopen

url = os.environ["WAIT_HEALTH_URL"]
expected = int(os.environ["WAIT_HEALTHY_INSTANCES"])
with urlopen(url, timeout=5) as response:
    payload = json.loads(response.read().decode("utf-8"))

instances = payload.get("instances", [])
healthy = int(payload.get("healthyInstances", 0))
total = int(payload.get("totalInstances", 0))
if total < expected or healthy < expected:
    raise SystemExit(f"agent not healthy after JMeter: healthy={healthy} total={total} expected={expected}")

unused = [item.get("address") for item in instances if int(item.get("totalConnections", 0)) <= 0]
no_bytes = [
    item.get("address")
    for item in instances
    if int(item.get("bytesIn", 0)) <= 0 or int(item.get("bytesOut", 0)) <= 0
]
if unused:
    raise SystemExit(f"some upstreams received no connections: {unused}")
if no_bytes:
    raise SystemExit(f"some upstreams did not transfer bytes both ways: {no_bytes}")

print(
    "agent distribution ok: "
    + ", ".join(
        f"{item.get('address')} conn={item.get('totalConnections')} in={item.get('bytesIn')} out={item.get('bytesOut')}"
        for item in sorted(instances, key=lambda item: item.get("address", ""))
    )
)
PY
}

generate_compose_file "${compose_file}" "${app_count}"

echo "[build] packaging JMeter sampler with ${mvn_image}" >&2
docker run --rm \
  -v "${repo_root}:/workspace" \
  -w /workspace \
  "${mvn_image}" \
  ./mvnw -q -f can-cache-performance-tests/pom.xml clean package

if [[ ! -f "${repo_root}/${sampler_jar}" ]]; then
  echo "Sampler JAR not found at ${sampler_jar}" >&2
  exit 1
fi

cleanup() {
  if [[ ${KEEP_STACK:-0} != "1" ]]; then
    docker compose -f "${compose_file}" down --remove-orphans >/dev/null 2>&1 || true
    rm -f "${compose_file}"
  fi
}
trap cleanup EXIT

app_services=()
for index in $(seq 1 "${app_count}"); do
  app_services+=("can-cache-app-${index}")
done

echo "[stack] starting can-cache-agent + ${app_count} can-cache-application containers" >&2
docker compose -f "${compose_file}" up -d --build can-cache-agent "${app_services[@]}"

echo "[wait] waiting for ${app_count} healthy cache applications behind the agent" >&2
docker compose -f "${compose_file}" run --rm wait-for-agent

echo "[wait] validating cross-connection data transfer" >&2
wait_for_data_transfer

props=(
  "-JtargetHost=${target_host}"
  "-JtargetPort=${target_port}"
  "-JresultFile=${result_file}"
  "-Jsearch_paths=/workspace/${sampler_jar}"
  "-JconnectionMode=${connection_mode}"
)

[[ -n ${TTL_SECONDS:-} ]] && props+=("-JttlSeconds=${TTL_SECONDS}")
[[ -n ${CONNECT_TIMEOUT_MILLIS:-} ]] && props+=("-JconnectTimeoutMillis=${CONNECT_TIMEOUT_MILLIS}")
[[ -n ${READ_TIMEOUT_MILLIS:-} ]] && props+=("-JreadTimeoutMillis=${READ_TIMEOUT_MILLIS}")
[[ -n ${KEY_PREFIX:-} ]] && props+=("-JkeyPrefix=${KEY_PREFIX}")
[[ -n ${PAYLOAD_SIZE:-} ]] && props+=("-JpayloadSize=${PAYLOAD_SIZE}")
[[ -n ${DURATION_SECONDS:-} ]] && props+=("-JdurationSeconds=${DURATION_SECONDS}")

jmeter_cmd=(-n -t "${plan}" -l "${result_file}" -j "${jmeter_log}")
jmeter_cmd+=("${props[@]}")
jmeter_cmd+=("$@")

echo "[jmeter] jmeter ${jmeter_cmd[*]}" >&2
docker compose -f "${compose_file}" run --rm jmeter "${jmeter_cmd[@]}"
validate_jmeter_results
validate_agent_distribution

echo "JMeter result: ${result_file}" >&2
echo "JMeter log: ${jmeter_log}" >&2
