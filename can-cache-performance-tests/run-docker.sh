#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-docker.sh [PROFILE] [-- JMETER_ARGS...]

Builds the custom JMeter Java sampler, starts one can-cache-agent plus two
can-cache-application containers, waits until both apps are registered and
healthy, then runs the selected JMeter plan in Docker.

Profiles:
  small   Lightweight smoke workload (default)
  medium  Steady mid-tier workload
  large   High concurrency workload
  xl      Saturation-level workload

Environment overrides:
  TARGET_HOST            Target inside Docker network (default: can-cache-agent)
  TARGET_PORT            Target TCP port (default: 11211)
  TTL_SECONDS            TTL in seconds for generated SET commands
  CONNECT_TIMEOUT_MILLIS Socket connect timeout (ms)
  READ_TIMEOUT_MILLIS    Socket read timeout (ms)
  KEY_PREFIX             Prefix for generated cache keys
  PAYLOAD_SIZE           Payload size in bytes
  DURATION_SECONDS       Thread group duration override in seconds
  RESULT_FILE            Result path under the repository
  JMETER_IMAGE           JMeter image (default: alpine/jmeter:5.6.3)
  JMETER_HEAP            JMeter JVM heap (default: -Xms64m -Xmx256m -XX:MaxMetaspaceSize=128m)
  MVN_IMAGE              Maven/JDK image for sampler build
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
compose_file="${script_dir}/docker-compose.performance.yml"

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

plan="can-cache-performance-tests/jmeter/can-cache-${profile}.jmx"
timestamp="$(date -u +%Y%m%d-%H%M%S)"
result_file="${RESULT_FILE:-can-cache-performance-tests/results/can-cache-${profile}-${timestamp}.jtl}"
sampler_jar="can-cache-performance-tests/target/can-cache-performance-test-0.0.1-SNAPSHOT.jar"
mvn_image="${MVN_IMAGE:-maven:3.9.11-eclipse-temurin-21}"
jmeter_log="${result_file%.jtl}.log"

validate_jmeter_results() {
  if [[ ${ALLOW_JMETER_ERRORS:-0} == "1" ]]; then
    echo "[jmeter] skipping result validation because ALLOW_JMETER_ERRORS=1" >&2
    return 0
  fi

  python3 - "${repo_root}/${result_file}" <<'PY'
import csv
import sys

path = sys.argv[1]
total = 0
failed = 0

with open(path, newline="") as handle:
    for row in csv.DictReader(handle):
        total += 1
        if row.get("success") != "true":
            failed += 1

print(f"[jmeter] samples={total} failed={failed}", file=sys.stderr)
if total == 0:
    raise SystemExit("JMeter produced no samples")
if failed:
    raise SystemExit(f"JMeter reported {failed} failed samples")
PY
}

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
  fi
}
trap cleanup EXIT

echo "[stack] starting can-cache-agent + 2 can-cache-application containers" >&2
docker compose -f "${compose_file}" up -d --build can-cache-agent can-cache-app-1 can-cache-app-2

echo "[wait] waiting for two healthy cache applications behind the agent" >&2
docker compose -f "${compose_file}" run --rm wait-for-agent

props=(
  "-JtargetHost=${TARGET_HOST:-can-cache-agent}"
  "-JtargetPort=${TARGET_PORT:-11211}"
  "-JresultFile=${result_file}"
  "-Jsearch_paths=/workspace/${sampler_jar}"
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

echo "JMeter result: ${result_file}" >&2
echo "JMeter log: ${jmeter_log}" >&2
