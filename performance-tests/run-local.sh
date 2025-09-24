#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-local.sh [PROFILE] [-- JMETER_ARGS...]

Runs a Can Cache JMeter performance profile against a locally running instance.

Profiles:
  small   Lightweight smoke workload (default)
  medium  Steady mid-tier workload
  large   High concurrency workload
  xl      Saturation-level workload

Environment overrides:
  TARGET_HOST            Target hostname/IP (default: 127.0.0.1)
  TARGET_PORT            Target TCP port (default: 11211)
  TTL_SECONDS            TTL in seconds for generated SET commands
  CONNECT_TIMEOUT_MILLIS Socket connect timeout (ms)
  READ_TIMEOUT_MILLIS    Socket read timeout (ms)
  KEY_PREFIX             Prefix for generated cache keys
  PAYLOAD_SIZE           Payload size in bytes (plan default if unset)
  DURATION_SECONDS       Thread group duration override in seconds
  RESULT_FILE            Path for the JMeter results (.jtl) file
  JMETER_IMAGE           Docker image to use when falling back to containerised JMeter

Any arguments after `--` are passed directly to the JMeter command.
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

profile="${1:-small}"
if [[ $# -gt 0 && ${1} != "--" ]]; then
  shift
fi

if [[ ${profile} == "--" ]]; then
  profile="small"
fi

sibling_mvnw="$(dirname "$0")/../mvnw"
sampler_jar_rel="performance-tests/java-sampler/target/can-cache-jmeter-sampler-0.0.1-SNAPSHOT.jar"
local_jmeter_env=()
docker_jmeter_env=()
local_classpath=${JMETER_ADD_CLASSPATH:-}
docker_classpath=${JMETER_ADD_CLASSPATH:-}

ensure_sampler_jar() {
  if [[ -f "${sampler_jar_rel}" ]]; then
    return 0
  fi

  if [[ -x ./mvnw ]]; then
    echo "Java sampler JAR not found at ${sampler_jar_rel}, attempting to build it" >&2
    ./mvnw -q -f performance-tests/java-sampler/pom.xml package >&2
  elif [[ -x ${sibling_mvnw} ]]; then
    echo "Java sampler JAR not found at ${sampler_jar_rel}, attempting to build it" >&2
    "${sibling_mvnw}" -q -f performance-tests/java-sampler/pom.xml package >&2
  else
    echo "Warning: Java sampler JAR not found at ${sampler_jar_rel}. Build it with ./mvnw -f performance-tests/java-sampler/pom.xml package" >&2
    return 1
  fi
}

ensure_sampler_jar || true

if [[ -f "${sampler_jar_rel}" ]]; then
  sampler_abs="$(pwd)/${sampler_jar_rel}"
  if [[ -n ${local_classpath} ]]; then
    local_classpath+=":${sampler_abs}"
    docker_classpath+=":/workspace/${sampler_jar_rel}"
  else
    local_classpath=${sampler_abs}
    docker_classpath=/workspace/${sampler_jar_rel}
  fi
fi

if [[ -n ${local_classpath} ]]; then
  local_jmeter_env+=("JMETER_ADD_CLASSPATH=${local_classpath}")
fi

if [[ -n ${docker_classpath} ]]; then
  docker_jmeter_env+=("-e")
  docker_jmeter_env+=("JMETER_ADD_CLASSPATH=${docker_classpath}")
fi

case "${profile}" in
  small) plan="jmeter/can-cache-small.jmx" ;;
  medium) plan="jmeter/can-cache-medium.jmx" ;;
  large) plan="jmeter/can-cache-large.jmx" ;;
  xl) plan="jmeter/can-cache-xl.jmx" ;;
  *)
    echo "Unknown profile: ${profile}" >&2
    usage >&2
    exit 1
    ;;
 esac

if [[ ${1:-} == "--" ]]; then
  shift
fi

results_dir="performance-tests/results"
mkdir -p "${results_dir}"

# Build default result file name if not provided via env or args.
default_result_file="${results_dir}/$(basename "${plan}" .jmx).jtl"
result_file="${RESULT_FILE:-${default_result_file}}"

props=(
  "-JtargetHost=${TARGET_HOST:-127.0.0.1}"
  "-JtargetPort=${TARGET_PORT:-11211}"
  "-JresultFile=${result_file}"
)

[[ -n ${TTL_SECONDS:-} ]] && props+=("-JttlSeconds=${TTL_SECONDS}")
[[ -n ${CONNECT_TIMEOUT_MILLIS:-} ]] && props+=("-JconnectTimeoutMillis=${CONNECT_TIMEOUT_MILLIS}")
[[ -n ${READ_TIMEOUT_MILLIS:-} ]] && props+=("-JreadTimeoutMillis=${READ_TIMEOUT_MILLIS}")
[[ -n ${KEY_PREFIX:-} ]] && props+=("-JkeyPrefix=${KEY_PREFIX}")
[[ -n ${PAYLOAD_SIZE:-} ]] && props+=("-JpayloadSize=${PAYLOAD_SIZE}")
[[ -n ${DURATION_SECONDS:-} ]] && props+=("-JdurationSeconds=${DURATION_SECONDS}")

jmeter_cmd=(jmeter -n -t "${plan}" -l "${result_file}")
jmeter_cmd+=("${props[@]}")
jmeter_cmd+=("$@")

if command -v jmeter >/dev/null 2>&1; then
  echo "Running JMeter locally: ${jmeter_cmd[*]}"
  if [[ ${#local_jmeter_env[@]} -gt 0 ]]; then
    env "${local_jmeter_env[@]}" "${jmeter_cmd[@]}"
  else
    "${jmeter_cmd[@]}"
  fi
  exit 0
fi

jmeter_image="${JMETER_IMAGE:-justb4/jmeter:5.6.2}"
docker_cmd=(docker run --rm)
if [[ ${#docker_jmeter_env[@]} -gt 0 ]]; then
  docker_cmd+=("${docker_jmeter_env[@]}")
fi
docker_cmd+=(-v "$(pwd)":/workspace -w /workspace "${jmeter_image}" jmeter -n -t "${plan}" -l "${result_file}")
docker_cmd+=("${props[@]}")

if command -v docker >/dev/null 2>&1; then
  echo "Local jmeter not found, using Docker image ${jmeter_image}" >&2
  echo "Running: ${docker_cmd[*]}"
  "${docker_cmd[@]}" "$@"
  exit 0
fi

echo "Neither jmeter nor docker is available on PATH. Install Apache JMeter or Docker to run the plans." >&2
exit 1
