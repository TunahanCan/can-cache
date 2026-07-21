#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-docker.sh [PROFILE] [-- JMETER_ARGS...]

Runs a Can Cache JMeter performance profile using a Dockerised JMeter
installation.

Profiles:
  small   Lightweight smoke workload (default)
  medium  Steady mid-tier workload
  large   High concurrency workload
  xl      Saturation-level workload

Environment overrides:
  TARGET_HOST            Target hostname/IP (default: host.docker.internal)
  TARGET_PORT            Target TCP port (default: 11211)
  TTL_SECONDS            TTL in seconds for generated SET commands
  CONNECT_TIMEOUT_MILLIS Socket connect timeout (ms)
  READ_TIMEOUT_MILLIS    Socket read timeout (ms)
  KEY_PREFIX             Prefix for generated cache keys
  PAYLOAD_SIZE           Payload size in bytes (plan default if unset)
  DURATION_SECONDS       Thread group duration override in seconds
  RESULT_FILE            Path for the JMeter results (.jtl) file
  JMETER_IMAGE           Docker image to use for JMeter
                         (default: anasoid/jmeter:5.6.3-21-jre)

Any arguments after `--` are passed directly to the JMeter command inside the
container.
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
sampler_module="${repo_root}/can-cache-performance-tests"
mvnw_path="${repo_root}/mvnw"

profile="small"
if [[ $# -gt 0 && ${1} != "--" ]]; then
  profile="${1}"
  shift
fi

if [[ ${1:-} == "--" ]]; then
  shift
fi

build_sampler_jar() {
  if [[ -x ${mvnw_path} ]]; then
    echo "Building Java sampler JAR" >&2
    "${mvnw_path}" -q -f "${sampler_module}/pom.xml" package >&2
    return 0
  fi

  if command -v mvn >/dev/null 2>&1; then
    echo "Building Java sampler JAR with system Maven" >&2
    mvn -q -f "${sampler_module}/pom.xml" package >&2
    return 0
  fi

  echo "Unable to locate mvnw or mvn to build the Java sampler." >&2
  return 1
}

build_sampler_jar

sampler_jar="$(find "${sampler_module}/target" -maxdepth 1 -type f \
  -name 'can-cache-performance-test-*.jar' \
  ! -name '*-sources.jar' ! -name '*-javadoc.jar' | LC_ALL=C sort | tail -n 1)"
if [[ -z ${sampler_jar} ]]; then
  echo "Java sampler JAR was not produced under ${sampler_module}/target." >&2
  exit 1
fi
sampler_jar_container="/jmeter/additional/lib/ext/$(basename "${sampler_jar}")"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available on PATH. Install Docker to run the plans in a container." >&2
  exit 1
fi

case "${profile}" in
  small) plan="can-cache-small.jmx" ;;
  medium) plan="can-cache-medium.jmx" ;;
  large) plan="can-cache-large.jmx" ;;
  xl) plan="can-cache-xl.jmx" ;;
  *)
    echo "Unknown profile: ${profile}" >&2
    usage >&2
    exit 1
    ;;
esac

plan_container="/workspace/can-cache-performance-tests/jmeter/${plan}"
results_dir="${script_dir}/results"
mkdir -p "${results_dir}"

# Build a unique default result file name if not provided via the environment.
default_result_file="${results_dir}/$(basename "${plan}" .jmx)-$(date +%Y%m%d-%H%M%S).jtl"
result_file="${RESULT_FILE:-${default_result_file}}"
case "${result_file}" in
  /*) ;;
  *) result_file="$(pwd)/${result_file}" ;;
esac
mkdir -p "$(dirname "${result_file}")"
result_dir="$(cd "$(dirname "${result_file}")" && pwd)"
result_file="${result_dir}/$(basename "${result_file}")"
result_file_container="/results/$(basename "${result_file}")"

props=(
  "-JtargetHost=${TARGET_HOST:-host.docker.internal}"
  "-JtargetPort=${TARGET_PORT:-11211}"
  "-JresultFile=${result_file_container}"
)

[[ -n ${TTL_SECONDS:-} ]] && props+=("-JttlSeconds=${TTL_SECONDS}")
[[ -n ${CONNECT_TIMEOUT_MILLIS:-} ]] && props+=("-JconnectTimeoutMillis=${CONNECT_TIMEOUT_MILLIS}")
[[ -n ${READ_TIMEOUT_MILLIS:-} ]] && props+=("-JreadTimeoutMillis=${READ_TIMEOUT_MILLIS}")
[[ -n ${KEY_PREFIX:-} ]] && props+=("-JkeyPrefix=${KEY_PREFIX}")
[[ -n ${PAYLOAD_SIZE:-} ]] && props+=("-JpayloadSize=${PAYLOAD_SIZE}")
[[ -n ${DURATION_SECONDS:-} ]] && props+=("-JdurationSeconds=${DURATION_SECONDS}")

jmeter_image="${JMETER_IMAGE:-anasoid/jmeter:5.6.3-21-jre}"

docker_cmd=(docker run --rm)
if [[ $(uname -s) == "Linux" ]]; then
  docker_cmd+=(--add-host=host.docker.internal:host-gateway)
fi

docker_cmd+=(-v "${sampler_jar}:${sampler_jar_container}:ro")
docker_cmd+=(-v "${repo_root}:/workspace:ro")
docker_cmd+=(-v "${result_dir}:/results")
docker_cmd+=(-w /tmp "${jmeter_image}" -n -t "${plan_container}")
docker_cmd+=("${props[@]}")
docker_cmd+=("$@")

echo "Running Dockerised JMeter: ${docker_cmd[*]}"
echo "Results will be written to ${result_file}" >&2
"${docker_cmd[@]}"

echo "JMeter execution finished. Results available at ${result_file}" >&2
