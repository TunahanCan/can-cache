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
  TARGET_HOST            Target hostname/IP (default: 127.0.0.1)
  TARGET_PORT            Target TCP port (default: 11211)
  TTL_SECONDS            TTL in seconds for generated SET commands
  CONNECT_TIMEOUT_MILLIS Socket connect timeout (ms)
  READ_TIMEOUT_MILLIS    Socket read timeout (ms)
  KEY_PREFIX             Prefix for generated cache keys
  PAYLOAD_SIZE           Payload size in bytes (plan default if unset)
  DURATION_SECONDS       Thread group duration override in seconds
  RESULT_FILE            Path for the JMeter results (.jtl) file
  JMETER_IMAGE           Docker image to use for JMeter (default: justb4/jmeter:5.6.2)

Any arguments after `--` are passed directly to the JMeter command inside the
container.
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

sampler_jar_rel="performance-tests/java-sampler/target/can-cache-jmeter-sampler-0.0.1-SNAPSHOT.jar"

build_sampler_jar() {
  if [[ -x ./mvnw ]]; then
    echo "Building Java sampler JAR" >&2
    ./mvnw -q -f performance-tests/java-sampler/pom.xml package >&2
    return 0
  fi

  sibling_mvnw="$(dirname "$0")/../mvnw"
  if [[ -x ${sibling_mvnw} ]]; then
    echo "Building Java sampler JAR" >&2
    "${sibling_mvnw}" -q -f performance-tests/java-sampler/pom.xml package >&2
    return 0
  fi

  echo "Unable to locate mvnw to build the Java sampler. Ensure the repository root contains mvnw." >&2
  return 1
}

build_sampler_jar

if [[ ! -f "${sampler_jar_rel}" ]]; then
  echo "Java sampler JAR not found at ${sampler_jar_rel} after build." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available on PATH. Install Docker to run the plans in a container." >&2
  exit 1
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

jmeter_image="${JMETER_IMAGE:-justb4/jmeter:5.6.2}"

docker_cmd=(docker run --rm)
if [[ -n ${JMETER_ADD_CLASSPATH:-} ]]; then
  docker_cmd+=(-e "JMETER_ADD_CLASSPATH=${JMETER_ADD_CLASSPATH}:/workspace/${sampler_jar_rel}")
else
  docker_cmd+=(-e "JMETER_ADD_CLASSPATH=/workspace/${sampler_jar_rel}")
fi

docker_cmd+=(-v "$(pwd)":/workspace)
docker_cmd+=(-w /workspace "${jmeter_image}" jmeter -n -t "${plan}" -l "${result_file}")
docker_cmd+=("${props[@]}")

echo "Running Dockerised JMeter: ${docker_cmd[*]} $*"
"${docker_cmd[@]}" "$@"
