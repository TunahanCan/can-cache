#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-local.sh [PROFILE] [-- JMETER_ARGS...]

Runs a Can Cache JMeter performance profile against a locally running instance
using a locally installed Apache JMeter distribution.

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

sampler_jar_abs="$(cd "$(dirname "${sampler_jar_rel}")" && pwd)/$(basename "${sampler_jar_rel}")"
echo "Sampler JAR available at ${sampler_jar_abs}" >&2

if [[ -n ${JMETER_HOME:-} ]]; then
  jmeter_home="${JMETER_HOME}"
elif command -v jmeter >/dev/null 2>&1; then
  jmeter_bin="$(command -v jmeter)"
  jmeter_home="$(cd "$(dirname "${jmeter_bin}")/.." && pwd)"
else
  echo "Apache JMeter is not available on PATH. Install JMeter locally or set JMETER_HOME." >&2
  exit 1
fi

if [[ ! -d "${jmeter_home}/lib/ext" ]]; then
  echo "Could not locate lib/ext directory under ${jmeter_home}." >&2
  exit 1
fi

sampler_target="${jmeter_home}/lib/ext/$(basename "${sampler_jar_rel}")"
echo "Copying sampler JAR to ${sampler_target}" >&2
cp "${sampler_jar_rel}" "${sampler_target}"

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
default_result_file="${results_dir}/$(basename "${plan}" .jmx)-$(date +%Y%m%d-%H%M%S).jtl"
result_file="${RESULT_FILE:-${default_result_file}}"

echo "Results will be written to ${result_file}" >&2

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

echo "Running JMeter locally: ${jmeter_cmd[*]}"
"${jmeter_cmd[@]}"

echo "JMeter execution finished. Results available at ${result_file}" >&2
