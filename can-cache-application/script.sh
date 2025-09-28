#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_DIR="${REPO_ROOT}/can-cache-application"
TARGET_JAR="${APP_DIR}/target/quarkus-app/quarkus-run.jar"
MVNW="${REPO_ROOT}/mvnw"

cd "${APP_DIR}"

if [[ ! -f "${TARGET_JAR}" ]]; then
  echo "[run-prod] Packaged runner not found; building it with tests skipped..." >&2
  "${MVNW}" package -DskipTests >/dev/null
  echo "[run-prod] Build complete." >&2
fi

echo "[run-prod] Starting can-cache from ${TARGET_JAR}" >&2
exec java -jar "${TARGET_JAR}" "$@"
