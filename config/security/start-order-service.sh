#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
unset JAVA_TOOL_OPTIONS
require_jars
cd "${DEMO_HOME}"
export DIFC_ENABLED="${DIFC_ENABLED:-true}"
export MAVEN_OPTS="${MAVEN_OPTS:-} -DDIFC_ENABLED=${DIFC_ENABLED}"
rm -rf "${DEMO_HOME}/order-service/target/liberty"
exec mvn -q -pl order-service liberty:run
