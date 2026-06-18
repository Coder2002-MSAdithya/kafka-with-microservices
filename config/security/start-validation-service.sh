#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
readarray -t AGENT_OPTS < <(policy_agent_java_opts validation-svc ValidationService "${STATE_DIR}")
KAFKA_CLIENTS_JAR="${KAFKA_HOME}/security/policy-agent/signed-jars/kafka-clients-4.0.0.jar"
JAR="${DEMO_HOME}/validation-service/target/validationservice.jar"
exec java "${AGENT_OPTS[@]}" \
  -DDIFC_ENABLED="${DIFC_ENABLED}" \
  -cp "${KAFKA_CLIENTS_JAR}:${JAR}" \
  jugistanbul.validationservice.ValidationService "$@"
