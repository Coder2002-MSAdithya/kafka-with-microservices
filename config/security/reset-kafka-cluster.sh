#!/usr/bin/env bash
# Wipe, format, and start the shared DIFC Kafka cluster for a clean grantor/tag state.
set -euo pipefail
source "$(dirname "$0")/_env.sh"

kill_demo_ports() {
  for port in 9092 9094 9093 9080; do
    if command -v fuser >/dev/null 2>&1; then
      fuser -k "${port}/tcp" 2>/dev/null || true
    fi
  done
  pkill -f 'kafka-server-start.*broker.node' 2>/dev/null || true
  pkill -f 'kafka-server-start.*controller.properties' 2>/dev/null || true
  pkill -f 'stockservice.jar' 2>/dev/null || true
  pkill -f 'validationservice.jar' 2>/dev/null || true
  pkill -f 'paymentservice.jar' 2>/dev/null || true
  pkill -f 'liberty:run' 2>/dev/null || true
  sleep 2
}

wait_for_port() {
  local port="$1"
  local label="$2"
  local tries="${3:-60}"
  for ((n = 1; n <= tries; n++)); do
    if ss -tln 2>/dev/null | grep -q ":${port} "; then
      echo "${label} listening on :${port}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${label} did not start on :${port}" >&2
  return 1
}

wait_for_kafka() {
  local tries="${1:-90}"
  for ((n = 1; n <= tries; n++)); do
    if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_CONFIG}" >/dev/null 2>&1; then
      echo "Kafka cluster is reachable at ${BOOTSTRAP_SERVERS}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Kafka cluster not reachable" >&2
  return 1
}

echo "=== Reset Kafka cluster (wipe + format + start) ==="
kill_demo_ports
"${EXAMPLES_SECURITY}/wipe-cluster-logs.sh"
"${EXAMPLES_SECURITY}/format-kraft-storage.sh"

nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONTROLLER_CONFIG}" \
  >"${STATE_DIR}/logs/kafka-controller.log" 2>&1 &
wait_for_port 9093 controller 60

nohup env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER1_CONFIG}" \
  >"${STATE_DIR}/logs/kafka-broker-1.log" 2>&1 &
nohup env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER2_CONFIG}" \
  >"${STATE_DIR}/logs/kafka-broker-2.log" 2>&1 &
wait_for_port 9092 broker-1 90
wait_for_port 9094 broker-2 90
wait_for_kafka 90
sleep 10
echo "Kafka cluster reset complete."
