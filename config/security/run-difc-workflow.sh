#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
# Liberty/Maven must not inherit Kafka policy-agent JAVA_TOOL_OPTIONS from the shell.
unset JAVA_TOOL_OPTIONS

mkdir -p "${STATE_DIR}/logs"
mkdir -p "${STATE_DIR}/policy"

pkill -f 'stockservice.jar' 2>/dev/null || true
pkill -f 'validationservice.jar' 2>/dev/null || true
pkill -f 'paymentservice.jar' 2>/dev/null || true
pkill -f 'liberty:run' 2>/dev/null || true
sleep 2

if [[ "${DIFC_RESET_CLUSTER}" == "true" ]]; then
  "$(dirname "$0")/reset-kafka-cluster.sh"
else
  echo "=== Skipping Kafka cluster reset (DIFC_RESET_CLUSTER=false) ==="
fi

: > "${STATE_DIR}/logs/order-service.log"
: > "${STATE_DIR}/logs/stock-service.log"
: > "${STATE_DIR}/logs/validation-service.log"
: > "${STATE_DIR}/logs/payment-service.log"
rm -rf "${STATE_DIR}/policy"/*
mkdir -p "${STATE_DIR}/policy"

echo "=== Ensure DIFC cluster (kafka-streams-examples scripts) ==="
"${EXAMPLES_SECURITY}/create-scram-users.sh" | tail -3
"$(dirname "$0")/create-scram-users.sh" | tail -3
"$(dirname "$0")/create-acls.sh" | tail -3
"$(dirname "$0")/create-topics.sh"

echo "=== Build ==="
unset JAVA_TOOL_OPTIONS
require_jars
# Ensure Liberty picks up the latest order-service WAR (async Kafka init).
mvn -q -f "${DEMO_HOME}/pom.xml" clean install -DskipTests -pl order-service -am
rm -rf "${DEMO_HOME}/order-service/target/liberty"

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
  echo "ERROR: Kafka cluster not reachable at ${BOOTSTRAP_SERVERS}" >&2
  return 1
}

wait_for_kafka 90

echo "=== Start order-service (grantor) ==="
LIBERTY_LOG="${DEMO_HOME}/order-service/target/liberty/wlp/usr/servers/defaultServer/logs/messages.log"
LIBERTY_CONSOLE="${DEMO_HOME}/order-service/target/liberty/wlp/usr/servers/defaultServer/logs/console.log"
mkdir -p "$(dirname "${LIBERTY_LOG}")"
readarray -t GRANTOR_OPTS < <(grantor_policy_trust_java_opts)
unset JAVA_TOOL_OPTIONS
export JAVA_TOOL_OPTIONS="${GRANTOR_OPTS[*]} -DDIFC_ENABLED=${DIFC_ENABLED}"
nohup "$(dirname "$0")/start-order-service.sh" >"${STATE_DIR}/logs/order-service.log" 2>&1 &
for i in $(seq 1 180); do
  if grep -q 'DIFC grantor ready' "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null; then
    echo "order-service grantor ready"
    break
  fi
  if grep -q 'Application order-service started' "${STATE_DIR}/logs/order-service.log" 2>/dev/null \
      && [[ "${DIFC_ENABLED}" == "false" ]]; then
    echo "order-service started (DIFC disabled)"
    break
  fi
  sleep 2
done
if [[ "${DIFC_ENABLED}" == "false" ]]; then
  if ! grep -q 'CWWKZ0001I: Application order-service started' "${LIBERTY_LOG}" 2>/dev/null; then
    echo "ERROR: order-service did not become ready" >&2
    tail -40 "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null | tail -40 >&2 || true
    exit 1
  fi
elif ! grep -q 'DIFC grantor ready' "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null; then
  echo "ERROR: order-service grantor did not become ready" >&2
  tail -40 "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null | tail -40 >&2 || true
  exit 1
fi

echo "=== Start pipeline services ==="
nohup "$(dirname "$0")/start-stock-service.sh" >"${STATE_DIR}/logs/stock-service.log" 2>&1 &
nohup "$(dirname "$0")/start-validation-service.sh" >"${STATE_DIR}/logs/validation-service.log" 2>&1 &
nohup "$(dirname "$0")/start-payment-service.sh" >"${STATE_DIR}/logs/payment-service.log" 2>&1 &

for i in $(seq 1 60); do
  if curl -sf -o /dev/null "http://localhost:9080/api/order" 2>/dev/null \
      || curl -sf -o /dev/null -X OPTIONS "http://localhost:9080/api/order" 2>/dev/null; then
    echo "order-service HTTP ready"
    break
  fi
  sleep 2
done

for i in $(seq 1 180); do
  stock=false; validation=false; payment=false
  grep -q 'DIFC grants ready' "${STATE_DIR}/logs/stock-service.log" 2>/dev/null && stock=true
  grep -q 'DIFC grants ready' "${STATE_DIR}/logs/validation-service.log" 2>/dev/null && validation=true
  grep -q 'DIFC grants ready' "${STATE_DIR}/logs/payment-service.log" 2>/dev/null && payment=true
  if [[ "${stock}" == true && "${validation}" == true && "${payment}" == true ]]; then
    echo "stock, validation, payment DIFC grants ready"
    break
  fi
  sleep 2
done
if ! grep -q 'DIFC grants ready' "${STATE_DIR}/logs/stock-service.log" 2>/dev/null \
    || ! grep -q 'DIFC grants ready' "${STATE_DIR}/logs/validation-service.log" 2>/dev/null \
    || ! grep -q 'DIFC grants ready' "${STATE_DIR}/logs/payment-service.log" 2>/dev/null; then
  echo "ERROR: pipeline services did not receive DIFC grants" >&2
  tail -20 "${STATE_DIR}/logs/stock-service.log" >&2 || true
  tail -20 "${STATE_DIR}/logs/validation-service.log" >&2 || true
  tail -20 "${STATE_DIR}/logs/payment-service.log" >&2 || true
  exit 1
fi

echo "=== Post test order ==="
HTTP_CODE=$(curl -s -o /tmp/jug-order-response.txt -w '%{http_code}' \
  -X POST "http://localhost:9080/api/order" \
  -H 'Content-Type: application/json' \
  -d '{"customerId":1,"productId":42,"amount":2,"price":10,"cardNumber":"4111111111111111"}')
echo "HTTP ${HTTP_CODE}"
cat /tmp/jug-order-response.txt 2>/dev/null || true

sleep 30

echo
echo "=== DIFC grant summary (order grantor) ==="
grep -E '\[DIFC\] (grantPrivilege|grantDenied|grantPolicyVerified|createTag)' \
  "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null | tail -20 || echo "(no grant lines)"

echo
echo "=== Expression lineage verification (grantor) ==="
grep -E '\[DIFC\] grantLineageVerify' "${STATE_DIR}/logs/order-service.log" "${LIBERTY_LOG}" 2>/dev/null | tail -40 \
  || echo "(no grantLineageVerify lines)"

echo
echo "=== Processing policy RA diagrams ==="
POLICY_DIAGRAM_DIR="${STATE_DIR}/policy-diagrams"
if ls "${STATE_DIR}/policy"/*/*.json >/dev/null 2>&1; then
  python3 "${EXAMPLES_SECURITY}/generate-policy-diagrams.py" "${STATE_DIR}/policy" "${POLICY_DIAGRAM_DIR}"
  JUG_DOCS="${DEMO_HOME}/docs/difc-ra-diagrams"
  mkdir -p "${JUG_DOCS}"
  cp -a "${POLICY_DIAGRAM_DIR}"/*.{png,dot} "${JUG_DOCS}/" 2>/dev/null || true
  EXAMPLES_DOCS="${DEMO_HOME}/../kafka-streams-examples/docs/difc-ra-diagrams"
  mkdir -p "${EXAMPLES_DOCS}"
  cp -a "${POLICY_DIAGRAM_DIR}"/*-svc-*.{png,dot} "${EXAMPLES_DOCS}/" 2>/dev/null || true
  echo "RA diagrams: ${POLICY_DIAGRAM_DIR}"
else
  echo "(no attested policies for diagrams)"
fi

echo
echo "=== Pipeline logs ==="
grep -E 'Published (stock-check|validation|billing)|Billing event consumed|DIFC grants ready' \
  "${STATE_DIR}/logs/"*.log 2>/dev/null | tail -30 || true

echo
echo "Logs: ${STATE_DIR}/logs"
echo "Policies: ${STATE_DIR}/policy"
ls -la "${STATE_DIR}/policy"/*/*.json 2>/dev/null || echo "(no policies yet)"
echo "Done."
