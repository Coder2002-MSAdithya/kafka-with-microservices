#!/usr/bin/env bash
# Baseline workflow without DIFC (plain localhost:9092). Requires a running Kafka broker.
set -euo pipefail
DEMO_HOME="$(cd "$(dirname "$0")/../.." && pwd)"
export DIFC_ENABLED=false
export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
STATE_DIR="${STATE_DIR:-/tmp/jug-kafka-baseline}"
export STATE_DIR
mkdir -p "${STATE_DIR}/logs"

mvn -q -f "${DEMO_HOME}/pom.xml" package -DskipTests

pkill -f 'stockservice.jar' 2>/dev/null || true
pkill -f 'validationservice.jar' 2>/dev/null || true
pkill -f 'paymentservice.jar' 2>/dev/null || true
pkill -f 'liberty:run' 2>/dev/null || true
sleep 2

nohup bash "${DEMO_HOME}/config/security/start-order-service.sh" >"${STATE_DIR}/logs/order-service.log" 2>&1 &
sleep 30
nohup java -DDIFC_ENABLED=false -jar "${DEMO_HOME}/stock-service/target/stockservice.jar" \
  >"${STATE_DIR}/logs/stock-service.log" 2>&1 &
nohup java -DDIFC_ENABLED=false -jar "${DEMO_HOME}/validation-service/target/validationservice.jar" \
  >"${STATE_DIR}/logs/validation-service.log" 2>&1 &
nohup java -DDIFC_ENABLED=false -jar "${DEMO_HOME}/payment-service/target/paymentservice.jar" \
  >"${STATE_DIR}/logs/payment-service.log" 2>&1 &
sleep 10

curl -sf -X POST "http://localhost:9080/api/order" \
  -H 'Content-Type: application/json' \
  -d '{"customerId":1,"productId":42,"amount":2,"cardNumber":"4111111111111111"}'
echo
sleep 15
grep -E 'Published|Billing event consumed|stock-check|validation|billing' "${STATE_DIR}/logs/"*.log | tail -20
echo "Baseline logs: ${STATE_DIR}/logs"
