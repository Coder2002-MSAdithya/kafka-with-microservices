#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
TOPICS=(
  ORDER_EVENT_TOPIC
  STOCK_CHECK_EVENT_TOPIC
  VALIDATION_EVENT_TOPIC
  BILLING_EVENT_TOPIC
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic ${topic}"
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BS}" \
    --command-config "${ADMIN_CONFIG}" \
    --create --if-not-exists --topic "${topic}" --partitions 3 --replication-factor 2 || true
done

echo "Topics ready."
