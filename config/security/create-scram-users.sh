#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
CONFIGS="${KAFKA_HOME}/bin/kafka-configs.sh"

create_user() {
  local name="$1"
  local password="$2"
  echo "Creating SCRAM user: ${name}"
  "${CONFIGS}" --bootstrap-server "${BS}" --command-config "${ADMIN_CONFIG}" \
    --alter --add-config "SCRAM-SHA-256=[password=${password}]" \
    --entity-type users --entity-name "${name}" 2>/dev/null || true
}

"${EXAMPLES_SECURITY}/create-scram-users.sh"

create_user order-svc order-svc-secret
create_user stock-svc stock-svc-secret
create_user validation-svc validation-svc-secret
create_user payment-svc payment-svc-secret

echo "JUG Istanbul SCRAM users ready."
