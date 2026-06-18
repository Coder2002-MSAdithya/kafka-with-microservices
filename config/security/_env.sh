#!/usr/bin/env bash
# Shared paths for JUG Istanbul kafka-with-microservices DIFC workflow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-$(cd "${DEMO_HOME}/../difc-for-kafka" && pwd)}"
EXAMPLES_SECURITY="$(cd "${DEMO_HOME}/../kafka-streams-examples/config/security" && pwd)"

export POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS="${POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS:-}"
export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092,localhost:9094}"
export STATE_DIR="${STATE_DIR:-/tmp/jug-kafka-demo}"
export DIFC_POLICY_REGISTRY_DIR="${DIFC_POLICY_REGISTRY_DIR:-${STATE_DIR}/policy}"
export DIFC_ENABLED="${DIFC_ENABLED:-true}"
export DIFC_RESET_CLUSTER="${DIFC_RESET_CLUSTER:-true}"

CONTROLLER_CONFIG="${EXAMPLES_SECURITY}/controller.properties"
BROKER1_CONFIG="${EXAMPLES_SECURITY}/broker.node1.properties"
BROKER2_CONFIG="${EXAMPLES_SECURITY}/broker.node2.properties"
ADMIN_CONFIG="${EXAMPLES_SECURITY}/admin-client.properties"
JAAS_CONFIG="${EXAMPLES_SECURITY}/kafka_server_jaas.conf"
export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}"

policy_agent_jar() {
  local module_jar="${KAFKA_HOME}/security/policy-agent/build/libs/policy-agent-1.0.0.jar"
  if [[ -f "${module_jar}" ]]; then
    echo "${module_jar}"
  else
    echo "${KAFKA_HOME}/build/libs/policy-agent-1.0.0.jar"
  fi
}

policy_trusted_ca_path() {
  local mkcert_ca="${KAFKA_HOME}/security/policy-agent/mkcert-ca/rootCA.pem"
  if [[ -f "${mkcert_ca}" ]]; then
    echo "${mkcert_ca}"
  else
    echo "${KAFKA_HOME}/security/policy-agent/src/main/resources/trusted-ca.pem"
  fi
}

grantor_policy_trust_java_opts() {
  local trusted_ca
  trusted_ca="$(policy_trusted_ca_path)"
  cat <<EOF
-Dpolicy.grantor.trusted.ca.path=${trusted_ca}
-Dpolicy.registry.dir=${STATE_DIR}/policy
-Dpolicy.agent.kafka.bootstrap=${BOOTSTRAP_SERVERS}
EOF
}

policy_agent_java_opts() {
  local principal="$1"
  local app_id="$2"
  local jar trusted_ca
  jar="$(policy_agent_jar)"
  trusted_ca="$(policy_trusted_ca_path)"
  local policy_dir="${STATE_DIR}/policy/${principal}"
  local signing_dir="${KAFKA_HOME}/security/policy-agent/policy-signing"
  mkdir -p "${policy_dir}"
  cat <<EOF
-javaagent:${jar}
-Dpolicy.agent.network.enforcement=false
-Dpolicy.agent.allowed.external.hosts=${POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS}
-Dpolicy.agent.trusted.ca.path=${trusted_ca}
-Dpolicy.grantor.trusted.ca.path=${trusted_ca}
-Dpolicy.agent.signing.key.path=${signing_dir}/policy-signing-key.pem
-Dpolicy.agent.signing.cert.path=${signing_dir}/policy-signing-cert.pem
-Dpolicy.app.principal=${principal}
-Dpolicy.app.id=${app_id}
-Dpolicy.registry.dir=${STATE_DIR}/policy
-Dpolicy.dsl.json.path=${policy_dir}/processing-policy.json
-Dpolicy.dsl.dot.path=${policy_dir}/dsl-topology.dot
EOF
}

require_jars() {
  mvn -q -f "${DEMO_HOME}/pom.xml" install -DskipTests
}
