#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"
TARGET_ENV_FILE="$PROJECT_ROOT/docker/.env"

if [ -f "$TARGET_ENV_FILE" ]; then
    # shellcheck disable=SC2046
    export $(grep -v '^#' "$TARGET_ENV_FILE" | grep -v '^$' | xargs)
else
    echo "❌ FATAL: Configuration profile missing at: $TARGET_ENV_FILE"
    exit 1
fi

echo "🔄 [Init Worker] Starting data plane verification and catalog registration loop..."

# ----------------------------------------------------
# 🔍 POSTGRES PROVISIONING CHECK
# ----------------------------------------------------
INIT_EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' docker-pg-db-init-1)
if [ "$INIT_EXIT_CODE" != "0" ]; then
    echo "❌ FATAL: pg-db-init container failed with exit code $INIT_EXIT_CODE"
    exit 1
fi
echo "✅ [Init Worker] PostgreSQL data layer fully provisioned!"

CONSUL_URL="http://localhost:8500/v1/agent/service/register"

echo "📡 [Init Worker] Announcing ClickHouse Analytics Engine to Consul Discovery catalog..."
curl --silent --request PUT --url "${CONSUL_URL}" --header 'Content-Type: application/json' \
  --data '{
    "ID": "clickhouse-analytics-1",
    "Name": "clickhouse-db",
    "Tags": ["olap-storage"],
    "Address": "clickhouse-db",
    "Port": 8123,
    "Check": {
      "TCP": "clickhouse-db:8123",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 [Init Worker] Announcing PostgreSQL Metrics Exporter to Consul Discovery catalog..."
curl --silent --request PUT --url "${CONSUL_URL}" --header 'Content-Type: application/json' \
  --data '{
    "ID": "postgres-exporter-1",
    "Name": "postgres-exporter",
    "Tags": ["metrics", "prometheus"],
    "Address": "postgres-exporter",
    "Port": 9187,
    "Check": {
      "TCP": "postgres-exporter:9187",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 [Init Worker] Announcing Jaeger Distributed Tracing Engine to Consul Discovery catalog..."
curl --silent --request PUT --url "${CONSUL_URL}" --header 'Content-Type: application/json' \
  --data '{
    "ID": "jaeger-tracing-1",
    "Name": "jaeger-tracing",
    "Tags": ["telemetry", "tracing"],
    "Address": "jaeger-tracing",
    "Port": 16686,
    "Check": {
      "TCP": "jaeger-tracing:16686",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 [Init Worker] Announcing Prometheus TSDB Server to Consul Discovery catalog..."
curl --silent --request PUT --url "${CONSUL_URL}" --header 'Content-Type: application/json' \
  --data '{
    "ID": "prometheus-server-1",
    "Name": "prometheus-server",
    "Tags": ["monitoring", "metrics"],
    "Address": "prometheus-server",
    "Port": 9090,
    "Check": {
      "TCP": "prometheus-server:9090",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

# ----------------------------------------------------
# 🛡️ 1. EXTRACT VAULT INTERNAL TOKEN FROM STORAGE LAYER
# ----------------------------------------------------
VAULT_INTERNAL_TOKEN=$(docker run --rm -v docker_vault_tokens:/tmp/tokens redis:8.8.0-alpine cat /tmp/tokens/validator_token 2>/dev/null | tr -d ' \n\r' || echo "")

if [ -n "$VAULT_INTERNAL_TOKEN" ]; then
    echo "✅ [Init Worker] Successfully extracted validator token natively from named volume storage layer."
else
    echo "❌ FATAL: Unable to extract validator token from persistent vault_tokens volume layer."
    exit 1
fi

# ----------------------------------------------------
# 🔑 2. FETCH SECRETS FROM VAULT REST API
# ----------------------------------------------------
echo "🔐 [Init Worker] Fetching platform engine credentials from Vault..."

GRAFANA_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/grafana" || echo "")
CLICKHOUSE_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/clickhouse" || echo "")
PROMETHEUS_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/prometheus" || echo "")
# ----------------------------------------------------
# 🏷️ 3. PARSE RAW JSON PAYLOADS INTO BASH VARIABLES
# ----------------------------------------------------
GRAFANA_ADMIN_USER=$(echo "$GRAFANA_SECRETS" | grep -o '"admin_user":"[^"]*' | grep -o '[^"]*$')
GRAFANA_ADMIN_PASSWORD=$(echo "$GRAFANA_SECRETS" | grep -o '"admin_password":"[^"]*' | grep -o '[^"]*$')
CLICKHOUSE_USER=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_user":"[^"]*' | grep -o '[^"]*$')
CLICKHOUSE_PASSWORD=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_password":"[^"]*' | grep -o '[^"]*$')
CLICKHOUSE_DB=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_db":"[^"]*' | grep -o '[^"]*$')
PROM_AUTH_USER=$(echo "$PROMETHEUS_SECRETS" | grep -o '"user":"[^"]*' | grep -o '[^"]*$')
PROM_AUTH_PASS=$(echo "$PROMETHEUS_SECRETS" | grep -o '"password":"[^"]*' | grep -o '[^"]*$')

if [ -z "$GRAFANA_ADMIN_USER" ] || [ -z "$CLICKHOUSE_USER" ] || [ -z "$PROM_AUTH_USER" ]; then
    echo "❌ FATAL: Failed to parse required cluster engine configuration values from Vault."
    exit 1
fi

# ----------------------------------------------------
# 🎛️ AUTOMATED GRAFANA PROVISIONING SYSTEM
# ----------------------------------------------------
echo "⏳ [Init Worker] Waiting for Grafana Telemetry Portal API to boot..."
until curl --output /dev/null --silent --head --fail "http://localhost:3000/api/health"; do
    printf "...Waiting for Grafana Telemetry Portal API to boot..."
    sleep 1
done
echo "✅ [Init Worker] Grafana engine online! Running automated API initialization..."

RESPONSE_FILE=$(mktemp)

# ==============================================================================
# 🛢️ STEP 4: PROVISION CLICKHOUSE DATA SOURCE (Logs)
# ==============================================================================
HTTP_STATUS=$(curl --silent --show-error \
  --request POST \
  --url "http://localhost:3000/api/datasources" \
  --user "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
  --header 'Content-Type: application/json' \
  --write-out "%{http_code}" \
  --output "$RESPONSE_FILE" \
  --data-binary @- <<EOF
{
  "name": "ClickHouse-Ecosystem",
  "type": "grafana-clickhouse-datasource",
  "access": "proxy",
  "jsonData": {
    "server": "clickhouse-db",
    "port": 8123,
    "protocol": "http",
    "username": "${CLICKHOUSE_USER}",
    "defaultDatabase": "${CLICKHOUSE_DB}"
  },
  "secureJsonData": {
    "password": "${CLICKHOUSE_PASSWORD}"
  },
  "isDefault": true
}
EOF
)

RESPONSE_BODY=$(cat "$RESPONSE_FILE")

if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ [Init Worker] ClickHouse data source successfully linked inside Grafana!"
elif [ "$HTTP_STATUS" = "409" ]; then
    echo "ℹ️ [Init Worker] ClickHouse data source link already exists. Skipping allocation."
else
    echo "❌ FATAL: Grafana API returned unexpected status code for ClickHouse: $HTTP_STATUS"
    echo "📄 Grafana API Response Body: $RESPONSE_BODY"
    rm -f "$RESPONSE_FILE"
    exit 1
fi

# ==============================================================================
# 📈 STEP 5: PROVISION PROMETHEUS DATA SOURCE WITH BASIC AUTH (Metrics)
# ==============================================================================
# Maps the data source target to prometheus-server (the container name on eco-net)
HTTP_STATUS=$(curl --silent --show-error \
  --request POST \
  --url "http://localhost:3000/api/datasources" \
  --user "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
  --header 'Content-Type: application/json' \
  --write-out "%{http_code}" \
  --output "$RESPONSE_FILE" \
  --data-binary @- <<EOF
{
  "name": "Prometheus-Metrics",
  "type": "prometheus",
  "access": "proxy",
  "url": "http://prometheus-server:9090",
  "jsonData": {
    "httpMethod": "POST",
    "manageAlerts": true,
    "basicAuth": true,
    "basicAuthUser": "${PROM_AUTH_USER}"
  },
  "secureJsonData": {
    "basicAuthPassword": "${PROM_AUTH_PASS}"
  },
  "isDefault": false
}
EOF
)

RESPONSE_BODY=$(cat "$RESPONSE_FILE")
rm -f "$RESPONSE_FILE"

if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ [Init Worker] Prometheus data source successfully linked inside Grafana!"
elif [ "$HTTP_STATUS" = "409" ]; then
    echo "ℹ️ [Init Worker] Prometheus data source link already exists. Skipping allocation."
else
    echo "❌ FATAL: Grafana API returned unexpected status code for Prometheus: $HTTP_STATUS"
    echo "📄 Grafana API Response Body: $RESPONSE_BODY"
    exit 1
fi

# ==============================================================================
# 🛠️ STEP 6: REUSABLE DASHBOARD PROVISIONER
# ==============================================================================
provision_grafana_dashboard() {
    local dashboard_name="$1"
    local source_file_path="$2"
    local compiled_file
    local response_file
    local http_status
    local response_body

    echo "📊 [Init Worker] Provisioning dashboard [${dashboard_name}] from file source..."

    if [ ! -f "$source_file_path" ]; then
        echo "❌ FATAL: Dashboard blueprint file not found at: $source_file_path"
        exit 1
    fi

    compiled_file=$(mktemp)
    response_file=$(mktemp)

    export CLICKHOUSE_DB
    # shellcheck disable=SC2016
    envsubst '${CLICKHOUSE_DB}' < "$source_file_path" > "$compiled_file"

    http_status=$(curl --silent --show-error \
      --request POST \
      --url "http://localhost:3000/api/dashboards/db" \
      --user "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
      --header 'Content-Type: application/json' \
      --write-out "%{http_code}" \
      --output "$response_file" \
      --data-binary "@$compiled_file"
    )

    response_body=$(cat "$response_file")
    rm -f "$compiled_file" "$response_file"

    if [ "$http_status" = "200" ]; then
        echo "✅ [Init Worker] Dashboard [${dashboard_name}] successfully provisioned!"
    else
        echo "❌ FATAL: Dashboard [${dashboard_name}] failed with HTTP Status: $http_status"
        echo "📄 API Error Response payload body: $response_body"
        exit 1
    fi
}

LAYOUTS_DIR="$PROJECT_ROOT/docker/deployments/grafana/layouts"

provision_grafana_dashboard "Master Log Stream Matrix" "$LAYOUTS_DIR/master_logs_dashboard.json"
provision_grafana_dashboard "Ingestion service performance Metrics Grid" "$LAYOUTS_DIR/ingestion_performance_dashboard.json"
provision_grafana_dashboard "Analysis service performance Metrics Grid" "$LAYOUTS_DIR/analysis_performance_dashboard.json"
provision_grafana_dashboard "History service performance Metrics Grid" "$LAYOUTS_DIR/history_performance_dashboard.json"
provision_grafana_dashboard "Api gateway service performance Metrics Grid" "$LAYOUTS_DIR/gateway_performance_dashboard.json"

echo "🎉 [Init Worker] Structured Telemetry Logs Dashboard provisioned!"

# Announce Grafana to Consul Discovery Catalog
echo "📡 [Init Worker] Announcing Grafana Visual Portal to Consul Discovery catalog..."
curl --silent --request PUT \
  --url "${CONSUL_URL}" \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "grafana-portal-1",
    "Name": "grafana",
    "Tags": ["visual-analytics", "monitoring-ui", "dashboard-engine"],
    "Address": "grafana",
    "Port": 3000,
    "Check": {
      "HTTP": "http://grafana:3000/api/health",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "✅ [Init Worker] Data configuration phase complete!"
