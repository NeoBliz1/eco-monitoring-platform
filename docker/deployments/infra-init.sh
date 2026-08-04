#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"
TARGET_ENV_FILE="$PROJECT_ROOT/docker/.env"

# Source the configuration file context to resolve credentials
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
echo "📡 [Init Worker] Waiting for database provisioning to finish..."
while [ "$(docker inspect --format='{{.State.Status}}' docker-pg-db-init-1 2>/dev/null)" != "exited" ]; do
    sleep 1
done

INIT_EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' docker-pg-db-init-1)
if [ "$INIT_EXIT_CODE" != "0" ]; then
    echo "❌ FATAL: pg-db-init container failed with exit code $INIT_EXIT_CODE"
    exit 1
fi
echo "✅ [Init Worker] PostgreSQL data layer fully provisioned!"

# Core service discovery registration endpoint
CONSUL_URL="http://localhost:8500/v1/agent/service/register"

echo "📡 [Init Worker] Announcing ClickHouse Analytics Engine to Consul Discovery catalog..."
curl --silent --request PUT --url "${CONSUL_URL}" --header 'Content-Type: application/json' \
  --data "{\"ID\":\"clickhouse-analytics-1\",\"Name\":\"clickhouse\",\"Tags\":[\"olap-storage\"],\"Address\":\"clickhouse-db\",\"Port\":8123}"

# ----------------------------------------------------
# 🎛️ AUTOMATED GRAFANA PROVISIONING SYSTEM
# ----------------------------------------------------
GRAFANA_DATA_DIR="$PROJECT_ROOT/.data/grafana/data"

echo "⏳ [Init Worker] Run data permissions check"
CURRENT_OWNER_UID=$(stat -c '%u' "$GRAFANA_DATA_DIR")

if [ "$CURRENT_OWNER_UID" != "472" ]; then
    echo "⚠️ Ownership mismatch detected on Grafana storage layer (Current UID: $CURRENT_OWNER_UID)."
    echo "🔑 Requesting superuser authentication to align permissions to Grafana user context (UID 472)..."
    sudo chown -R 472:472 "$GRAFANA_DATA_DIR"
    echo "✅ Permissions aligned successfully!"
else
    echo "✅ Grafana storage layer ownership verified (UID 472 matches perfectly)."
fi

echo "⏳ [Init Worker] Waiting for Grafana Telemetry Portal API to boot..."
until curl --output /dev/null --silent --head --fail "http://localhost:3000/api/health"; do
    printf "."
    sleep 1
done
echo "✅ [Init Worker] Grafana engine online! Running automated API initialization..."

# Create a secure temporary file to capture the raw JSON response payload
RESPONSE_FILE=$(mktemp)

# Execute curl, piping the status code directly into the variable while writing the body text to disk
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

# Read the raw response text from the temporary file allocation node
RESPONSE_BODY=$(cat "$RESPONSE_FILE")
rm -f "$RESPONSE_FILE" # Clean up the temporary file immediately

if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ [Init Worker] ClickHouse data source successfully linked inside Grafana!"
    echo "📦 Response Body: $RESPONSE_BODY"
elif [ "$HTTP_STATUS" = "409" ]; then
    echo "ℹ️ [Init Worker] ClickHouse data source link already exists. Skipping allocation."
    echo "📦 Response Body: $RESPONSE_BODY"
else
    echo "❌ FATAL: Grafana API returned unexpected status code: $HTTP_STATUS"
    echo "📄 Grafana API Response Body: $RESPONSE_BODY"
    exit 1
fi

# ==============================================================================
# Injecting the Unified Polyglot Master Log Stream Dashboard
# ==============================================================================
echo "📊 [Init Worker] Injecting unified side-by-side master log dashboard..."

DASHBOARD_FILE=$(mktemp)

DASHBOARD_STATUS=$(curl --silent --show-error \
  --request POST \
  --url "http://localhost:3000/api/dashboards/db" \
  --user "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
  --header 'Content-Type: application/json' \
  --write-out "%{http_code}" \
  --output "$DASHBOARD_FILE" \
  --data-binary @- <<EOF
{
  "dashboard": {
    "id": null,
    "uid": "eco-platform-master-logs",
    "title": "📜 Eco Platform Master Log Stream Matrix",
    "tags": ["telemetry", "production", "mesh"],
    "timezone": "browser",
    "schemaVersion": 39,
    "panels": [
      {
        "id": 1,
        "type": "logs",
        "title": "🌱 Ingestion Service Logs",
        "gridPos": { "h": 11, "w": 12, "x": 0, "y": 0 },
        "targets": [
          {
            "datasource": { "type": "grafana-clickhouse-datasource", "uid": "ClickHouse-Ecosystem" },
            "format": "logs",
            "queryType": "logs",
            "rawSql": "SELECT timestamp, level, message, thread_name, logger_name, trace_id FROM ${CLICKHOUSE_DB}.application_logs WHERE service_name = 'eco-monitoring-ingestion' AND timestamp >= fromUnixTimestamp(intDiv(\$__from, 1000)) AND timestamp <= fromUnixTimestamp(intDiv(\$__to, 1000)) ORDER BY timestamp DESC LIMIT 1000",
            "meta": { "timeColumn": "timestamp", "levelColumn": "level", "messageColumn": "message" }
          }
        ],
        "options": {
          "dedupStrategy": "numbers",
          "enableInfiniteScrolling": true,
          "enableLogDetails": true,
          "prettifyLogMessage": true,
          "showControls": true,
          "showFieldSelector": true,
          "showLevel": true,
          "showTime": true,
          "sortOrder": "Descending",
          "wrapLogMessage": true
        }
      },
      {
        "id": 2,
        "type": "logs",
        "title": "📊 Analysis Service Logs",
        "gridPos": { "h": 11, "w": 12, "x": 12, "y": 0 },
        "targets": [
          {
            "datasource": { "type": "grafana-clickhouse-datasource", "uid": "ClickHouse-Ecosystem" },
            "format": "logs",
            "queryType": "logs",
            "rawSql": "SELECT timestamp, level, message, thread_name, logger_name, trace_id FROM ${CLICKHOUSE_DB}.application_logs WHERE service_name = 'eco-monitoring-analysis' AND timestamp >= fromUnixTimestamp(intDiv(\$__from, 1000)) AND timestamp <= fromUnixTimestamp(intDiv(\$__to, 1000)) ORDER BY timestamp DESC LIMIT 1000",
            "meta": { "timeColumn": "timestamp", "levelColumn": "level", "messageColumn": "message" }
          }
        ],
        "options": {
          "dedupStrategy": "numbers",
          "enableInfiniteScrolling": true,
          "enableLogDetails": true,
          "prettifyLogMessage": true,
          "showControls": true,
          "showFieldSelector": true,
          "showLevel": true,
          "showTime": true,
          "sortOrder": "Descending",
          "wrapLogMessage": true
        }
      },
      {
        "id": 3,
        "type": "logs",
        "title": "📜 History Service Logs",
        "gridPos": { "h": 11, "w": 12, "x": 0, "y": 11 },
        "targets": [
          {
            "datasource": { "type": "grafana-clickhouse-datasource", "uid": "ClickHouse-Ecosystem" },
            "format": "logs",
            "queryType": "logs",
            "rawSql": "SELECT timestamp, level, message, thread_name, logger_name, trace_id FROM ${CLICKHOUSE_DB}.application_logs WHERE service_name = 'eco-monitoring-history' AND timestamp >= fromUnixTimestamp(intDiv(\$__from, 1000)) AND timestamp <= fromUnixTimestamp(intDiv(\$__to, 1000)) ORDER BY timestamp DESC LIMIT 1000",
            "meta": { "timeColumn": "timestamp", "levelColumn": "level", "messageColumn": "message" }
          }
        ],
        "options": {
          "dedupStrategy": "numbers",
          "enableInfiniteScrolling": true,
          "enableLogDetails": true,
          "prettifyLogMessage": true,
          "showControls": true,
          "showFieldSelector": true,
          "showLevel": true,
          "showTime": true,
          "sortOrder": "Descending",
          "wrapLogMessage": true
        }
      },
      {
        "id": 4,
        "type": "logs",
        "title": "🐹 Go API Gateway Proxy Logs",
        "gridPos": { "h": 11, "w": 12, "x": 12, "y": 11 },
        "targets": [
          {
            "datasource": { "type": "grafana-clickhouse-datasource", "uid": "ClickHouse-Ecosystem" },
            "format": "logs",
            "queryType": "logs",
            "rawSql": "SELECT timestamp, level, message, thread_name, logger_name, trace_id FROM ${CLICKHOUSE_DB}.application_logs WHERE service_name = 'go-service' AND timestamp >= fromUnixTimestamp(intDiv(\$__from, 1000)) AND timestamp <= fromUnixTimestamp(intDiv(\$__to, 1000)) ORDER BY timestamp DESC LIMIT 1000",
            "meta": { "timeColumn": "timestamp", "levelColumn": "level", "messageColumn": "message" }
          }
        ],
        "options": {
          "dedupStrategy": "numbers",
          "enableInfiniteScrolling": true,
          "enableLogDetails": true,
          "prettifyLogMessage": true,
          "showControls": true,
          "showFieldSelector": true,
          "showLevel": true,
          "showTime": true,
          "sortOrder": "Descending",
          "wrapLogMessage": true
        }
      }
    ],
    "time": { "from": "now-24h", "to": "now" },
    "refresh": "5s"
  },
  "overwrite": true
}
EOF
)

DASHBOARD_BODY=$(cat "$DASHBOARD_FILE")
rm -f "$DASHBOARD_FILE"

if [ "$DASHBOARD_STATUS" = "200" ]; then
    echo "🎉 [Init Worker] Consolidated side-by-side Master Dashboard provisioned successfully!"
else
    echo "❌ FATAL: Dashboard injection failed! Status code: $DASHBOARD_STATUS"
    echo "📄 API Error: $DASHBOARD_BODY"
    exit 1
fi

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
