#!/usr/bin/env bash
set -e

VAULT_INTERNAL_TOKEN=$(docker run --rm -v docker_vault_tokens:/tmp/tokens redis:8.8.0-alpine cat /tmp/tokens/validator_token 2>/dev/null | tr -d ' \n\r' || echo "")

if [ -n "$VAULT_INTERNAL_TOKEN" ]; then
    echo "✅ [Log Cleaner] Successfully extracted validator token from named volume storage layer."
else
    echo "❌ FATAL: Unable to extract validator token from persistent vault_tokens volume layer."
    exit 1
fi

echo "🔐 [Log Cleaner] Fetching ClickHouse credentials from Vault..."
CLICKHOUSE_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/clickhouse" || echo "")

CLICKHOUSE_USER=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_user":"[^"]*' | grep -o '[^"]*$')
CLICKHOUSE_PASSWORD=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_password":"[^"]*' | grep -o '[^"]*$')
CLICKHOUSE_DB=$(echo "$CLICKHOUSE_SECRETS" | grep -o '"clickhouse_db":"[^"]*' | grep -o '[^"]*$')

if [ -z "$CLICKHOUSE_USER" ] || [ -z "$CLICKHOUSE_PASSWORD" ] || [ -z "$CLICKHOUSE_DB" ]; then
    echo "❌ FATAL: Failed to parse required ClickHouse engine configurations from Vault payload."
    exit 1
fi

TARGET_TABLE="application_logs"

echo "🧹 === WIPING APP LOGS ==="
docker exec -i docker-clickhouse-db-1 clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --query="TRUNCATE TABLE IF EXISTS ${TARGET_TABLE};"

echo "📊 === VERIFYING PURGE (SHOULD BE EMPTY) ==="
docker exec -i docker-clickhouse-db-1 clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --query="SELECT * FROM ${TARGET_TABLE};"

echo "✅ [Log Cleaner] Data management operation successfully completed!"
