#!/bin/bash
set -euo pipefail

echo "📡 Uploading global shared platform properties to Consul KV..."
GLOBAL_CONTENT=$(cat /tmp/platform-global-properties.yaml)
consul kv put -http-addr="http://consul-server:8500" "config/application/data" "$GLOBAL_CONTENT"

echo "📡 Uploading service-specific properties for Api Gateway Service..."
GATEWAY_CONTENT=$(cat /tmp/platform-gateway-properties.yaml)
consul kv put -http-addr="http://consul-server:8500" "config/eco-monitoring-gateway/data" "$GATEWAY_CONTENT"

echo "📡 Uploading service-specific properties for Analysis Service..."
ANALYSIS_CONTENT=$(cat /tmp/platform-analysis-properties.yaml)
consul kv put -http-addr="http://consul-server:8500" "config/eco-monitoring-analysis/data" "$ANALYSIS_CONTENT"

echo "📡 Uploading service-specific properties for History Service..."
HISTORY_CONTENT=$(cat /tmp/platform-history-properties.yaml)
consul kv put -http-addr="http://consul-server:8500" "config/eco-monitoring-history/data" "$HISTORY_CONTENT"

echo "✅ All configurations loaded successfully into Consul KV."
