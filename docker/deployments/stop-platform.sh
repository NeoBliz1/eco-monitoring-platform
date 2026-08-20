#!/usr/bin/env bash
set -uo pipefail

echo -e "\n🛑 Initiating graceful teardown of polyglot platform..."

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../.." || exit 1
PROJECT_ROOT="$(pwd)"

echo "⚙️ Scanning and terminating background orchestration loops..."
pkill -9 -f "infra-init.sh" 2>/dev/null || true
pkill -9 -f "infra.sh" 2>/dev/null || true
pkill -9 -f "service-java.sh" 2>/dev/null || true
pkill -9 -f "service-go.sh" 2>/dev/null || true
pkill -9 -f "org.codehaus.plexus.classworlds.launcher.Launcher" 2>/dev/null || true  # Targets active Maven builds
pkill -9 -f "go build" 2>/dev/null || true

sleep 2

pkill -15 -f "ingestion-service.jar" 2>/dev/null || true
pkill -15 -f "analysis-service.jar" 2>/dev/null || true
pkill -15 -f "history-service.jar" 2>/dev/null || true
pkill -15 -f "go-service" 2>/dev/null || true

if [ -d "$PROJECT_ROOT/docker" ]; then
    cd "$PROJECT_ROOT/docker" || exit 1

    VAULT_STATUS=$(docker compose ps --format '{{.Service}}:{{.State}}' | grep -E '^vault-server:' || echo "")
    CLICKHOUSE_STATUS=$(docker compose ps --format '{{.Service}}:{{.State}}' | grep -E '^clickhouse-db:' || echo "")

    if [[ "$VAULT_STATUS" =~ "running" ]] && [[ "$CLICKHOUSE_STATUS" =~ "running" ]]; then
        echo "🧹 [Teardown] Both Vault and ClickHouse are active. Running log flush sequence..."
        bash "$PROJECT_ROOT/docker/deployments/clear-logs.sh"
        echo "✅ Apps logs cleared"
    else
        echo "ℹ️ [Teardown] Target backend storage layers are offline or stopping. Skipping database log wipe."
    fi
fi

echo "🐳 Spinning down foundational Docker infrastructure stacks..."
if [ -d "$PROJECT_ROOT/docker" ]; then
    cd "$PROJECT_ROOT/docker" || exit 1
    docker compose down -v
fi

echo "✨ Platform ecosystem teardown complete!"
