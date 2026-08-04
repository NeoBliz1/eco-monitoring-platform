#!/usr/bin/env bash
set -uo pipefail

# This script can be run automatically by traps, or manually to flush frozen contexts
echo -e "\n🛑 Initiating graceful teardown of polyglot platform..."

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../.." || exit 1
PROJECT_ROOT="$(pwd)"

# ----------------------------------------------------
# 1. Forcefully kill background loop workers and orchestration scripts
# ----------------------------------------------------
echo "⚙️ Scanning and terminating background orchestration loops..."
pkill -9 -f "infra-init.sh" 2>/dev/null || true
pkill -9 -f "infra.sh" 2>/dev/null || true
pkill -9 -f "service-java.sh" 2>/dev/null || true
pkill -9 -f "service-go.sh" 2>/dev/null || true
pkill -9 -f "org.codehaus.plexus.classworlds.launcher.Launcher" 2>/dev/null || true  # Targets active Maven builds
pkill -9 -f "go build" 2>/dev/null || true
# ----------------------------------------------------
# 2. Terminate running host binaries (Java apps & Go gateway)
# ----------------------------------------------------
echo "📡 Terminating active host application runtimes..."
pkill -15 -f "ingestion-service.jar" 2>/dev/null || true
pkill -15 -f "analysis-service.jar" 2>/dev/null || true
pkill -15 -f "history-service.jar" 2>/dev/null || true
pkill -15 -f "go-service" 2>/dev/null || true

# Give services a brief moment to release ports and file locks gracefully
sleep 2

# ----------------------------------------------------
# 3. Spin down the foundational Docker network stack and clear volumes
# ----------------------------------------------------
echo "🐳 Spinning down foundational Docker infrastructure stacks..."
if [ -d "$PROJECT_ROOT/docker" ]; then
    cd "$PROJECT_ROOT/docker" || exit 1
    # Runs compose down with volume flushes to guarantee a pristine state for the next run
    docker compose down -v 2>/dev/null || docker compose down
fi

echo "✨ Platform ecosystem teardown complete!"
