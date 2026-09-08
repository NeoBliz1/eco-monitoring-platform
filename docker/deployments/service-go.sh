#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../.."
PROJECT_ROOT="$(pwd)"

GATEWAY_SUBMODULE_DIR="$PROJECT_ROOT/gateway-service"

cd "$PROJECT_ROOT"

echo "🧹 [Go Worker] Checking for submodule index corruption..."

if [ -d "$GATEWAY_SUBMODULE_DIR" ] && [ -f "$GATEWAY_SUBMODULE_DIR/go.mod" ]; then
    echo "✅ [Go Worker] Submodule directory and source code verified on disk. Skipping network execution."
else
    echo "📥 [Go Worker] Submodule files are missing or incomplete. Pulling from GitHub..."
    git submodule deinit -f gateway-service 2>/dev/null || true
    git rm --cached -f gateway-service 2>/dev/null || true
    rm -rf .git/modules/gateway-service
    rm -rf "$GATEWAY_SUBMODULE_DIR"
    git submodule update --init --recursive gateway-service
fi

if [ ! -d "$GATEWAY_SUBMODULE_DIR" ] || [ ! -f "$GATEWAY_SUBMODULE_DIR/go.mod" ]; then
    echo "❌ FATAL: Gateway service source code could not be verified inside $GATEWAY_SUBMODULE_DIR"
    exit 1
fi

echo "🚀 SUCCESS: Workspace synchronization complete."

echo "🐹 [Go Worker] Compiling high-performance API Gateway binary from source..."
cd "$GATEWAY_SUBMODULE_DIR"
mkdir -p "$PROJECT_ROOT/bin"
GATEWAY_DIR="$PROJECT_ROOT/bin/gateway"
mkdir -p "$GATEWAY_DIR"
cat "$GATEWAY_SUBMODULE_DIR/.env" > "$GATEWAY_DIR/.env"
go build -v -o "$GATEWAY_DIR/go-service"

echo "✅ [Go Worker] Compilation complete. Binary available at bin/go-service"
