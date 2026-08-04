#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../.."
PROJECT_ROOT="$(pwd)"

# Define the local submodule directory inside platform root
GATEWAY_SUBMODULE_DIR="$PROJECT_ROOT/gateway-service"
GATEWAY_REPO_URL="https://github.com/NeoBliz1/eco-platform-api-gateway.git"

cd "$PROJECT_ROOT"
pwd
# 🔍 Check if the gateway-service submodule is already tracked by Git
if git config --file .gitmodules --get "submodule.gateway-service.path" &>/dev/null; then
    echo "📦 [Go Worker] Submodule 'gateway-service' already exists. Synchronizing and updating..."
    # Safely initialize and update the existing submodule directory state
    git submodule update --init --recursive --remote gateway-service
else
    echo "📥 [Go Worker] Submodule missing. Registering and downloading 'gateway-service'..."
    # Ensure any untracked or leftover folder is safely cleared before adding
    if [ -d "$GATEWAY_SUBMODULE_DIR" ]; then
        echo "⚠️ Warning: Found an unindexed directory at $GATEWAY_SUBMODULE_DIR. Removing it to prevent Git conflicts..."
        rm -rf "$GATEWAY_SUBMODULE_DIR"
    fi

    # Execute the submodule attachment
    git submodule add "$GATEWAY_REPO_URL" gateway-service
    git submodule update --init --recursive gateway-service
fi

# 🚨 Final verification check before compiling
if [ ! -d "$GATEWAY_SUBMODULE_DIR" ] || [ ! -f "$GATEWAY_SUBMODULE_DIR/go.mod" ]; then
    echo "❌ FATAL: Gateway service source code could not be verified inside $GATEWAY_SUBMODULE_DIR"
    exit 1
fi

echo "🐹 [Go Worker] Compiling high-performance API Gateway binary from source..."
cd "$GATEWAY_SUBMODULE_DIR"
mkdir -p "$PROJECT_ROOT/bin"

go build -v -o "$PROJECT_ROOT/bin/go-service"
