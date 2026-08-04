#!/usr/bin/env bash
set -euo pipefail

MODULE_NAME=$1
SKIP_TESTS=${2:-false}

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../.."
PROJECT_ROOT="$(pwd)"

cd "$PROJECT_ROOT"

echo "⚙️ Compiling and packaging Java module: $MODULE_NAME..."
MAVEN_OPTS=""
if [ "$SKIP_TESTS" = true ]; then
    MAVEN_OPTS="-DskipTests"
fi

mvn clean package $MAVEN_OPTS -pl "$MODULE_NAME" -am
