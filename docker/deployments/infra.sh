#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"
cd "$PROJECT_ROOT/docker"

echo "🛑 Stopping all running services and core infrastructure..."
docker compose down
echo "🚀 Spinning up core Docker infrastructure stacks..."
docker compose up -d
echo "📡 Waiting for database provisioning to finish..."
while [ "$(docker inspect --format='{{.State.Status}}' docker-pg-db-init-1 2>/dev/null)" != "exited" ]; do
    printf "."
    sleep 1
done
INIT_EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' docker-pg-db-init-1)
if [ "$INIT_EXIT_CODE" != "0" ]; then
    echo -e "\n❌ FATAL: pg-db-init failed with exit code $INIT_EXIT_CODE."
    exit 1
fi
bash "$PROJECT_ROOT/docker/deployments/infra-init.sh"
echo -e "\n✅ PostgreSQL data layer fully provisioned!"
echo "⚙️ Executing Liquibase schema migrations..."
cd "$PROJECT_ROOT"
mvn liquibase:update -pl history-service
