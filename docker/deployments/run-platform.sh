#!/usr/bin/env bash

# 🛑 Exit immediately if any command fails, treats unset variables as an error
set -euo pipefail

echo "🚀 Step 1: Spinning up core Docker infrastructure stacks..."
# Boots up the container mesh (-d flags it to run safely in the background)
cd "$(dirname "$0")/.."
docker compose down
docker compose up -d

echo "📡 Step 2: Waiting for database provisioning to finish..."
# 🔄 Loops cleanly until 'docker-pg-db-init-1' transitions to the lowercase 'exited' state
while [ "$(docker inspect --format='{{.State.Status}}' docker-pg-db-init-1 2>/dev/null)" != "exited" ]; do
    printf "."
    sleep 1
done

# 🚨 Safety check: verify the container exited with code 0 (Success) instead of crashing
INIT_EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' docker-pg-db-init-1)
if [ "$INIT_EXIT_CODE" != "0" ]; then
    echo -e "\n❌ FATAL: pg-db-init failed with exit code $INIT_EXIT_CODE. Check container logs!"
    exit 1
fi

echo -e "\n✅ PostgreSQL application data layer and users are fully provisioned!"

echo "⚙️ Step 3: Executing Liquibase schema migrations via host Maven reactor..."
cd ../
mvn liquibase:update -pl history-service

echo "🌟 Platform infrastructure initialization completed successfully!"
