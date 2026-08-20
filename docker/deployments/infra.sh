#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"
cd "$PROJECT_ROOT/docker"

SKIP_INFRA_BOOT=false

for arg in "$@"; do
    if [ "$arg" == "-ddi" ]; then
        # shellcheck disable=SC2034
        SKIP_INFRA_BOOT=true
    fi
done

if [ "$SKIP_INFRA_BOOT" = false ]; then
    echo "🚀 Spinning up core Docker infrastructure stacks..."
    docker compose up -d
else
    echo "⚡ Keeping active docker context alive. Skipping docker compose up task..."
fi
echo "📡 Waiting for database provisioning to finish..."
while [ "$(docker inspect --format='{{.State.Status}}' docker-pg-db-init-1 2>/dev/null)" != "exited" ]; do
    printf ".wat docker-pg-db-init-1."
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

VAULT_INTERNAL_TOKEN=$(docker run --rm -v docker_vault_tokens:/tmp/tokens redis:8.8.0-alpine cat /tmp/tokens/validator_token 2>/dev/null | tr -d ' \n\r' || echo "")
POSTGRES_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/postgres" || echo "")
if [ -z "$POSTGRES_SECRETS" ]; then
    echo "❌ FATAL: Vault response is empty."
    exit 1
fi
echo "🔑 Extracting database credentials using native string parsing..."
LIQUIBASE_COMMAND_USERNAME=$(echo "$POSTGRES_SECRETS" | grep -o '"eco_user_name":"[^"]*' | sed 's/"eco_user_name":"//')
LIQUIBASE_COMMAND_PASSWORD=$(echo "$POSTGRES_SECRETS" | grep -o '"eco_user_password":"[^"]*' | sed 's/"eco_user_password":"//')
if [ -z "$LIQUIBASE_COMMAND_USERNAME" ] || [ -z "$LIQUIBASE_COMMAND_PASSWORD" ]; then
    echo "❌ FATAL: Failed to parse eco_user_name or eco_user_password from payload."
    exit 1
fi
ENV_PAYLOAD=(
    "LIQUIBASE_COMMAND_USERNAME=$LIQUIBASE_COMMAND_USERNAME"
    "LIQUIBASE_COMMAND_PASSWORD=$LIQUIBASE_COMMAND_PASSWORD"
)
echo "🚀 Running Liquibase migrations for history-service..."
env "${ENV_PAYLOAD[@]}" mvn liquibase:update -pl history-service
