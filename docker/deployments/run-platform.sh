#!/usr/bin/env bash
set -euo pipefail

SKIP_TESTS=true
DEPLOY_HISTORY=false
DEPLOY_INGESTION=false
DEPLOY_ANALYSIS=false
DEPLOY_GATEWAY=false
DEBUG_INGESTION=false
DEBUG_ANALYSIS=false
DEBUG_HISTORY=false
SKIP_INFRA_TEARDOWN=false  # New state tracking parameter

for arg in "$@"; do
    case $arg in
        -help|--help|-h)
            echo "================================================================"
            echo "📜 Eco Platform Build & Deployment Helper Tool"
            echo "================================================================"
            echo "Usage: ./build.sh [options]"
            echo ""
            echo "Options:"
            echo "  -rt    Run tests during compilation (Disabled by default)"
            echo "  -ddi   Don't rerun docker infrastructure (Keep warm state alive)"
            echo "  -dhs   Deploy History Service exclusively"
            echo "  -dis   Deploy Ingestion Service exclusively"
            echo "  -das   Deploy Analysis Service exclusively"
            echo "  -dgs   Deploy Gateway Service exclusively"
            echo "  -xis   Enable Debugging profile on Ingestion Service"
            echo "  -xas   Enable Debugging profile on Analysis Service"
            echo "  -xhs   Enable Debugging profile on History Service"
            echo "  -help  Display this architectural routing guide"
            echo ""
            echo "Note: If no explicit service flag (-d...) is specified,"
            echo "      the engine defaults to compiling and deploying all modules."
            echo "================================================================"
            exit 0
            ;;
        -rt)  SKIP_TESTS=false ;;
        -ddi) SKIP_INFRA_TEARDOWN=true ;;
        -dhs) DEPLOY_HISTORY=true ;;
        -dis) DEPLOY_INGESTION=true ;;
        -das) DEPLOY_ANALYSIS=true ;;
        -dgs) DEPLOY_GATEWAY=true ;;
        -xis) DEBUG_INGESTION=true ;;
        -xas) DEBUG_ANALYSIS=true ;;
        -xhs) DEBUG_HISTORY=true ;;
        *)    echo "Unknown option: $arg (Use -help for valid targets)" && exit 1 ;;
    esac
done

if [ "$DEPLOY_HISTORY" = false ] && [ "$DEPLOY_INGESTION" = false ] && [ "$DEPLOY_ANALYSIS" = false ] && [ "$DEPLOY_GATEWAY" = false ]; then
    DEPLOY_HISTORY=true
    DEPLOY_INGESTION=true
    DEPLOY_ANALYSIS=true
    DEPLOY_GATEWAY=true
fi

SCRIPT_DIR="$(dirname "$0")"
CURR_DIR="$(pwd)"
cd "$SCRIPT_DIR/../.."
PROJECT_ROOT="$(pwd)"
cd "$CURR_DIR"

cleanup_gracefully() {
    # shellcheck disable=SC2317
    bash "$SCRIPT_DIR/stop-platform.sh"
    # shellcheck disable=SC2317
    exit 130
}
trap cleanup_gracefully SIGINT SIGTERM

if [ "$SKIP_INFRA_TEARDOWN" = false ]; then
    echo "🛑 Stopping all running services and core infrastructure..."
    bash "$PROJECT_ROOT/docker/deployments/stop-platform.sh"
else
    echo "♻️ Skipping environmental infrastructure teardown (-ddi active)..."
fi

echo "⚡ Starting Asynchronous Infrastructure Provisioning and Service Compilation..."

# ----------------------------------------------------
# THREAD 1: Start Docker infrastructure + Liquibase migrations
# ----------------------------------------------------
echo "🐳 [BACKGROUND] Booting Docker container infrastructure mesh via infra.sh..."
bash "$SCRIPT_DIR/infra.sh" "$@" &
INFRA_PID=$!

# ----------------------------------------------------
# THREAD 2: Start Service Compilation in background wrapper
# ----------------------------------------------------
echo "⚙️ [BACKGROUND] Launching Maven and Go compilation sequence..."
(
    if [ "$DEPLOY_INGESTION" = true ]; then bash "$SCRIPT_DIR/service-java.sh" "ingestion-service" "$SKIP_TESTS"; fi
    if [ "$DEPLOY_ANALYSIS" = true ]; then bash "$SCRIPT_DIR/service-java.sh" "analysis-service" "$SKIP_TESTS"; fi
    if [ "$DEPLOY_HISTORY" = true ]; then bash "$SCRIPT_DIR/service-java.sh" "history-service" "$SKIP_TESTS"; fi
    if [ "$DEPLOY_GATEWAY" = true ]; then bash "$SCRIPT_DIR/service-go.sh"; fi
) &
BUILD_PID=$!

# ----------------------------------------------------
# SYNCHRONIZATION POINT: Safe verification of background thread lifecycles
# ----------------------------------------------------
echo "⏳ Waiting for concurrent compilation and container setup threads to complete..."
wait $INFRA_PID || { echo "❌ FATAL: Infrastructure initialization or migrations failed!"; exit 1; }
wait $BUILD_PID || { echo "❌ FATAL: Compilation pipeline failed!"; exit 1; }

# ----------------------------------------------------
# Global deployment artifact verification check
# ----------------------------------------------------
echo "🔍 Verifying deployment artifact existence..."
MISSING_ARTIFACTS=0

INGESTION_JAR=$(find "$PROJECT_ROOT/ingestion-service/target" -maxdepth 1 -name "ingestion-service-*.jar" ! -name "*.original" 2>/dev/null | head -n 1 || echo "")
ANALYSIS_JAR=$(find "$PROJECT_ROOT/analysis-service/target" -maxdepth 1 -name "analysis-service-*.jar" ! -name "*.original" 2>/dev/null | head -n 1 || echo "")
HISTORY_JAR=$(find "$PROJECT_ROOT/history-service/target" -maxdepth 1 -name "history-service-*.jar" ! -name "*.original" 2>/dev/null | head -n 1 || echo "")

if [ -z "$INGESTION_JAR" ] || [ ! -f "$INGESTION_JAR" ]; then echo "❌ ERROR: ingestion-service build artifact is missing!" && MISSING_ARTIFACTS=1; fi
if [ -z "$ANALYSIS_JAR" ] || [ ! -f "$ANALYSIS_JAR" ]; then echo "❌ ERROR: analysis-service build artifact is missing!" && MISSING_ARTIFACTS=1; fi
if [ -z "$HISTORY_JAR" ] || [ ! -f "$HISTORY_JAR" ]; then echo "❌ ERROR: history-service build artifact is missing!" && MISSING_ARTIFACTS=1; fi
if [ ! -f "$PROJECT_ROOT/bin/gateway/go-service" ]; then echo "❌ ERROR: gateway binary artifact is missing!" && MISSING_ARTIFACTS=1; fi

if [ "$MISSING_ARTIFACTS" -eq 1 ]; then
    echo "🚨 Execution halted due to missing artifacts."
    exit 1
fi

echo "✅ All ecosystem platform artifacts validated successfully."

cp "$INGESTION_JAR" "$PROJECT_ROOT/bin/ingestion-service.jar"
cp "$ANALYSIS_JAR" "$PROJECT_ROOT/bin/analysis-service.jar"
cp "$HISTORY_JAR" "$PROJECT_ROOT/bin/history-service.jar"
# ==============================================================================
# Spawning polyglot execution runtime concurrently
# ==============================================================================
echo "🚀 Launching eco platform runtime context..."
cd "$PROJECT_ROOT/bin" || exit 1

JVM_MEM_OPTS="-Xms256m -Xmx512m"

ENV_PAYLOAD=()

VAULT_INTERNAL_TOKEN=$(docker run --rm -v docker_vault_tokens:/tmp/tokens redis:8.8.0-alpine cat /tmp/tokens/validator_token 2>/dev/null | tr -d ' \n\r' || echo "")
if [ -z "$VAULT_INTERNAL_TOKEN" ]; then
    echo "❌ FATAL: Vault token extraction failed."
    exit 1
fi
POSTGRES_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/postgres" || echo "")
REDIS_SECRETS=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "http://localhost:8200/v1/secret/data/redis" || echo "")
if [ -z "$POSTGRES_SECRETS" ] || [ -z "$REDIS_SECRETS" ]; then
    echo "❌ FATAL: Vault secret responses are empty."
    exit 1
fi
echo "🔑 Extracting run database credentials using native string parsing..."
SPRING_DATA_POSTGRES_USER_NAME=$(echo "$POSTGRES_SECRETS" | grep -o '"eco_user_name":"[^"]*' | sed 's/"eco_user_name":"//')
SPRING_DATA_POSTGRES_PASSWORD=$(echo "$POSTGRES_SECRETS" | grep -o '"eco_user_password":"[^"]*' | sed 's/"eco_user_password":"//')
SPRING_DATA_REDIS_PASSWORD=$(echo "$REDIS_SECRETS" | grep -o '"redis_password":"[^"]*' | sed 's/"redis_password":"//')
if [ -z "$SPRING_DATA_POSTGRES_USER_NAME" ] || [ -z "$SPRING_DATA_POSTGRES_PASSWORD" ] || [ -z "$SPRING_DATA_REDIS_PASSWORD" ] ; then
    echo "❌ FATAL: Failed to parse required credentials from Vault payload."
    exit 1
fi
ENV_PAYLOAD=(
    "SPRING_DATA_POSTGRES_USER_NAME=$SPRING_DATA_POSTGRES_USER_NAME"
    "SPRING_DATA_POSTGRES_PASSWORD=$SPRING_DATA_POSTGRES_PASSWORD"
    "SPRING_DATA_REDIS_PASSWORD=$SPRING_DATA_REDIS_PASSWORD"
)
if [ -f "${PROJECT_ROOT:-.}/.env" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        line=$(echo "$line" | tr -d '\r')
        if [[ -z "$line" || "$line" =~ ^# ]]; then continue; fi
        ENV_PAYLOAD+=("$line")
    done < "${PROJECT_ROOT:-.}/.env"
else
    echo "⚠️  Warning: Local .env file not found, skipping."
fi
INGESTION_DEBUG_OPTS=""
ANALYSIS_DEBUG_OPTS=""
HISTORY_DEBUG_OPTS=""
if [ "${DEBUG_INGESTION:-false}" = true ]; then INGESTION_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"; fi
if [ "${DEBUG_ANALYSIS:-false}" = true ]; then ANALYSIS_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5006"; fi
if [ "${DEBUG_HISTORY:-false}" = true ]; then HISTORY_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5007"; fi
echo "📡 Terminating active host application runtimes..."
pkill -15 -f "ingestion-service.jar" 2>/dev/null || true
pkill -15 -f "analysis-service.jar" 2>/dev/null || true
pkill -15 -f "history-service.jar" 2>/dev/null || true
pkill -15 -f "go-service" 2>/dev/null || true
echo "📡 Spawning background processes..."
# shellcheck disable=SC2086
env "${ENV_PAYLOAD[@]}" java $INGESTION_DEBUG_OPTS ${JVM_MEM_OPTS:-} -Dspring.profiles.active="prod,local" -jar ingestion-service.jar > ingestion.log 2>&1 &
PID_INGESTION=$!
# shellcheck disable=SC2086
env "${ENV_PAYLOAD[@]}" java $ANALYSIS_DEBUG_OPTS ${JVM_MEM_OPTS:-} -Dspring.profiles.active="prod,local" -jar analysis-service.jar > analysis.log 2>&1 &
PID_ANALYSIS=$!
# shellcheck disable=SC2086
env "${ENV_PAYLOAD[@]}" java $HISTORY_DEBUG_OPTS ${JVM_MEM_OPTS:-} -Dspring.profiles.active="prod,local" -jar history-service.jar > history.log 2>&1 &
PID_HISTORY=$!

if [ -d "${PROJECT_ROOT:-.}/bin/gateway" ]; then
    cd "${PROJECT_ROOT}/bin/gateway"
    env "${ENV_PAYLOAD[@]}" ./go-service > gateway.log 2>&1 &
    PID_GATEWAY=$!
    echo "✅ All instances launched cleanly."
else
    echo "❌ FATAL: Compiled gateway directory path does not exist."
    exit 1
fi

echo "⏳ Waiting for initial process warmup validation..."
sleep 2

CRITICAL_FAILURE=0

if kill -0 "$PID_INGESTION" 2>/dev/null; then
    echo "✅ INGESTION SERVICE is active (PID: $PID_INGESTION)"
else
    echo "❌ ERROR: INGESTION SERVICE failed to start! Check bin/ingestion.log"
    CRITICAL_FAILURE=1
fi

if kill -0 "$PID_ANALYSIS" 2>/dev/null; then
    echo "✅ ANALYSIS SERVICE is active (PID: $PID_ANALYSIS)"
else
    echo "❌ ERROR: ANALYSIS SERVICE failed to start! Check bin/analysis.log"
    CRITICAL_FAILURE=1
fi

if kill -0 "$PID_HISTORY" 2>/dev/null; then
    echo "✅ HISTORY SERVICE is active (PID: $PID_HISTORY)"
else
    echo "❌ ERROR: HISTORY SERVICE failed to start! Check bin/history.log"
    CRITICAL_FAILURE=1
fi

if kill -0 "$PID_GATEWAY" 2>/dev/null; then
    echo "✅ GO GATEWAY SERVICE is active (PID: $PID_GATEWAY)"
else
    echo "❌ ERROR: GO GATEWAY SERVICE failed to start! Check bin/gateway.log"
    CRITICAL_FAILURE=1
fi

if [ "$CRITICAL_FAILURE" -eq 1 ]; then
    echo "🚨 Execution halted due to background service crashes. Triggering cleanup script..."
    bash "$PROJECT_ROOT/docker/deployments/stop-platform.sh"
    exit 1
fi

echo "✨ Core ecosystem launched successfully with verified active processes. Main orchestrator closing now..."
bash "$PROJECT_ROOT/docker/deployments/validate.sh"
exit 0

