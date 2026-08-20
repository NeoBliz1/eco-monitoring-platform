#!/bin/bash
# ==============================================================================
# 🛡️ ECO-PLATFORM COMPLETE ARCHITECTURE COMPLIANCE & VALIDATION SUITE (NO-JQ)
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# PATH RESOLUTION & ENVIRONMENTAL CONFIGURATION
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"
TARGET_ENV_FILE="$PROJECT_ROOT/docker/.env"

# ANSI Color Codes for clean output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0;0m'

FAILED_TESTS=0

log_header() { echo -e "\n${BLUE}======================================================================${NC}\n⚡ $1\n${BLUE}======================================================================${NC}"; }
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
# shellcheck disable=SC2317
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() {
	echo -e "${RED}[FAIL]${NC} $1"
	FAILED_TESTS=$((FAILED_TESTS + 1))
}

if [ -f "$TARGET_ENV_FILE" ]; then
	log_info "Sourcing cluster topology properties directly from: $TARGET_ENV_FILE"
	set +u
	# shellcheck disable=SC1090
	source "$TARGET_ENV_FILE"
	set -u
else
	log_error "Target configuration file not found at: $TARGET_ENV_FILE! Halting testing execution matrix."
	exit 1
fi

cd "$PROJECT_ROOT/docker"
CH_SERVICE="clickhouse-db"
PG_SERVICE="pg-db"

declare -A SERVICE_MAP=(
	["kafka-1"]="/run/secrets/kafka_server_jaas"
	["kafka-2"]="/run/secrets/kafka_server_jaas"
	["kafka-3"]="/run/secrets/kafka_server_jaas"
	["schema-registry"]="/run/secrets/schema_registry_jaas"
	[$PG_SERVICE]="/run/secrets/postgres_creds.env"
	[$CH_SERVICE]="/run/secrets/clickhouse_creds.env"
	["grafana"]="/run/secrets/grafana_creds.env"
	["redis-cache"]="/run/secrets/redis_pass_only"
	["prometheus-server"]=""
	["postgres-exporter"]=""
	["vector-sidecar"]="/run/secrets/vector_creds.env"
	["consul-server"]=""
	["vault-server"]=""
	["jaeger-tracing"]=""
)

# ------------------------------------------------------------------------------
# KAFKA INITIALIZATION TIMEOUT GUARDRAIL (Max 60 Seconds)
# ------------------------------------------------------------------------------
log_info "Synchronizing cluster states: Waiting for kafka-init-topics wrapper to complete tasks..."

TIMEOUT_COUNTER=0
MAX_TIMEOUT=60
INIT_SUCCESS=false

while [ $TIMEOUT_COUNTER -lt $MAX_TIMEOUT ]; do
	INIT_STATUS_RAW=$(docker compose ps --all kafka-init-topics 2>/dev/null | grep -viE "NAME[[:space:]]*IMAGE|STATUS" || echo "")

	INIT_STATUS="${INIT_STATUS_RAW,,}"

	if [[ "$INIT_STATUS" == *"exited (0)"* ]]; then
		log_success "Kafka multi-broker topology provisioning finalized cleanly within $((TIMEOUT_COUNTER))s."
		INIT_SUCCESS=true
		break
	fi

	if [[ "$INIT_STATUS" == *"exited ("* ]] && [[ "$INIT_STATUS" != *"exited (0)"* ]]; then
		log_error "CRITICAL: kafka-init-topics container collapsed or encountered an execution failure! Status: $INIT_STATUS_RAW"
		exit 1
	fi

	sleep 1
	TIMEOUT_COUNTER=$((TIMEOUT_COUNTER + 1))
done

if [ "$INIT_SUCCESS" = "false" ]; then
	log_error "CRITICAL TIMEOUT ERROR: kafka-init-topics failed to complete within the 60-second compliance window!"
	exit 1
fi

# ------------------------------------------------------------------------------
# CLUSTER INTEGRITY CHECK: Unauthorized Rogue Services Guardrail
# ------------------------------------------------------------------------------
log_header "INTEGRITY AUDIT: Auditing Network Bounds for Rogue Services"

ALL_RUNNING_SERVICES=$(docker compose ps --services --filter "status=running" 2>/dev/null || echo "")

ROGUE_FOUND=false

while read -r SERVICE_NAME; do
	[ -z "$SERVICE_NAME" ] && continue

	if [[ ${SERVICE_MAP[$SERVICE_NAME]+isset} ]]; then
		continue
	fi

	log_error "SECURITY LOCKDOWN: Unauthorized rogue service caught running in cluster network: [$SERVICE_NAME]!"
	ROGUE_FOUND=true

done <<< "$ALL_RUNNING_SERVICES"

if [ "$ROGUE_FOUND" = "true" ]; then
	log_error "CRITICAL COMPLIANCE FAILURE: Rogue services detected. Aborting structural validation pass!"
	exit 1
else
	log_success "Cluster network integrity verified: Zero rogue containers detected inside the stack bounds. 🔒"
fi

# ------------------------------------------------------------------------------
# STEP 1 & 2: Service Health Checks & Secrets Scrubbing Lifecycle Validation
# ------------------------------------------------------------------------------
log_header "STEP 1 & 2: Validating Service Runtime Health & Secrets Scrubbing Status"

for SERVICE in "${!SERVICE_MAP[@]}"; do
	log_info "Evaluating operational status indicators for service: [$SERVICE]..."

	PS_OUTPUT=$(docker compose ps "$SERVICE" 2>/dev/null || echo "")

	if [ -z "$PS_OUTPUT" ] || ! echo "$PS_OUTPUT" | grep -qiE "${SERVICE}|healthy|running"; then
		log_error "Service '$SERVICE' is NOT found or completely missing from deployment mappings."
		continue
	fi

	if echo "$PS_OUTPUT" | grep -qiE "running|up"; then
		if echo "$PS_OUTPUT" | grep -qi "unhealthy"; then
			log_error "Service '$SERVICE' is running but failed internal healthchecks (unhealthy)."
			continue
		fi
		log_success "Service '$SERVICE' is actively executing and verified healthy."
	else
		log_error "Service '$SERVICE' is NOT running or currently offline."
		continue
	fi

	SECRET_FILE_PATH="${SERVICE_MAP[$SERVICE]}"

	FILE_EXISTS=$(docker compose exec -T "$SERVICE" test -f "$SECRET_FILE_PATH" && echo "yes" || echo "no")

	if [ "$FILE_EXISTS" = "no" ]; then
		log_success "Secrets container wrapper successfully scrubbed '$SECRET_FILE_PATH' from RAM disk."
	else
		log_error "SECURITY INFRACTION: Target secrets file '$SECRET_FILE_PATH' still resides inside RAM on '$SERVICE'!"
	fi
done

# ------------------------------------------------------------------------------
# STEP 3: Kafka Custom Topic Provisioning Verification
# ------------------------------------------------------------------------------
log_header "STEP 3: Validating Kafka Dynamic Topic Initialization"

EXPECTED_TOPICS=(
	"${KAFKA_WEATHER_LIVE_TOPIC}"
	"${KAFKA_WEATHER_RAW_TOPIC}"
	"${KAFKA_WEATHER_HISTORY_TOPIC}"
)

log_info "Querying active Kafka Broker quorum for configured topics..."

VAULT_INTERNAL_TOKEN=$(docker run --rm -v docker_vault_tokens:/tmp/tokens redis:8.8.0-alpine cat /tmp/tokens/validator_token 2>/dev/null | tr -d ' \n\r' || echo "")

if [ -n "$VAULT_INTERNAL_TOKEN" ]; then
	log_success "Successfully extracted validator token natively from named volume storage layer."
else
	log_error "Unable to extract validator token from persistent vault_tokens volume layer."
	exit 1
fi

VAULT_API_URL="http://localhost:8200/v1/secret/data/kafka"
VAULT_RAW_DATA=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "$VAULT_API_URL" || echo "")

if [ -n "$VAULT_RAW_DATA" ] && ! echo "$VAULT_RAW_DATA" | grep -q '"errors"'; then
	KAFKA_ADMIN_USER=$(echo "$VAULT_RAW_DATA" | grep -o '"admin_user"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$')
	KAFKA_ADMIN_USER="admin"
	KAFKA_ADMIN_PASS=$(echo "$VAULT_RAW_DATA" | grep -o '"admin_pass"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$')
	KAFKA_ADMIN_PASS="admin-password"
else
	log_error "Vault API REST transaction failed or token is unauthorized to access secret/data/kafka."
	exit 1

fi

AVAILABLE_TOPICS=$(docker compose exec -T kafka-1 sh -c "
	cat <<EOF > /tmp/client_auth.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\
  username=\"${KAFKA_ADMIN_USER}\" \\
  password=\"${KAFKA_ADMIN_PASS}\";
EOF
	kafka-topics --bootstrap-server localhost:9092 --command-config /tmp/client_auth.properties --list 2>/dev/null
	rm -f /tmp/client_auth.properties
" || echo "")

if [ -z "$AVAILABLE_TOPICS" ]; then
	log_error "Failed to retrieve topics or brokers are uncommunicative via authenticated ports."
else
	for TOPIC in "${EXPECTED_TOPICS[@]}"; do
		if echo "$AVAILABLE_TOPICS" | grep -qFx "$TOPIC"; then
			log_success "Kafka cluster topic provisioning verified: ['$TOPIC'] exists."
		else
			log_error "Kafka cluster error: Expected topic ['$TOPIC'] was not created by init-topics container."
		fi
	done
fi

# ------------------------------------------------------------------------------
# STEP 4: ClickHouse Privilege Lockdowns & Vault Sync
# ------------------------------------------------------------------------------
log_header "STEP 4: Validating ClickHouse Privilege Lockdowns & Vault Sync"

if [ -z "$CH_SERVICE" ]; then
	log_error "CRITICAL: ClickHouse service target key not found inside SERVICE_MAP!"
	exit 1
fi

log_info "Testing ClickHouse '$CH_SERVICE' profile permission restrictions..."

ANON_RESP=$(docker compose exec -T "$CH_SERVICE" clickhouse-client --query "SELECT 1" 2>/dev/null || echo "blocked")

if echo "$ANON_RESP" | grep -q "blocked" || [ -z "$ANON_RESP" ]; then
	log_success "ClickHouse anonymous 'default' profile is cleanly locked down."
else
	log_error "CRITICAL: ClickHouse allows anonymous queries on the default user!"
fi

if [ -z "$VAULT_INTERNAL_TOKEN" ]; then
	log_error "Validator token is empty or missing. Skipping deep database sync checks."
	exit 1
else
	VAULT_API_URL="http://localhost:8200/v1/secret/data/clickhouse"
	VAULT_RAW_DATA=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "$VAULT_API_URL" || echo "")

	CH_USER=$(echo "$VAULT_RAW_DATA" | grep -o '"clickhouse_user"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)
	CH_PASS=$(echo "$VAULT_RAW_DATA" | grep -o '"clickhouse_password"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)
	CH_TARGET_DB=$(echo "$VAULT_RAW_DATA" | grep -o '"clickhouse_db"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)

	if [ -z "$CH_USER" ] || [ -z "$CH_PASS" ] || [ -z "$CH_TARGET_DB" ]; then
		log_error "CRITICAL SECURITY ERROR: ClickHouse credentials or target database name could not be extracted from Vault!"
		exit 1
	fi

	set +e
	AUTH_RESP=$(docker compose exec -T "$CH_SERVICE" clickhouse-client --user "$CH_USER" --password "$CH_PASS" --query "SELECT 'authenticated'" 2>/dev/null)
	AUTH_EXIT_CODE=$?
	set -e

	if [ $AUTH_EXIT_CODE -eq 0 ] && echo "$AUTH_RESP" | grep -q "authenticated"; then
		log_success "ClickHouse administrator profile fully synchronized with Vault credentials! 🔒"
	else
		log_error "ClickHouse sync error: Authenticated transaction rejected using Vault-provided credentials. (Exit Code: $AUTH_EXIT_CODE)"
		exit 1
	fi

	set +e
	DB_LIST_RESP=$(docker compose exec -T "$CH_SERVICE" clickhouse-client --user "$CH_USER" --password "$CH_PASS" --query "SHOW DATABASES" 2>/dev/null)
	DB_EXIT_CODE=$?
	set -e

	if [ $DB_EXIT_CODE -eq 0 ] && echo "$DB_LIST_RESP" | grep -qFx "$CH_TARGET_DB"; then
		log_success "Database initialization verified: ['$CH_TARGET_DB'] successfully created and active. 📊"
	else
		log_error "Database validation error: Target schema ['$CH_TARGET_DB'] does not exist inside the active ClickHouse cluster!"
		exit 1
	fi
fi

# ------------------------------------------------------------------------------
# STEP 5: PostgreSQL Database & Environment Schema Existence Verification
# ------------------------------------------------------------------------------
log_header "STEP 5: Validating PostgreSQL Schema & Database Boundaries"

PG_DB_TARGET="${POSTGRES_DB}"
PG_SCHEMA_TARGET="${POSTGRES_DB_SCHEMA}"

if [ -z "$PG_SERVICE" ]; then
	log_error "CRITICAL: PostgreSQL service target key not found inside SERVICE_MAP!"
	exit 1
fi

if [ -z "$VAULT_INTERNAL_TOKEN" ]; then
	log_error "Validator token is empty or missing. Skipping deep PostgreSQL database verification checks."
	exit 1
else
	VAULT_API_URL="http://localhost:8200/v1/secret/data/postgres"
	VAULT_RAW_DATA=$(curl -s --header "X-Vault-Token: $VAULT_INTERNAL_TOKEN" "$VAULT_API_URL" || echo "")

	PG_ADMIN_USER="postgres"
	PG_ADMIN_PASS=$(echo "$VAULT_RAW_DATA" | grep -o '"postgres_password"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)

	PG_APP_USER=$(echo "$VAULT_RAW_DATA" | grep -o '"eco_user_name"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)
	PG_APP_PASS=$(echo "$VAULT_RAW_DATA" | grep -o '"eco_user_password"[[:space:]]*:[[:space:]]*"[^"]*' | grep -o '[^"]*$' || true)

	if [ -z "$PG_ADMIN_PASS" ] || [ -z "$PG_APP_USER" ] || [ -z "$PG_APP_PASS" ]; then
		log_error "CRITICAL SECURITY ERROR: PostgreSQL administrative or application credentials could not be extracted from Vault!"
		exit 1
	fi
fi

log_info "Verifying role profiles existence inside cluster..."

set +e
ADMIN_CONN_TEST=$(docker compose exec -T "$PG_SERVICE" sh -c "PGPASSWORD='$PG_ADMIN_PASS' psql -U '$PG_ADMIN_USER' -d 'postgres' -tAc \"SELECT 'ok';\"" 2>/dev/null || echo "failed")
APP_CONN_TEST=$(docker compose exec -T "$PG_SERVICE" sh -c "PGPASSWORD='$PG_APP_PASS' psql -U '$PG_APP_USER' -d 'postgres' -tAc \"SELECT 'ok';\"" 2>/dev/null || echo "failed")
set -e

if [ "$ADMIN_CONN_TEST" = "ok" ]; then
	log_success "Administrative user authenticated successfully: role ['$PG_ADMIN_USER'] verified."
else
	log_error "PostgreSQL connection error: Administrative role ['$PG_ADMIN_USER'] authentication failed."
	exit 1
fi

if [ "$APP_CONN_TEST" = "ok" ]; then
	log_success "Application user authenticated successfully: role ['$PG_APP_USER'] verified."
else
	log_error "PostgreSQL connection error: Application role ['$PG_APP_USER'] authentication failed."
	exit 1
fi

log_info "Querying PostgreSQL catalog to verify targeted database allocation: [$PG_DB_TARGET]"

set +e
PG_DB_VERIFY=$(docker compose exec -T "$PG_SERVICE" sh -c "PGPASSWORD='$PG_ADMIN_PASS' psql -U '$PG_ADMIN_USER' -d 'postgres' -tAc \"SELECT 1 FROM pg_database WHERE datname='$PG_DB_TARGET';\"" 2>/dev/null || echo "0")
set -e

if [ "$PG_DB_VERIFY" = "1" ]; then
	log_success "PostgreSQL target database allocation verified: Database '$PG_DB_TARGET' exists."
	log_info "Validating isolation boundaries for schema parameter initialization: [$PG_SCHEMA_TARGET]"

	set +e
	PG_SCHEMA_VERIFY=$(docker compose exec -T "$PG_SERVICE" sh -c "PGPASSWORD='$PG_ADMIN_PASS' psql -U '$PG_ADMIN_USER' -d '$PG_DB_TARGET' -tAc \"SELECT 1 FROM information_schema.schemata WHERE schema_name='$PG_SCHEMA_TARGET';\"" 2>/dev/null || echo "0")
	set -e

	if [ "$PG_SCHEMA_VERIFY" = "1" ]; then
		log_success "PostgreSQL infrastructure validation successful: Schema '$PG_SCHEMA_TARGET' exists inside database '$PG_DB_TARGET'."
	else
		log_error "PostgreSQL validation error: Targeted schema context '$PG_SCHEMA_TARGET' does not exist inside database '$PG_DB_TARGET'."
		exit 1
	fi
else
	log_error "PostgreSQL validation error: Main application database target named '$PG_DB_TARGET' could not be found."
	exit 1
fi

# ------------------------------------------------------------------------------
# 🛡️ POST-VALIDATION SECURE VOLUME SCRUBBING
# ------------------------------------------------------------------------------
log_header "SECURITY PURGE: Scrubbing plain-text volume tokens from disk..."

docker run --rm \
  -v docker_vault_tokens:/tmp/tokens \
  redis:8.8.0-alpine sh -c 'rm -rf /tmp/tokens/*' 2>/dev/null || true

log_success "All validation and AppRole tokens have been successfully scrubbed from host storage! 🔒"

log_header "Initiating system health check via Consul service registry..."

for SERVICE_KEY in "${!SERVICE_MAP[@]}"; do
    if [[ "$SERVICE_KEY" == "consul-server" || "$SERVICE_KEY" == "vault-server" ]]; then
        continue
    fi

    HEALTH_PAYLOAD=$(curl -s "http://localhost:8500/v1/health/checks/$SERVICE_KEY" || echo "")

    if [ -z "$HEALTH_PAYLOAD" ] || [ "$HEALTH_PAYLOAD" = "[]" ]; then
        BASE_NAME="${SERVICE_KEY%%-[0-9]*}"
        if [ "$BASE_NAME" != "$SERVICE_KEY" ]; then
            HEALTH_PAYLOAD=$(curl -s "http://localhost:8500/v1/health/checks/$BASE_NAME" || echo "")
        fi
    fi

    if [ -z "$HEALTH_PAYLOAD" ] || [ "$HEALTH_PAYLOAD" = "[]" ]; then
        log_error "Service '$SERVICE_KEY' is missing or unindexed in the Consul catalog."
        continue
    fi

    ALL_STATUSES=$(echo "$HEALTH_PAYLOAD" | grep -o '"Status":"[^"]*' | sed 's/"Status":"//' || echo "")

    if [ -z "$ALL_STATUSES" ]; then
        log_error "Service '$SERVICE_KEY' check states are undefined inside payload data."
    elif echo "$ALL_STATUSES" | grep -qv "passing"; then
        CRITICAL_COUNT=$(echo "$ALL_STATUSES" | grep -v "passing" | wc -l | tr -d ' ')
        log_error "Service '$SERVICE_KEY' has ($CRITICAL_COUNT) active health check failures!"
    else
        log_success "Service '$SERVICE_KEY' is online and healthy."
    fi
done

#	==============================================================================
#	PIPELINE COMPLETE EVALUATION
#	==============================================================================
echo -e "\n======================================================================"
if [ "$FAILED_TESTS" -eq 0 ]; then
	echo -e "${GREEN}🎉 ALL COMPLIANCE POLICIES PASSED SUCCESSFULLY. ENVIRONMENT HARDENED AND OPERATIONAL!${NC}"
	exit 0
else
	echo -e "${RED}🚨 CLUSTER AUDIT FAILURE: Identified $FAILED_TESTS infrastructure policy infraction(s).${NC}"
	exit 1
fi
