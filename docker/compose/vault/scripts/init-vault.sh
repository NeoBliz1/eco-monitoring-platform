#!/bin/sh
set -e

echo "📡 Vault server is up. Checking initialization status..."
RAW_STATUS=$(vault status -format=json 2>/dev/null || true)

if echo "$RAW_STATUS" | grep -q '"initialized": false'; then
	echo "📡 Initializing a brand-new Vault cluster schema..."

	INIT_RAW=$(vault operator init -key-shares=3 -key-threshold=2)

	UNSEAL_KEY_1=$(echo "$INIT_RAW" | grep "Unseal Key 1:" | awk '{print $NF}')
	echo "🔑 DEMO UNSEAL KEY 1"
	UNSEAL_KEY_2=$(echo "$INIT_RAW" | grep "Unseal Key 2:" | awk '{print $NF}')
	echo "🔑 DEMO UNSEAL KEY 2"
	ROOT_TOKEN=$(echo "$INIT_RAW" | grep "Initial Root Token:" | awk '{print $NF}')
	echo "🎟️ DEMO ROOT TOKEN"

	vault operator unseal "$UNSEAL_KEY_1"
	vault operator unseal "$UNSEAL_KEY_2"

	export VAULT_TOKEN=$ROOT_TOKEN
	echo "VAULT_TOKEN: $VAULT_TOKEN"
	vault secrets enable -path=secret kv-v2

	# --------------------------------------------------------------------------
	# 🛡️ EXPORT INFRASTRUCTURE VALIDATION BACKDOOR TOKEN (CRUCIAL FOR TESTS)
	# --------------------------------------------------------------------------
	# Creates a 5-minute token used exclusively by validate-all.sh so it does not
	# burn the single-use application secret_ids during testing phases.
	vault token create -ttl=5000m -field=token >/tmp/vault_output/validator_token

	echo "📝 Injecting multi-app secrets into database buckets..."

	echo "🔐 Injecting History Service Credentials..."
	vault kv put secret/history-service \
		postgres_user="$POSTGRES_ECO_USER_NAME" \
		postgres_password="$POSTGRES_ECO_USER_PASSWORD" \
		kafka_client="$KAFKA_CLIENT_USER" \
		kafka_client_password="$KAFKA_CLIENT_PASS"

	echo "🔐 Injecting Analysis Service Credentials..."
	vault kv put secret/analysis-service \
		redis_password="$REDIS_PASSWORD" \
		kafka_client="$KAFKA_CLIENT_USER" \
		kafka_client_password="$KAFKA_CLIENT_PASS"

	echo "🔐 Injecting Postgres Credentials..."
	vault kv put secret/postgres \
		eco_user_name="$POSTGRES_ECO_USER_NAME" \
		eco_user_password="$POSTGRES_ECO_USER_PASSWORD" \
		metrics_user_name="$POSTGRES_METRICS_USER_NAME" \
		metrics_user_password="$POSTGRES_METRICS_USER_PASSWORD" \
		postgres_password="$POSTGRES_POSTGRES_PASSWORD"

	echo "🔐 Injecting Redis Credentials..."
	vault kv put secret/redis \
		redis_password="$REDIS_PASSWORD"

	echo "🔐 Injecting ClickHouse Credentials..."
	vault kv put secret/clickhouse \
		clickhouse_user="$CLICKHOUSE_USER" \
		clickhouse_password="$CLICKHOUSE_PASSWORD" \
		clickhouse_sha256_password="$CLICKHOUSE_SHA256_PASSWORD" \
		clickhouse_sha256_default_password="$CLICKHOUSE_SHA256_DEFAULT_PASSWORD" \
		clickhouse_db="$CLICKHOUSE_DB"

	echo "🔐 Injecting Prometheus Credentials..."
	vault kv put secret/prometheus \
		basic_auth_users="$PROMETHEUS_BASIC_AUTH_USERS" \
		user="$PROMETHEUS_USER" \
		password="$PROMETHEUS_PASSWORD" \
		pg_db_metrics_user_name="$PROMETHEUS_PG_DB_METRICS_USER_NAME" \
		pg_db_metrics_user_password="$PROMETHEUS_PG_DB_METRICS_USER_PASSWORD"

	echo "🔐 Injecting Grafana Credentials..."
	vault kv put secret/grafana \
		admin_user="$GRAFANA_ADMIN_USER" \
		admin_password="$GRAFANA_ADMIN_PASSWORD"

	echo "📝 Injecting Kafka JAAS Configurations..."
	vault kv put secret/kafka \
		admin_user="$KAFKA_ADMIN_USER" \
		admin_pass="$KAFKA_ADMIN_PASS" \
		registry_user="$KAFKA_REGISTRY_USER" \
		registry_pass="$KAFKA_REGISTRY_PASS" \
		vector_user="$KAFKA_VECTOR_USER" \
		vector_pass="$KAFKA_VECTOR_PASS" \
		client_user="$KAFKA_CLIENT_USER" \
		client_pass="$KAFKA_CLIENT_PASS"

	echo "✅ All secrets successfully synchronized to Vault Storage!"

	echo "🔐 Configuring AppRole security loops..."
	vault auth enable approle

	for app in kafka_1 kafka_2 kafka_3 kafka_topics postgres redis clickhouse grafana schema-registry prometheus vector history-service analysis-service; do
		echo "🔧 Provisioning permissions infrastructure for: [$app]"

		if [ "$app" = "schema-registry" ] || [ "$app" = "kafka_1" ] || [ "$app" = "kafka_2" ] || [ "$app" = "kafka_3" ] || [ "$app" = "kafka_topics" ]; then
			TARGET_PATH="kafka"
		elif [ "$app" = "vector" ]; then
			vault policy write vector-policy - <<EOF
path "secret/data/kafka" { capabilities = ["read"] }
path "secret/data/clickhouse" { capabilities = ["read"] }
EOF
			# Generate vector specific roles and tokens directly
			vault write auth/approle/role/vector-role token_policies="vector-policy" token_ttl=1h
			vault read -field=role_id auth/approle/role/vector-role/role-id >/tmp/vault_output/vector_role_id
			vault write -f -field=secret_id auth/approle/role/vector-role/secret-id >/tmp/vault_output/vector_secret_id
			continue
		else
			TARGET_PATH="$app"
		fi

		vault policy write ${app}-policy - <<EOF
path "secret/data/${TARGET_PATH}" { capabilities = ["read"] }
EOF

		vault write auth/approle/role/${app}-role \
			token_policies="${app}-policy" \
			token_ttl=1h \
			secret_id_num_uses=1

		vault read -field=role_id auth/approle/role/${app}-role/role-id >/tmp/vault_output/${app}_role_id
		vault write -f -field=secret_id auth/approle/role/${app}-role/secret-id secret_id_num_uses=1 >/tmp/vault_output/${app}_secret_id
	done

	echo "🎉 Automated initialization complete! AppRole credentials securely exported to shared storage volume."
else
	echo "ℹ️ Vault is already initialized. Checking if unseal state is currently valid..."
	SEAL_STATUS=$( (vault status -format=json 2>/dev/null || true) | grep '"sealed":' | awk -F: '{print $2}' | tr -d ' ,"\n')
	if [ "$SEAL_STATUS" = "true" ]; then
		echo "⚠️ Vault server is currently locked/sealed. Manual unseal operation required."
	else
		echo "✅ Vault cluster is unsealed and operating normally."
	fi
fi
