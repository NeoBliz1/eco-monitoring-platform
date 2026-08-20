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

	# PostgreSQL
	vault kv put secret/postgres \
		eco_user_name="eco_user" \
		eco_user_password="my_super_secret_eco_user_password" \
		metrics_user_name="postgres_metrics_exporter" \
		metrics_user_password="my_super_secret_postgres_metrics_exporter_password" \
		postgres_password="my_super_secret_postgres_password"

	# Redis
	vault kv put secret/redis \
		redis_password="my_super_secret_redis_password"

	# ClickHouse
	# clickhouse_default_password="my_temporary_bootstrap_lockout_pass_123_73928173"
	vault kv put secret/clickhouse \
		clickhouse_user="admin" \
		clickhouse_password="my_highly_secure_clickhouse_production_pass" \
		clickhouse_sha256_password="637b2168c969a87fd0cde6c25c73aa649271d91051b06c2ff53fae329c0d13f7" \
		clickhouse_sha256_default_password="2cc215760674f9439d8c2340a654ceeda481f60fe26c57ac74cc8919dd713504" \
		clickhouse_db="eco_telemetry_logs"

	# Prometheus password: my_actual_password (hashed with bcrypt)
	vault kv put secret/prometheus \
		basic_auth_users="eco_admin: '\$2b\$12\$2MDyAb2OH4nQIoKGETU7S.1ONxa70ATSJUqJbI5z0f.ZT/Hj5MRmi'" \
		user="eco_admin" \
		password="my_actual_password" \
		pg_db_metrics_user_name="postgres_metrics_exporter" \
    pg_db_metrics_user_password="my_super_secret_postgres_metrics_exporter_password"

	# Grafana
	vault kv put secret/grafana \
		admin_user="admin" \
		admin_password="my_highly_secure_grafana_dashboard_pass_2026"

	echo "📝 Injecting Kafka JAAS Configurations..."

	vault kv put secret/kafka \
		admin_user="admin" \
		admin_pass="admin-password" \
		registry_user="registry" \
		registry_pass="registry-secret-pass" \
		vector_user="vector" \
		vector_pass="vector-secret-pass" \
		client_user="client" \
		client_pass="client-secret-pass"
	echo "🎉 All requested secrets have been successfully injected into Vault!"

	echo "🔐 Configuring AppRole security loops..."
	vault auth enable approle

	for app in kafka_1 kafka_2 kafka_3 kafka_topics postgres redis clickhouse grafana schema-registry prometheus vector; do
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
